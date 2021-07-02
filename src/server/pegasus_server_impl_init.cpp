/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "pegasus_server_impl.h"

#include <unordered_map>
#include <dsn/utility/flags.h>
#include <rocksdb/filter_policy.h>

#include "capacity_unit_calculator.h"
#include "hashkey_transform.h"
#include "meta_store.h"
#include "pegasus_event_listener.h"
#include "pegasus_server_write.h"
#include "hotkey_collector.h"

namespace pegasus {
namespace server {

DSN_DEFINE_int64(
    "pegasus.server",
    rocksdb_limiter_max_write_megabytes_per_sec,
    500,
    "max rate of rocksdb flush and compaction(MB/s), if less than or equal to 0 means close limit");

DSN_DEFINE_bool("pegasus.server",
                rocksdb_limiter_enable_auto_tune,
                false,
                "whether to enable write rate auto tune when open rocksdb write limit");

static const std::unordered_map<std::string, rocksdb::BlockBasedTableOptions::IndexType>
    INDEX_TYPE_STRING_MAP = {
        {"binary_search", rocksdb::BlockBasedTableOptions::IndexType::kBinarySearch},
        {"hash_search", rocksdb::BlockBasedTableOptions::IndexType::kHashSearch},
        {"two_level_index_search",
         rocksdb::BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch},
        {"binary_search_with_first_key",
         rocksdb::BlockBasedTableOptions::IndexType::kBinarySearchWithFirstKey}};

pegasus_server_impl::pegasus_server_impl(dsn::replication::replica *r)
    : pegasus_read_service(r),
      _db(nullptr),
      _data_cf(nullptr),
      _meta_cf(nullptr),
      _is_open(false),
      _pegasus_data_version(PEGASUS_DATA_VERSION_MAX),
      _last_durable_decree(0),
      _is_checkpointing(false),
      _manual_compact_svc(this),
      _partition_version(0)
{
    _primary_address = dsn::rpc_address(dsn_primary_address()).to_string();
    _gpid = get_gpid();

    _read_hotkey_collector =
        std::make_shared<hotkey_collector>(dsn::replication::hotkey_type::READ, this);
    _write_hotkey_collector =
        std::make_shared<hotkey_collector>(dsn::replication::hotkey_type::WRITE, this);

    _verbose_log = dsn_config_get_value_bool("pegasus.server",
                                             "rocksdb_verbose_log",
                                             false,
                                             "whether to print verbose log for debugging");
    _slow_query_threshold_ns_in_config = dsn_config_get_value_uint64(
        "pegasus.server",
        "rocksdb_slow_query_threshold_ns",
        100000000,
        "get/multi-get operation duration exceed this threshold will be logged");
    _slow_query_threshold_ns = _slow_query_threshold_ns_in_config;
    dassert(_slow_query_threshold_ns > 0, "slow query threshold must be greater than 0");
    _abnormal_get_size_threshold = dsn_config_get_value_uint64(
        "pegasus.server",
        "rocksdb_abnormal_get_size_threshold",
        1000000,
        "get operation value size exceed this threshold will be logged, 0 means no check");
    _abnormal_multi_get_size_threshold =
        dsn_config_get_value_uint64("pegasus.server",
                                    "rocksdb_abnormal_multi_get_size_threshold",
                                    10000000,
                                    "multi-get operation total key-value size exceed this "
                                    "threshold will be logged, 0 means no check");
    _abnormal_multi_get_iterate_count_threshold = dsn_config_get_value_uint64(
        "pegasus.server",
        "rocksdb_abnormal_multi_get_iterate_count_threshold",
        1000,
        "multi-get operation iterate count exceed this threshold will be logged, 0 means no check");

    _rng_rd_opts.multi_get_max_iteration_count = (uint32_t)dsn_config_get_value_uint64(
        "pegasus.server",
        "rocksdb_multi_get_max_iteration_count",
        3000,
        "max iteration count for each range read for multi-get operation, if "
        "exceed this threshold,"
        "iterator will be stopped");

    _rng_rd_opts.multi_get_max_iteration_size =
        dsn_config_get_value_uint64("pegasus.server",
                                    "rocksdb_multi_get_max_iteration_size",
                                    30 << 20,
                                    "multi-get operation total key-value size exceed "
                                    "this threshold will stop iterating rocksdb, 0 means no check");

    _rng_rd_opts.rocksdb_max_iteration_count =
        (uint32_t)dsn_config_get_value_uint64("pegasus.server",
                                              "rocksdb_max_iteration_count",
                                              1000,
                                              "max iteration count for each range "
                                              "read, if exceed this threshold, "
                                              "iterator will be stopped");

    _rng_rd_opts.rocksdb_iteration_threshold_time_ms_in_config = dsn_config_get_value_uint64(
        "pegasus.server",
        "rocksdb_iteration_threshold_time_ms",
        30000,
        "max duration for handling one pegasus scan request(sortkey_count/multiget/scan) if exceed "
        "this threshold, iterator will be stopped, 0 means no check");
    _rng_rd_opts.rocksdb_iteration_threshold_time_ms =
        _rng_rd_opts.rocksdb_iteration_threshold_time_ms_in_config;

    // init rocksdb::DBOptions
    _db_opts.create_if_missing = true;
    // atomic flush data CF and meta CF, aim to keep consistency of 'last flushed decree' in meta CF
    // and data in data CF.
    _db_opts.atomic_flush = true;

    _db_opts.use_direct_reads = dsn_config_get_value_bool(
        "pegasus.server", "rocksdb_use_direct_reads", false, "rocksdb options.use_direct_reads");

    _db_opts.use_direct_io_for_flush_and_compaction =
        dsn_config_get_value_bool("pegasus.server",
                                  "rocksdb_use_direct_io_for_flush_and_compaction",
                                  false,
                                  "rocksdb options.use_direct_io_for_flush_and_compaction");

    _db_opts.compaction_readahead_size =
        dsn_config_get_value_uint64("pegasus.server",
                                    "rocksdb_compaction_readahead_size",
                                    2 * 1024 * 1024,
                                    "rocksdb options.compaction_readahead_size");

    _db_opts.writable_file_max_buffer_size =
        dsn_config_get_value_uint64("pegasus.server",
                                    "rocksdb_writable_file_max_buffer_size",
                                    1024 * 1024,
                                    "rocksdb options.writable_file_max_buffer_size");

    _statistics = rocksdb::CreateDBStatistics();
    _statistics->set_stats_level(rocksdb::kExceptDetailedTimers);
    _db_opts.statistics = _statistics;

    _db_opts.listeners.emplace_back(new pegasus_event_listener(this));

    // flush threads are shared among all rocksdb instances in one process.
    _db_opts.max_background_flushes =
        (int)dsn_config_get_value_int64("pegasus.server",
                                        "rocksdb_max_background_flushes",
                                        4,
                                        "rocksdb options.max_background_flushes");

    // compaction threads are shared among all rocksdb instances in one process.
    _db_opts.max_background_compactions =
        (int)dsn_config_get_value_int64("pegasus.server",
                                        "rocksdb_max_background_compactions",
                                        12,
                                        "rocksdb options.max_background_compactions");

    // init rocksdb::ColumnFamilyOptions for data column family
    _data_cf_opts.write_buffer_size =
        (size_t)dsn_config_get_value_uint64("pegasus.server",
                                            "rocksdb_write_buffer_size",
                                            64 * 1024 * 1024,
                                            "rocksdb options.write_buffer_size");

    _data_cf_opts.max_write_buffer_number =
        (int)dsn_config_get_value_int64("pegasus.server",
                                        "rocksdb_max_write_buffer_number",
                                        3,
                                        "rocksdb options.max_write_buffer_number");

    _data_cf_opts.num_levels = (int)dsn_config_get_value_int64(
        "pegasus.server", "rocksdb_num_levels", 6, "rocksdb options.num_levels");

    _data_cf_opts.target_file_size_base =
        dsn_config_get_value_uint64("pegasus.server",
                                    "rocksdb_target_file_size_base",
                                    64 * 1024 * 1024,
                                    "rocksdb options.target_file_size_base");

    _data_cf_opts.target_file_size_multiplier =
        (int)dsn_config_get_value_int64("pegasus.server",
                                        "rocksdb_target_file_size_multiplier",
                                        1,
                                        "rocksdb options.target_file_size_multiplier");

    _data_cf_opts.max_bytes_for_level_base =
        dsn_config_get_value_uint64("pegasus.server",
                                    "rocksdb_max_bytes_for_level_base",
                                    10 * 64 * 1024 * 1024,
                                    "rocksdb options.max_bytes_for_level_base");

    _data_cf_opts.max_bytes_for_level_multiplier =
        dsn_config_get_value_double("pegasus.server",
                                    "rocksdb_max_bytes_for_level_multiplier",
                                    10,
                                    "rocksdb options.rocksdb_max_bytes_for_level_multiplier");

    // we need set max_compaction_bytes definitely because set_usage_scenario() depends on it.
    _data_cf_opts.max_compaction_bytes = _data_cf_opts.target_file_size_base * 25;

    _data_cf_opts.level0_file_num_compaction_trigger =
        (int)dsn_config_get_value_int64("pegasus.server",
                                        "rocksdb_level0_file_num_compaction_trigger",
                                        4,
                                        "rocksdb options.level0_file_num_compaction_trigger");

    _data_cf_opts.level0_slowdown_writes_trigger = (int)dsn_config_get_value_int64(
        "pegasus.server",
        "rocksdb_level0_slowdown_writes_trigger",
        30,
        "rocksdb options.level0_slowdown_writes_trigger, default 30");

    _data_cf_opts.level0_stop_writes_trigger =
        (int)dsn_config_get_value_int64("pegasus.server",
                                        "rocksdb_level0_stop_writes_trigger",
                                        60,
                                        "rocksdb options.level0_stop_writes_trigger");

    std::string compression_str = dsn_config_get_value_string(
        "pegasus.server",
        "rocksdb_compression_type",
        "lz4",
        "rocksdb options.compression. Available config: '[none|snappy|zstd|lz4]' "
        "for all level 2 and higher levels, and "
        "'per_level:[none|snappy|zstd|lz4],[none|snappy|zstd|lz4],...' for each level 0,1,..., the "
        "last compression type will be used for levels not specified in the list.");
    dassert(parse_compression_types(compression_str, _data_cf_opts.compression_per_level),
            "parse rocksdb_compression_type failed.");

    _meta_cf_opts = _data_cf_opts;
    // Set level0_file_num_compaction_trigger of meta CF as 10 to reduce frequent compaction.
    _meta_cf_opts.level0_file_num_compaction_trigger = 10;
    // Data in meta CF is very little, disable compression to save CPU load.
    dassert(parse_compression_types("none", _meta_cf_opts.compression_per_level),
            "parse rocksdb_compression_type failed.");

    rocksdb::BlockBasedTableOptions tbl_opts;
    if (dsn_config_get_value_bool("pegasus.server",
                                  "rocksdb_disable_table_block_cache",
                                  false,
                                  "rocksdb tbl_opts.no_block_cache")) {
        tbl_opts.no_block_cache = true;
        tbl_opts.block_restart_interval = 4;
    } else {
        // If block cache is enabled, all replicas on this server will share the same block cache
        // object. It's convenient to control the total memory used by this server, and the LRU
        // algorithm used by the block cache object can be more efficient in this way.
        static std::once_flag flag;
        std::call_once(flag, [&]() {
            uint64_t capacity = dsn_config_get_value_uint64(
                "pegasus.server",
                "rocksdb_block_cache_capacity",
                10 * 1024 * 1024 * 1024ULL,
                "block cache capacity for one pegasus server, shared by all rocksdb instances");

            // block cache num shard bits, default -1(auto)
            int num_shard_bits = (int)dsn_config_get_value_int64(
                "pegasus.server",
                "rocksdb_block_cache_num_shard_bits",
                -1,
                "block cache will be sharded into 2^num_shard_bits shards");

            // init block cache
            _s_block_cache = rocksdb::NewLRUCache(capacity, num_shard_bits);
        });

        // every replica has the same block cache
        tbl_opts.block_cache = _s_block_cache;
    }

    // FLAGS_rocksdb_limiter_max_write_megabytes_per_sec <= 0 means close the rate limit.
    // For more detail arguments see
    // https://github.com/facebook/rocksdb/blob/v6.6.4/include/rocksdb/rate_limiter.h#L111-L137
    if (FLAGS_rocksdb_limiter_max_write_megabytes_per_sec > 0) {
        static std::once_flag flag;
        std::call_once(flag, [&]() {
            _s_rate_limiter = std::shared_ptr<rocksdb::RateLimiter>(rocksdb::NewGenericRateLimiter(
                FLAGS_rocksdb_limiter_max_write_megabytes_per_sec << 20,
                100 * 1000, // refill_period_us
                10,         // fairness
                rocksdb::RateLimiter::Mode::kWritesOnly,
                FLAGS_rocksdb_limiter_enable_auto_tune));
        });
        _db_opts.rate_limiter = _s_rate_limiter;
    }

    bool enable_write_buffer_manager =
        dsn_config_get_value_bool("pegasus.server",
                                  "rocksdb_enable_write_buffer_manager",
                                  false,
                                  "enable write buffer manager to limit total memory "
                                  "used by memtables and block caches across multiple replicas");
    ddebug_replica("rocksdb_enable_write_buffer_manager = {}", enable_write_buffer_manager);
    if (enable_write_buffer_manager) {
        // If write buffer manager is enabled, all replicas(one DB instance for each
        // replica) on this server will share the same write buffer manager object,
        // thus the same block cache object. It's convenient to control the total memory
        // of memtables and block caches used by this server.
        //
        // While write buffer manager is enabled, total_size_across_write_buffer = 0
        // indicates no limit on memory, for details see:
        // https://github.com/facebook/rocksdb/blob/v6.6.4/include/rocksdb/write_buffer_manager.h#L23-24
        static std::once_flag flag;
        std::call_once(flag, [&]() {
            uint64_t total_size_across_write_buffer = dsn_config_get_value_uint64(
                "pegasus.server",
                "rocksdb_total_size_across_write_buffer",
                0,
                "total size limit used by memtables across multiple replicas");
            ddebug_replica("rocksdb_total_size_across_write_buffer = {}",
                           total_size_across_write_buffer);
            _s_write_buffer_manager = std::make_shared<rocksdb::WriteBufferManager>(
                static_cast<size_t>(total_size_across_write_buffer), tbl_opts.block_cache);
        });
        _db_opts.write_buffer_manager = _s_write_buffer_manager;
    }

    int64_t max_open_files = dsn_config_get_value_int64(
        "pegasus.server",
        "rocksdb_max_open_files",
        -1, /* always keep files opened, default by rocksdb */
        "number of opened files that can be used by a replica(namely a DB instance)");
    _db_opts.max_open_files = static_cast<int>(max_open_files);
    ddebug_replica("rocksdb_max_open_files = {}", _db_opts.max_open_files);

    // Configurations of rocksdb info-log.
    // By default, we keep 10 days of info-logs at maximum, combined with the total size limit
    // of 10*32MB = 320MB.
    _db_opts.keep_log_file_num = dsn_config_get_value_int64(
        "pegasus.server", "rocksdb_keep_log_file_num", 10, "Maximal RocksDB log files to be kept.");
    _db_opts.log_file_time_to_roll =
        dsn_config_get_value_int64("pegasus.server",
                                   "rocksdb_log_file_time_to_roll",
                                   24 * 3600, /*86400*/
                                   "Time for the info log file to roll (in seconds).");
    _db_opts.max_log_file_size =
        dsn_config_get_value_int64("pegasus.server",
                                   "rocksdb_max_log_file_size",
                                   32 * 1024 * 1024, /*33554432*/
                                   "The maximal size of the info log file.");

    std::string index_type =
        dsn_config_get_value_string("pegasus.server",
                                    "rocksdb_index_type",
                                    "binary_search",
                                    "The index type that will be used for this table.");
    auto index_type_item = INDEX_TYPE_STRING_MAP.find(index_type);
    dassert(index_type_item != INDEX_TYPE_STRING_MAP.end(),
            "[pegasus.server]rocksdb_index_type should be one among binary_search, "
            "hash_search, two_level_index_search or binary_search_with_first_key.");
    tbl_opts.index_type = index_type_item->second;
    ddebug_replica("rocksdb_index_type = {}", index_type.c_str());

    tbl_opts.partition_filters = dsn_config_get_value_bool(
        "pegasus.server",
        "rocksdb_partition_filters",
        false,
        "Note: currently this option requires two_level_index_search to be set as well. "
        "Use partitioned full filters for each SST file. This option is "
        "incompatibile with block-based filters.");
    ddebug_replica("rocksdb_partition_filters = {}", tbl_opts.partition_filters);

    tbl_opts.metadata_block_size = dsn_config_get_value_uint64(
        "pegasus.server",
        "rocksdb_metadata_block_size",
        4096,
        "Block size for partitioned metadata. Currently applied to indexes when "
        "two_level_index_search is used and to filters when partition_filters is used. "
        "Note: Since in the current implementation the filters and index partitions "
        "are aligned, an index/filter block is created when either index or filter "
        "block size reaches the specified limit. "
        "Note: this limit is currently applied to only index blocks; a filter "
        "partition is cut right after an index block is cut");
    ddebug_replica("rocksdb_metadata_block_size = {}", tbl_opts.metadata_block_size);

    tbl_opts.cache_index_and_filter_blocks = dsn_config_get_value_bool(
        "pegasus.server",
        "rocksdb_cache_index_and_filter_blocks",
        false,
        "Indicating if we'd put index/filter blocks to the block cache. "
        "If not specified, each \"table reader\" object will pre-load index/filter "
        "block during table initialization.");
    ddebug_replica("rocksdb_cache_index_and_filter_blocks = {}",
                   tbl_opts.cache_index_and_filter_blocks);

    tbl_opts.pin_top_level_index_and_filter = dsn_config_get_value_bool(
        "pegasus.server",
        "rocksdb_pin_top_level_index_and_filter",
        true,
        "If cache_index_and_filter_blocks is true and the below is true, then "
        "the top-level index of partitioned filter and index blocks are stored in "
        "the cache, but a reference is held in the \"table reader\" object so the "
        "blocks are pinned and only evicted from cache when the table reader is "
        "freed. This is not limited to l0 in LSM tree.");
    ddebug_replica("rocksdb_pin_top_level_index_and_filter = {}",
                   tbl_opts.pin_top_level_index_and_filter);

    tbl_opts.cache_index_and_filter_blocks_with_high_priority = dsn_config_get_value_bool(
        "pegasus.server",
        "rocksdb_cache_index_and_filter_blocks_with_high_priority",
        true,
        "If cache_index_and_filter_blocks is enabled, cache index and filter "
        "blocks with high priority. If set to true, depending on implementation of "
        "block cache, index and filter blocks may be less likely to be evicted "
        "than data blocks.");
    ddebug_replica("rocksdb_cache_index_and_filter_blocks_with_high_priority = {}",
                   tbl_opts.cache_index_and_filter_blocks_with_high_priority);

    tbl_opts.pin_l0_filter_and_index_blocks_in_cache = dsn_config_get_value_bool(
        "pegasus.server",
        "rocksdb_pin_l0_filter_and_index_blocks_in_cache",
        false,
        "if cache_index_and_filter_blocks is true and the below is true, then "
        "filter and index blocks are stored in the cache, but a reference is "
        "held in the \"table reader\" object so the blocks are pinned and only "
        "evicted from cache when the table reader is freed.");
    ddebug_replica("rocksdb_pin_l0_filter_and_index_blocks_in_cache = {}",
                   tbl_opts.pin_l0_filter_and_index_blocks_in_cache);

    // Bloom filter configurations.
    bool disable_bloom_filter = dsn_config_get_value_bool(
        "pegasus.server", "rocksdb_disable_bloom_filter", false, "Whether to disable bloom filter");
    if (!disable_bloom_filter) {
        // average bits allocated per key in bloom filter.
        // bits_per_key    |           false positive rate
        //                 | format_version < 5 | format_version = 5
        //       6                5.70953              5.69888
        //       8                2.45766              2.29709
        //      10                1.13977              0.959254
        //      12                0.662498             0.411593
        //      16                0.353023             0.0873754
        //      24                0.261552             0.0060971
        //      50                0.225453             ~0.00003
        // Recommend using no more than three decimal digits after the decimal point, as in 6.667.
        // More details: https://github.com/facebook/rocksdb/wiki/RocksDB-Bloom-Filter
        double bits_per_key =
            dsn_config_get_value_double("pegasus.server",
                                        "rocksdb_bloom_filter_bits_per_key",
                                        10,
                                        "average bits allocated per key in bloom filter");
        // COMPATIBILITY ATTENTION:
        // Although old releases would see the new structure as corrupt filter data and read the
        // table as if there's no filter, we've decided only to enable the new Bloom filter with new
        // format_version=5. This provides a smooth path for automatic adoption over time, with an
        // option for early opt-in.
        // Reference from rocksdb commit:
        // https://github.com/facebook/rocksdb/commit/f059c7d9b96300091e07429a60f4ad55dac84859
        int format_version =
            (int)dsn_config_get_value_int64("pegasus.server",
                                            "rocksdb_format_version",
                                            2,
                                            "block based table data format version, "
                                            "only 2 and 5 is supported in Pegasus. "
                                            "2 is the old version, 5 is the new "
                                            "version supported since rocksdb "
                                            "v6.6.4");
        dassert(format_version == 2 || format_version == 5,
                "[pegasus.server]rocksdb_format_version should be either '2' or '5'.");
        tbl_opts.format_version = format_version;
        tbl_opts.filter_policy.reset(rocksdb::NewBloomFilterPolicy(bits_per_key, false));

        std::string filter_type =
            dsn_config_get_value_string("pegasus.server",
                                        "rocksdb_filter_type",
                                        "prefix",
                                        "Bloom filter type, should be either 'common' or 'prefix'");
        dassert(filter_type == "common" || filter_type == "prefix",
                "[pegasus.server]rocksdb_filter_type should be either 'common' or 'prefix'.");
        if (filter_type == "prefix") {
            _data_cf_opts.prefix_extractor.reset(new HashkeyTransform());
            _data_cf_opts.memtable_prefix_bloom_size_ratio = 0.1;

            _data_cf_rd_opts.prefix_same_as_start = true;
        }
    }

    _data_cf_opts.table_factory.reset(NewBlockBasedTableFactory(tbl_opts));
    _meta_cf_opts.table_factory.reset(NewBlockBasedTableFactory(tbl_opts));

    _key_ttl_compaction_filter_factory = std::make_shared<KeyWithTTLCompactionFilterFactory>();
    _data_cf_opts.compaction_filter_factory = _key_ttl_compaction_filter_factory;

    // get the checkpoint reserve options.
    _checkpoint_reserve_min_count_in_config = (uint32_t)dsn_config_get_value_uint64(
        "pegasus.server", "checkpoint_reserve_min_count", 2, "checkpoint_reserve_min_count");
    _checkpoint_reserve_min_count = _checkpoint_reserve_min_count_in_config;
    _checkpoint_reserve_time_seconds_in_config =
        (uint32_t)dsn_config_get_value_uint64("pegasus.server",
                                              "checkpoint_reserve_time_seconds",
                                              1800,
                                              "checkpoint_reserve_time_seconds, 0 means no check");
    _checkpoint_reserve_time_seconds = _checkpoint_reserve_time_seconds_in_config;

    _update_rdb_stat_interval = std::chrono::seconds(dsn_config_get_value_uint64(
        "pegasus.server", "update_rdb_stat_interval", 600, "update_rdb_stat_interval, in seconds"));

    // TODO: move the qps/latency counters and it's statistics to replication_app_base layer
    std::string str_gpid = _gpid.to_string();
    char name[256];

    // register the perf counters
    snprintf(name, 255, "get_qps@%s", str_gpid.c_str());
    _pfc_get_qps.init_app_counter(
        "app.pegasus", name, COUNTER_TYPE_RATE, "statistic the qps of GET request");

    snprintf(name, 255, "multi_get_qps@%s", str_gpid.c_str());
    _pfc_multi_get_qps.init_app_counter(
        "app.pegasus", name, COUNTER_TYPE_RATE, "statistic the qps of MULTI_GET request");

    snprintf(name, 255, "scan_qps@%s", str_gpid.c_str());
    _pfc_scan_qps.init_app_counter(
        "app.pegasus", name, COUNTER_TYPE_RATE, "statistic the qps of SCAN request");

    snprintf(name, 255, "get_latency@%s", str_gpid.c_str());
    _pfc_get_latency.init_app_counter("app.pegasus",
                                      name,
                                      COUNTER_TYPE_NUMBER_PERCENTILES,
                                      "statistic the latency of GET request");

    snprintf(name, 255, "multi_get_latency@%s", str_gpid.c_str());
    _pfc_multi_get_latency.init_app_counter("app.pegasus",
                                            name,
                                            COUNTER_TYPE_NUMBER_PERCENTILES,
                                            "statistic the latency of MULTI_GET request");

    snprintf(name, 255, "scan_latency@%s", str_gpid.c_str());
    _pfc_scan_latency.init_app_counter("app.pegasus",
                                       name,
                                       COUNTER_TYPE_NUMBER_PERCENTILES,
                                       "statistic the latency of SCAN request");

    snprintf(name, 255, "recent.expire.count@%s", str_gpid.c_str());
    _pfc_recent_expire_count.init_app_counter("app.pegasus",
                                              name,
                                              COUNTER_TYPE_VOLATILE_NUMBER,
                                              "statistic the recent expired value read count");

    snprintf(name, 255, "recent.filter.count@%s", str_gpid.c_str());
    _pfc_recent_filter_count.init_app_counter("app.pegasus",
                                              name,
                                              COUNTER_TYPE_VOLATILE_NUMBER,
                                              "statistic the recent filtered value read count");

    snprintf(name, 255, "recent.abnormal.count@%s", str_gpid.c_str());
    _pfc_recent_abnormal_count.init_app_counter("app.pegasus",
                                                name,
                                                COUNTER_TYPE_VOLATILE_NUMBER,
                                                "statistic the recent abnormal read count");

    snprintf(name, 255, "disk.storage.sst.count@%s", str_gpid.c_str());
    _pfc_rdb_sst_count.init_app_counter(
        "app.pegasus", name, COUNTER_TYPE_NUMBER, "statistic the count of sstable files");

    snprintf(name, 255, "disk.storage.sst(MB)@%s", str_gpid.c_str());
    _pfc_rdb_sst_size.init_app_counter(
        "app.pegasus", name, COUNTER_TYPE_NUMBER, "statistic the size of sstable files");

    snprintf(name, 255, "rdb.block_cache.hit_count@%s", str_gpid.c_str());
    _pfc_rdb_block_cache_hit_count.init_app_counter(
        "app.pegasus", name, COUNTER_TYPE_NUMBER, "statistic the hit count of rocksdb block cache");

    snprintf(name, 255, "rdb.block_cache.total_count@%s", str_gpid.c_str());
    _pfc_rdb_block_cache_total_count.init_app_counter(
        "app.pegasus",
        name,
        COUNTER_TYPE_NUMBER,
        "statistic the total count of rocksdb block cache");

    // These counters are singletons on this server shared by all replicas, so we initialize
    // them only once.
    static std::once_flag flag;
    std::call_once(flag, [&]() {
        _pfc_rdb_block_cache_mem_usage.init_global_counter(
            "replica",
            "app.pegasus",
            "rdb.block_cache.memory_usage",
            COUNTER_TYPE_NUMBER,
            "statistic the memory usage of rocksdb block cache");

        _pfc_rdb_write_limiter_rate_bytes.init_global_counter(
            "replica",
            "app.pegasus",
            "rdb.write_limiter_rate_bytes",
            COUNTER_TYPE_NUMBER,
            "statistic the through bytes of rocksdb write rate limiter");
    });

    snprintf(name, 255, "rdb.index_and_filter_blocks.memory_usage@%s", str_gpid.c_str());
    _pfc_rdb_index_and_filter_blocks_mem_usage.init_app_counter(
        "app.pegasus",
        name,
        COUNTER_TYPE_NUMBER,
        "statistic the memory usage of rocksdb index and filter blocks");

    snprintf(name, 255, "rdb.memtable.memory_usage@%s", str_gpid.c_str());
    _pfc_rdb_memtable_mem_usage.init_app_counter(
        "app.pegasus", name, COUNTER_TYPE_NUMBER, "statistic the memory usage of rocksdb memtable");

    snprintf(name, 255, "rdb.estimate_num_keys@%s", str_gpid.c_str());
    _pfc_rdb_estimate_num_keys.init_app_counter(
        "app.pegasus",
        name,
        COUNTER_TYPE_NUMBER,
        "statistics the estimated number of keys inside the rocksdb");

    snprintf(name, 255, "rdb.bf_seek_negatives@%s", str_gpid.c_str());
    _pfc_rdb_bf_seek_negatives.init_app_counter("app.pegasus",
                                                name,
                                                COUNTER_TYPE_NUMBER,
                                                "statistics the number of times bloom filter was "
                                                "checked before creating iterator on a file and "
                                                "useful in avoiding iterator creation (and thus "
                                                "likely IOPs)");

    snprintf(name, 255, "rdb.bf_seek_total@%s", str_gpid.c_str());
    _pfc_rdb_bf_seek_total.init_app_counter("app.pegasus",
                                            name,
                                            COUNTER_TYPE_NUMBER,
                                            "statistics the number of times bloom filter was "
                                            "checked before creating iterator on a file");

    snprintf(name, 255, "rdb.bf_point_positive_true@%s", str_gpid.c_str());
    _pfc_rdb_bf_point_positive_true.init_app_counter(
        "app.pegasus",
        name,
        COUNTER_TYPE_NUMBER,
        "statistics the number of times bloom filter has avoided file reads, i.e., negatives");

    snprintf(name, 255, "rdb.bf_point_positive_total@%s", str_gpid.c_str());
    _pfc_rdb_bf_point_positive_total.init_app_counter(
        "app.pegasus",
        name,
        COUNTER_TYPE_NUMBER,
        "statistics the number of times bloom FullFilter has not avoided the reads");

    snprintf(name, 255, "rdb.bf_point_negatives@%s", str_gpid.c_str());
    _pfc_rdb_bf_point_negatives.init_app_counter("app.pegasus",
                                                 name,
                                                 COUNTER_TYPE_NUMBER,
                                                 "statistics the number of times bloom FullFilter "
                                                 "has not avoided the reads and data actually "
                                                 "exist");
}
} // namespace server
} // namespace pegasus
