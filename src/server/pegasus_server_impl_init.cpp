// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "pegasus_server_impl.h"

#include <dsn/utility/flags.h>
#include <rocksdb/filter_policy.h>

#include "capacity_unit_calculator.h"
#include "hashkey_transform.h"
#include "meta_store.h"
#include "pegasus_event_listener.h"
#include "pegasus_server_write.h"
#include "pegasus_store_configs.h"

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

    _verbose_log = FLAGS_rocksdb_verbose_log;
    _slow_query_threshold_ns = FLAGS_rocksdb_slow_query_threshold_ns;
    _abnormal_get_size_threshold = FLAGS_rocksdb_abnormal_get_size_threshold;
    _abnormal_multi_get_size_threshold = FLAGS_rocksdb_abnormal_multi_get_size_threshold;
    _abnormal_multi_get_iterate_count_threshold =
        FLAGS_rocksdb_abnormal_multi_get_iterate_count_threshold;

    _rng_rd_opts.multi_get_max_iteration_count = FLAGS_rocksdb_multi_get_max_iteration_size;
    _rng_rd_opts.multi_get_max_iteration_size = FLAGS_rocksdb_multi_get_max_iteration_size;
    _rng_rd_opts.rocksdb_max_iteration_count = FLAGS_rocksdb_max_iteration_count;
    _rng_rd_opts.rocksdb_iteration_threshold_time_ms_in_config =
        FLAGS_rocksdb_iteration_threshold_time_ms;
    _rng_rd_opts.rocksdb_iteration_threshold_time_ms =
        _rng_rd_opts.rocksdb_iteration_threshold_time_ms_in_config;

    // init rocksdb::DBOptions
    _db_opts.create_if_missing = true;
    // atomic flush data CF and meta CF, aim to keep consistency of 'last flushed decree' in meta CF
    // and data in data CF.
    _db_opts.atomic_flush = true;

    _db_opts.use_direct_reads = FLAGS_rocksdb_use_direct_reads;
    _db_opts.use_direct_io_for_flush_and_compaction =
        FLAGS_rocksdb_use_direct_io_for_flush_and_compaction;
    _db_opts.compaction_readahead_size = FLAGS_rocksdb_compaction_readahead_size;
    _db_opts.writable_file_max_buffer_size = FLAGS_rocksdb_writable_file_max_buffer_size;

    _statistics = rocksdb::CreateDBStatistics();
    _statistics->set_stats_level(rocksdb::kExceptDetailedTimers);
    _db_opts.statistics = _statistics;

    _db_opts.listeners.emplace_back(new pegasus_event_listener(this));

    // flush threads are shared among all rocksdb instances in one process.
    _db_opts.max_background_flushes = FLAGS_rocksdb_max_background_flushes;

    // compaction threads are shared among all rocksdb instances in one process.
    _db_opts.max_background_compactions = FLAGS_rocksdb_max_background_compactions;

    // init rocksdb::ColumnFamilyOptions for data column family
    _data_cf_opts.write_buffer_size = FLAGS_rocksdb_write_buffer_size;
    _data_cf_opts.max_write_buffer_number = FLAGS_rocksdb_max_write_buffer_number;
    _data_cf_opts.num_levels = FLAGS_rocksdb_num_levels;
    _data_cf_opts.target_file_size_base = FLAGS_rocksdb_target_file_size_base;
    _data_cf_opts.target_file_size_multiplier = FLAGS_rocksdb_target_file_size_multiplier;
    _data_cf_opts.max_bytes_for_level_base = FLAGS_rocksdb_max_bytes_for_level_base;
    _data_cf_opts.max_bytes_for_level_multiplier = FLAGS_rocksdb_max_bytes_for_level_multiplier;

    // we need set max_compaction_bytes definitely because set_usage_scenario() depends on it.
    _data_cf_opts.max_compaction_bytes = _data_cf_opts.target_file_size_base * 25;

    _data_cf_opts.level0_file_num_compaction_trigger =
        FLAGS_rocksdb_level0_file_num_compaction_trigger;
    _data_cf_opts.level0_slowdown_writes_trigger = FLAGS_rocksdb_level0_slowdown_writes_trigger;
    _data_cf_opts.level0_stop_writes_trigger = FLAGS_rocksdb_level0_stop_writes_trigger;
    std::string compression_str = FLAGS_rocksdb_compression_type;
    dassert(parse_compression_types(compression_str, _data_cf_opts.compression_per_level),
            "parse rocksdb_compression_type failed.");

    _meta_cf_opts = _data_cf_opts;
    // Set level0_file_num_compaction_trigger of meta CF as 10 to reduce frequent compaction.
    _meta_cf_opts.level0_file_num_compaction_trigger = 10;
    // Data in meta CF is very little, disable compression to save CPU load.
    dassert(parse_compression_types("none", _meta_cf_opts.compression_per_level),
            "parse rocksdb_compression_type failed.");

    rocksdb::BlockBasedTableOptions tbl_opts;
    if (FLAGS_rocksdb_disable_table_block_cache) {
        tbl_opts.no_block_cache = true;
        tbl_opts.block_restart_interval = 4;
    } else {
        // If block cache is enabled, all replicas on this server will share the same block cache
        // object. It's convenient to control the total memory used by this server, and the LRU
        // algorithm used by the block cache object can be more efficient in this way.
        static std::once_flag flag;
        std::call_once(flag, [&]() {
            uint64_t capacity = FLAGS_rocksdb_block_cache_capacity;
            // block cache num shard bits, default -1(auto)
            int num_shard_bits = FLAGS_rocksdb_block_cache_num_shard_bits;
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

    // Bloom filter configurations.
    bool disable_bloom_filter = FLAGS_rocksdb_disable_bloom_filter;
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
        double bits_per_key = FLAGS_rocksdb_bloom_filter_bits_per_key;
        // COMPATIBILITY ATTENTION:
        // Although old releases would see the new structure as corrupt filter data and read the
        // table as if there's no filter, we've decided only to enable the new Bloom filter with new
        // format_version=5. This provides a smooth path for automatic adoption over time, with an
        // option for early opt-in.
        // Reference from rocksdb commit:
        // https://github.com/facebook/rocksdb/commit/f059c7d9b96300091e07429a60f4ad55dac84859
        int format_version = FLAGS_rocksdb_format_version;
        tbl_opts.format_version = format_version;
        tbl_opts.filter_policy.reset(rocksdb::NewBloomFilterPolicy(bits_per_key, false));

        std::string filter_type = FLAGS_rocksdb_filter_type;
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
    _checkpoint_reserve_min_count_in_config = FLAGS_checkpoint_reserve_min_count;
    _checkpoint_reserve_min_count = _checkpoint_reserve_min_count_in_config;
    _checkpoint_reserve_time_seconds_in_config = FLAGS_checkpoint_reserve_time_seconds;
    _checkpoint_reserve_time_seconds = _checkpoint_reserve_time_seconds_in_config;

    _update_rdb_stat_interval = std::chrono::seconds(FLAGS_update_rdb_stat_interval);

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
