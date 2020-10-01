// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "pegasus_store_configs.h"

#include <dsn/c/api_utilities.h>

namespace pegasus {

DSN_DEFINE_bool("pegasus.server",
                rocksdb_verbose_log,
                false,
                "whether to print verbose log for debugging");

DSN_DEFINE_uint64("pegasus.server",
                  rocksdb_slow_query_threshold_ns,
                  100000000,
                  "get/multi-get operation duration exceed this threshold will be logged");

DSN_DEFINE_uint64(
    "pegasus.server",
    rocksdb_abnormal_get_size_threshold,
    1000000,
    "get operation value size exceed this threshold will be logged, 0 means no check");

DSN_DEFINE_uint64("pegasus.server",
                  rocksdb_abnormal_multi_get_size_threshold,
                  10000000,
                  "multi-get operation total key-value size exceed this "
                  "threshold will be logged, 0 means no check");

DSN_DEFINE_uint64(
    "pegasus.server",
    rocksdb_abnormal_multi_get_iterate_count_threshold,
    1000,
    "multi-get operation iterate count exceed this threshold will be logged, 0 means no check");

DSN_DEFINE_uint32("pegasus.server",
                  rocksdb_multi_get_max_iteration_count,
                  3000,
                  "max iteration count for each range read for multi-get operation, if "
                  "exceed this threshold,"
                  "iterator will be stopped");

DSN_DEFINE_uint64("pegasus.server",
                  rocksdb_multi_get_max_iteration_size,
                  30 << 20,
                  "multi-get operation total key-value size exceed "
                  "this threshold will stop iterating rocksdb, 0 means no check");

DSN_DEFINE_uint32("pegasus.server",
                  rocksdb_max_iteration_count,
                  1000,
                  "max iteration count for each range "
                  "read, if exceed this threshold, "
                  "iterator will be stopped");

DSN_DEFINE_uint64(
    "pegasus.server",
    rocksdb_iteration_threshold_time_ms,
    30000,
    "max duration for handling one pegasus scan request(sortkey_count/multiget/scan) if exceed "
    "this threshold, iterator will be stopped, 0 means no check");

DSN_DEFINE_bool("pegasus.server",
                rocksdb_use_direct_reads,
                false,
                "rocksdb options.use_direct_reads");

DSN_DEFINE_bool("pegasus.server",
                rocksdb_use_direct_io_for_flush_and_compaction,
                false,
                "rocksdb options.use_direct_io_for_flush_and_compaction");

DSN_DEFINE_uint64("pegasus.server",
                  rocksdb_compaction_readahead_size,
                  2 * 1024 * 1024,
                  "rocksdb options.compaction_readahead_size");

DSN_DEFINE_uint64("pegasus.server",
                  rocksdb_writable_file_max_buffer_size,
                  1024 * 1024,
                  "rocksdb options.writable_file_max_buffer_size");

DSN_DEFINE_uint32("pegasus.server",
                  rocksdb_max_background_flushes,
                  4,
                  "rocksdb options.max_background_flushes");

DSN_DEFINE_uint32("pegasus.server",
                  rocksdb_max_background_compactions,
                  12,
                  "rocksdb options.max_background_compactions");

DSN_DEFINE_uint64("pegasus.server",
                  rocksdb_write_buffer_size,
                  64 * 1024 * 1024,
                  "rocksdb options.write_buffer_size");

DSN_DEFINE_uint32("pegasus.server",
                  rocksdb_max_write_buffer_number,
                  3,
                  "rocksdb options.max_write_buffer_number");

DSN_DEFINE_uint32("pegasus.server", rocksdb_num_levels, 6, "rocksdb options.num_levels");

DSN_DEFINE_uint64("pegasus.server",
                  rocksdb_target_file_size_base,
                  64 * 1024 * 1024,
                  "rocksdb options.target_file_size_base");

DSN_DEFINE_uint32("pegasus.server",
                  rocksdb_target_file_size_multiplier,
                  1,
                  "rocksdb options.target_file_size_multiplier");

DSN_DEFINE_uint64("pegasus.server",
                  rocksdb_max_bytes_for_level_base,
                  10 * 64 * 1024 * 1024,
                  "rocksdb options.max_bytes_for_level_base");

DSN_DEFINE_double("pegasus.server",
                  rocksdb_max_bytes_for_level_multiplier,
                  10,
                  "rocksdb options.max_bytes_for_level_multiplier");

DSN_DEFINE_uint32("pegasus.server",
                  rocksdb_level0_file_num_compaction_trigger,
                  4,
                  "rocksdb options.level0_file_num_compaction_trigger");

DSN_DEFINE_uint32("pegasus.server",
                  rocksdb_level0_slowdown_writes_trigger,
                  30,
                  "rocksdb options.level0_slowdown_writes_trigger");

DSN_DEFINE_uint32("pegasus.server",
                  rocksdb_level0_stop_writes_trigger,
                  60,
                  "rocksdb options.level0_stop_writes_trigger");

DSN_DEFINE_string(
    "pegasus.server",
    rocksdb_compression_type,
    "lz4",
    "rocksdb options.compression. Available config: '[none|snappy|zstd|lz4]' "
    "for all level 2 and higher levels, and "
    "'per_level:[none|snappy|zstd|lz4],[none|snappy|zstd|lz4],...' for each level 0,1,..., the "
    "last compression type will be used for levels not specified in the list.");

DSN_DEFINE_bool("pegasus.server",
                rocksdb_disable_table_block_cache,
                false,
                "rocksdb tbl_opts.no_block_cache");

DSN_DEFINE_uint64("pegasus.server",
                  rocksdb_block_cache_capacity,
                  10 * 1024 * 1024 * 1024ULL,
                  "block cache capacity for one pegasus server, shared by all rocksdb instances");

DSN_DEFINE_int32("pegasus.server",
                 rocksdb_block_cache_num_shard_bits,
                 -1,
                 "block cache will be sharded into 2^num_shard_bits shards");

DSN_DEFINE_bool("pegasus.server",
                rocksdb_disable_bloom_filter,
                false,
                "Whether to disable bloom filter");

DSN_DEFINE_double("pegasus.server",
                  rocksdb_bloom_filter_bits_per_key,
                  10,
                  "average bits allocated per key in bloom filter");

DSN_DEFINE_uint32("pegasus.server",
                  rocksdb_format_version,
                  2,
                  "block based table data format version, "
                  "only 2 and 5 is supported in Pegasus. "
                  "2 is the old version, 5 is the new "
                  "version supported since rocksdb "
                  "v6.6.4");
DSN_DEFINE_validator(rocksdb_format_version,
                     [](uint32_t version) -> bool { return version == 2 || version == 5; });

DSN_DEFINE_string("pegasus.server",
                  rocksdb_filter_type,
                  "prefix",
                  "Bloom filter type, should be either 'common' or 'prefix'");
DSN_DEFINE_validator(rocksdb_filter_type,
                     [](std::string type) -> bool { return type == "common" || type == "prefix"; });

DSN_DEFINE_uint32("pegasus.server",
                  checkpoint_reserve_min_count,
                  2,
                  "checkpoint_reserve_min_count");

DSN_DEFINE_uint64("pegasus.server",
                  checkpoint_reserve_time_seconds,
                  1800,
                  "checkpoint_reserve_time_seconds, 0 means no check");

DSN_DEFINE_uint64("pegasus.server",
                  update_rdb_stat_interval,
                  600,
                  "update_rdb_stat_interval, in seconds");

} // namespace pegasus
