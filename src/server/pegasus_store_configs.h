// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dsn/utility/flags.h>

namespace pegasus {

DSN_DECLARE_bool(rocksdb_verbose_log);

DSN_DECLARE_uint64(rocksdb_slow_query_threshold_ns);

DSN_DECLARE_uint64(rocksdb_abnormal_get_size_threshold);

DSN_DECLARE_uint64(rocksdb_abnormal_multi_get_size_threshold);

DSN_DECLARE_uint64(rocksdb_abnormal_multi_get_iterate_count_threshold);

DSN_DECLARE_uint32(rocksdb_multi_get_max_iteration_count);

DSN_DECLARE_uint64(rocksdb_multi_get_max_iteration_size);

DSN_DECLARE_uint32(rocksdb_max_iteration_count);

DSN_DECLARE_uint64(rocksdb_iteration_threshold_time_ms);

DSN_DECLARE_uint64(rocksdb_iteration_threshold_time_ms);

DSN_DECLARE_bool(rocksdb_use_direct_reads);

DSN_DECLARE_bool(rocksdb_use_direct_io_for_flush_and_compaction);

DSN_DECLARE_uint64(rocksdb_compaction_readahead_size);

DSN_DECLARE_uint64(rocksdb_writable_file_max_buffer_size);

DSN_DECLARE_uint32(rocksdb_max_background_flushes);

DSN_DECLARE_uint32(rocksdb_max_background_compactions);

DSN_DECLARE_uint64(rocksdb_write_buffer_size);

DSN_DECLARE_uint32(rocksdb_max_write_buffer_number);

DSN_DECLARE_uint32(rocksdb_num_levels);

DSN_DECLARE_uint64(rocksdb_target_file_size_base);

DSN_DECLARE_uint32(rocksdb_target_file_size_multiplier);

DSN_DECLARE_uint64(rocksdb_max_bytes_for_level_base);

DSN_DECLARE_double(rocksdb_max_bytes_for_level_multiplier);

DSN_DECLARE_uint32(rocksdb_level0_file_num_compaction_trigger);

DSN_DECLARE_uint32(rocksdb_level0_slowdown_writes_trigger);

DSN_DECLARE_uint32(rocksdb_level0_stop_writes_trigger);

DSN_DECLARE_string(rocksdb_compression_type);

DSN_DECLARE_bool(rocksdb_disable_table_block_cache);

DSN_DECLARE_uint64(rocksdb_block_cache_capacity);

DSN_DECLARE_int32(rocksdb_block_cache_num_shard_bits);

DSN_DECLARE_bool(rocksdb_disable_bloom_filter);

DSN_DECLARE_double(rocksdb_bloom_filter_bits_per_key);

DSN_DECLARE_uint32(rocksdb_format_version);

DSN_DECLARE_string(rocksdb_filter_type);

DSN_DECLARE_uint32(checkpoint_reserve_min_count);

DSN_DECLARE_uint64(checkpoint_reserve_time_seconds);

DSN_DECLARE_uint64(update_rdb_stat_interval);

} // namespace pegasus
