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

#pragma once

#include <getopt.h>
#include <thread>
#include <iomanip>
#include <fstream>
#include <queue>
#include <boost/algorithm/string.hpp>
#include <rocksdb/db.h>
#include <rocksdb/sst_dump_tool.h>
#include <rocksdb/env.h>
#include <rocksdb/statistics.h>
#include <dsn/cpp/json_helper.h>
#include <dsn/dist/remote_command.h>
#include <dsn/dist/replication/replication_ddl_client.h>
#include <dsn/dist/replication/mutation_log_tool.h>
#include <dsn/perf_counter/perf_counter_utils.h>
#include <dsn/utility/string_view.h>
#include <dsn/utils/time_utils.h>

#include <rrdb/rrdb.code.definition.h>
#include <rrdb/rrdb_types.h>
#include <pegasus/version.h>
#include <pegasus/git_commit.h>
#include <pegasus/error.h>
#include <geo/lib/geo_client.h>

#include "base/pegasus_key_schema.h"
#include "base/pegasus_value_schema.h"
#include "base/pegasus_utils.h"

#include "command_executor.h"
#include "command_utils.h"

using namespace dsn::replication;

#define STR_I(var) #var
#define STR(var) STR_I(var)
#ifndef DSN_BUILD_TYPE
#define PEGASUS_BUILD_TYPE ""
#else
#define PEGASUS_BUILD_TYPE STR(DSN_BUILD_TYPE)
#endif

DEFINE_TASK_CODE(LPC_SCAN_DATA, TASK_PRIORITY_COMMON, ::dsn::THREAD_POOL_DEFAULT)
enum scan_data_operator
{
    SCAN_COPY,
    SCAN_CLEAR,
    SCAN_COUNT,
    SCAN_GEN_GEO
};
class top_container
{
public:
    struct top_heap_item
    {
        std::string hash_key;
        std::string sort_key;
        long row_size;
        top_heap_item(std::string &&hash_key_, std::string &&sort_key_, long row_size_)
            : hash_key(std::move(hash_key_)), sort_key(std::move(sort_key_)), row_size(row_size_)
        {
        }
    };
    struct top_heap_compare
    {
        bool operator()(top_heap_item i1, top_heap_item i2) { return i1.row_size < i2.row_size; }
    };
    typedef std::priority_queue<top_heap_item, std::vector<top_heap_item>, top_heap_compare>
        top_heap;

    top_container(int count) : _count(count) {}

    void push(std::string &&hash_key, std::string &&sort_key, long row_size)
    {
        dsn::utils::auto_lock<dsn::utils::ex_lock_nr> l(_lock);
        if (_heap.size() < _count) {
            _heap.emplace(std::move(hash_key), std::move(sort_key), row_size);
        } else {
            const top_heap_item &top = _heap.top();
            if (top.row_size < row_size) {
                _heap.pop();
                _heap.emplace(std::move(hash_key), std::move(sort_key), row_size);
            }
        }
    }

    top_heap &all() { return _heap; }

private:
    int _count;
    top_heap _heap;
    dsn::utils::ex_lock_nr _lock;
};

enum class histogram_type
{
    HASH_KEY_SIZE,
    SORT_KEY_SIZE,
    VALUE_SIZE,
    ROW_SIZE
};

struct scan_data_context
{
    scan_data_operator op;
    int split_id;
    int max_batch_count;
    int timeout_ms;
    bool no_overwrite; // if set true, then use check_and_set() instead of set()
                       // when inserting data to destination table for copy_data,
                       // to not overwrite old data if it aleady exist.
    pegasus::pegasus_client::filter_type sort_key_filter_type;
    std::string sort_key_filter_pattern;
    pegasus::pegasus_client::filter_type value_filter_type;
    std::string value_filter_pattern;
    pegasus::pegasus_client::pegasus_scanner_wrapper scanner;
    pegasus::pegasus_client *client;
    pegasus::geo::geo_client *geoclient;
    std::atomic_bool *error_occurred;
    std::atomic_long split_rows;
    std::atomic_long split_request_count;
    std::atomic_bool split_completed;
    bool stat_size;
    std::shared_ptr<rocksdb::Statistics> statistics;
    int top_count;
    top_container top_rows;
    bool count_hash_key;
    std::string last_hash_key;
    std::atomic_long split_hash_key_count;
    scan_data_context(scan_data_operator op_,
                      int split_id_,
                      int max_batch_count_,
                      int timeout_ms_,
                      pegasus::pegasus_client::pegasus_scanner_wrapper scanner_,
                      pegasus::pegasus_client *client_,
                      pegasus::geo::geo_client *geoclient_,
                      std::atomic_bool *error_occurred_,
                      bool stat_size_ = false,
                      std::shared_ptr<rocksdb::Statistics> statistics_ = nullptr,
                      int top_count_ = 0,
                      bool count_hash_key_ = false)
        : op(op_),
          split_id(split_id_),
          max_batch_count(max_batch_count_),
          timeout_ms(timeout_ms_),
          no_overwrite(false),
          sort_key_filter_type(pegasus::pegasus_client::FT_NO_FILTER),
          value_filter_type(pegasus::pegasus_client::FT_NO_FILTER),
          scanner(scanner_),
          client(client_),
          geoclient(geoclient_),
          error_occurred(error_occurred_),
          split_rows(0),
          split_request_count(0),
          split_completed(false),
          stat_size(stat_size_),
          statistics(statistics_),
          top_count(top_count_),
          top_rows(top_count_),
          count_hash_key(count_hash_key_),
          split_hash_key_count(0)
    {
        // max_batch_count should > 1 because scan may be terminated
        // when split_request_count = 1
        dassert(max_batch_count > 1, "");
    }
    void set_sort_key_filter(pegasus::pegasus_client::filter_type type, const std::string &pattern)
    {
        sort_key_filter_type = type;
        sort_key_filter_pattern = pattern;
    }
    void set_value_filter(pegasus::pegasus_client::filter_type type, const std::string &pattern)
    {
        value_filter_type = type;
        value_filter_pattern = pattern;
    }
    void set_no_overwrite() { no_overwrite = true; }
};
inline void update_atomic_max(std::atomic_long &max, long value)
{
    while (true) {
        long old = max.load();
        if (value <= old || max.compare_exchange_weak(old, value)) {
            break;
        }
    }
}
inline pegasus::pegasus_client::filter_type parse_filter_type(const std::string &name,
                                                              bool include_exact)
{
    if (include_exact && name == "exact")
        return pegasus::pegasus_client::FT_MATCH_EXACT;
    else
        return (pegasus::pegasus_client::filter_type)type_from_string(
            dsn::apps::_filter_type_VALUES_TO_NAMES,
            std::string("ft_match_") + name,
            ::dsn::apps::filter_type::FT_NO_FILTER);
}
// return true if the data is valid for the filter
inline bool validate_filter(pegasus::pegasus_client::filter_type filter_type,
                            const std::string &filter_pattern,
                            const std::string &value)
{
    switch (filter_type) {
    case pegasus::pegasus_client::FT_NO_FILTER:
        return true;
    case pegasus::pegasus_client::FT_MATCH_EXACT:
        return filter_pattern == value;
    case pegasus::pegasus_client::FT_MATCH_ANYWHERE:
    case pegasus::pegasus_client::FT_MATCH_PREFIX:
    case pegasus::pegasus_client::FT_MATCH_POSTFIX: {
        if (filter_pattern.length() == 0)
            return true;
        if (value.length() < filter_pattern.length())
            return false;
        if (filter_type == pegasus::pegasus_client::FT_MATCH_ANYWHERE) {
            return dsn::string_view(value).find(filter_pattern) != dsn::string_view::npos;
        } else if (filter_type == pegasus::pegasus_client::FT_MATCH_PREFIX) {
            return ::memcmp(value.data(), filter_pattern.data(), filter_pattern.length()) == 0;
        } else { // filter_type == pegasus::pegasus_client::FT_MATCH_POSTFIX
            return ::memcmp(value.data() + value.length() - filter_pattern.length(),
                            filter_pattern.data(),
                            filter_pattern.length()) == 0;
        }
    }
    default:
        dassert(false, "unsupported filter type: %d", filter_type);
    }
    return false;
}
// return true if the data is valid for the filter
inline bool
validate_filter(scan_data_context *context, const std::string &sort_key, const std::string &value)
{
    // for sort key, we only need to check MATCH_EXACT, because it is not supported
    // on the server side, but MATCH_PREFIX is already satisified.
    if (context->sort_key_filter_type == pegasus::pegasus_client::FT_MATCH_EXACT &&
        sort_key.length() > context->sort_key_filter_pattern.length())
        return false;
    return validate_filter(context->value_filter_type, context->value_filter_pattern, value);
}

inline int compute_ttl_seconds(uint32_t expire_ts_seconds, bool &ts_expired)
{
    auto epoch_now = pegasus::utils::epoch_now();
    ts_expired = pegasus::check_if_ts_expired(epoch_now, expire_ts_seconds);
    if (expire_ts_seconds > 0 && !ts_expired) {
        return static_cast<int>(expire_ts_seconds - epoch_now);
    }
    return 0;
}

inline void scan_data_next(scan_data_context *context)
{
    while (!context->split_completed.load() && !context->error_occurred->load() &&
           context->split_request_count.load() < context->max_batch_count) {
        context->split_request_count++;
        context->scanner->async_next([context](int ret,
                                               std::string &&hash_key,
                                               std::string &&sort_key,
                                               std::string &&value,
                                               pegasus::pegasus_client::internal_info &&info,
                                               uint32_t expire_ts_seconds) {
            if (ret == pegasus::PERR_OK) {
                if (validate_filter(context, sort_key, value)) {
                    bool ts_expired = false;
                    int ttl_seconds = 0;
                    switch (context->op) {
                    case SCAN_COPY:
                        context->split_request_count++;
                        ttl_seconds = compute_ttl_seconds(expire_ts_seconds, ts_expired);
                        if (ts_expired) {
                            scan_data_next(context);
                        } else if (context->no_overwrite) {
                            auto callback = [context](
                                int err,
                                pegasus::pegasus_client::check_and_set_results &&results,
                                pegasus::pegasus_client::internal_info &&info) {
                                if (err != pegasus::PERR_OK) {
                                    if (!context->split_completed.exchange(true)) {
                                        fprintf(stderr,
                                                "ERROR: split[%d] async check and set failed: %s\n",
                                                context->split_id,
                                                context->client->get_error_string(err));
                                        context->error_occurred->store(true);
                                    }
                                } else {
                                    if (results.set_succeed) {
                                        context->split_rows++;
                                    }
                                    scan_data_next(context);
                                }
                                // should put "split_request_count--" at end of the scope,
                                // to prevent that split_request_count becomes 0 in the middle.
                                context->split_request_count--;
                            };
                            pegasus::pegasus_client::check_and_set_options options;
                            options.set_value_ttl_seconds = ttl_seconds;
                            context->client->async_check_and_set(
                                hash_key,
                                sort_key,
                                pegasus::pegasus_client::cas_check_type::CT_VALUE_NOT_EXIST,
                                "",
                                sort_key,
                                value,
                                options,
                                std::move(callback),
                                context->timeout_ms);
                        } else {
                            auto callback =
                                [context](int err, pegasus::pegasus_client::internal_info &&info) {
                                    if (err != pegasus::PERR_OK) {
                                        if (!context->split_completed.exchange(true)) {
                                            fprintf(stderr,
                                                    "ERROR: split[%d] async set failed: %s\n",
                                                    context->split_id,
                                                    context->client->get_error_string(err));
                                            context->error_occurred->store(true);
                                        }
                                    } else {
                                        context->split_rows++;
                                        scan_data_next(context);
                                    }
                                    // should put "split_request_count--" at end of the scope,
                                    // to prevent that split_request_count becomes 0 in the middle.
                                    context->split_request_count--;
                                };
                            context->client->async_set(hash_key,
                                                       sort_key,
                                                       value,
                                                       std::move(callback),
                                                       context->timeout_ms,
                                                       ttl_seconds);
                        }
                        break;
                    case SCAN_CLEAR:
                        context->split_request_count++;
                        context->client->async_del(
                            hash_key,
                            sort_key,
                            [context](int err, pegasus::pegasus_client::internal_info &&info) {
                                if (err != pegasus::PERR_OK) {
                                    if (!context->split_completed.exchange(true)) {
                                        fprintf(stderr,
                                                "ERROR: split[%d] async del failed: %s\n",
                                                context->split_id,
                                                context->client->get_error_string(err));
                                        context->error_occurred->store(true);
                                    }
                                } else {
                                    context->split_rows++;
                                    scan_data_next(context);
                                }
                                // should put "split_request_count--" at end of the scope,
                                // to prevent that split_request_count becomes 0 in the middle.
                                context->split_request_count--;
                            },
                            context->timeout_ms);
                        break;
                    case SCAN_COUNT:
                        context->split_rows++;
                        if (context->stat_size && context->statistics) {
                            long hash_key_size = hash_key.size();
                            context->statistics->measureTime(
                                static_cast<uint32_t>(histogram_type::HASH_KEY_SIZE),
                                hash_key_size);

                            long sort_key_size = sort_key.size();
                            context->statistics->measureTime(
                                static_cast<uint32_t>(histogram_type::SORT_KEY_SIZE),
                                sort_key_size);

                            long value_size = value.size();
                            context->statistics->measureTime(
                                static_cast<uint32_t>(histogram_type::VALUE_SIZE), value_size);

                            long row_size = hash_key_size + sort_key_size + value_size;
                            context->statistics->measureTime(
                                static_cast<uint32_t>(histogram_type::ROW_SIZE), row_size);

                            if (context->top_count > 0) {
                                context->top_rows.push(
                                    std::move(hash_key), std::move(sort_key), row_size);
                            }
                        }
                        if (context->count_hash_key) {
                            if (hash_key != context->last_hash_key) {
                                context->split_hash_key_count++;
                                context->last_hash_key = std::move(hash_key);
                            }
                        }
                        scan_data_next(context);
                        break;
                    case SCAN_GEN_GEO:
                        context->split_request_count++;
                        ttl_seconds = compute_ttl_seconds(expire_ts_seconds, ts_expired);
                        if (ts_expired) {
                            scan_data_next(context);
                        } else {
                            context->geoclient->async_set(
                                hash_key,
                                sort_key,
                                value,
                                [context](int err, pegasus::pegasus_client::internal_info &&info) {
                                    if (err != pegasus::PERR_OK) {
                                        if (!context->split_completed.exchange(true)) {
                                            fprintf(stderr,
                                                    "ERROR: split[%d] async set failed: %s\n",
                                                    context->split_id,
                                                    context->client->get_error_string(err));
                                            context->error_occurred->store(true);
                                        }
                                    } else {
                                        context->split_rows++;
                                        scan_data_next(context);
                                    }
                                    // should put "split_request_count--" at end of the scope,
                                    // to prevent that split_request_count becomes 0 in the middle.
                                    context->split_request_count--;
                                },
                                context->timeout_ms,
                                ttl_seconds);
                        }
                        break;
                    default:
                        dassert(false, "op = %d", context->op);
                        break;
                    }
                } else {
                    scan_data_next(context);
                }
            } else if (ret == pegasus::PERR_SCAN_COMPLETE) {
                context->split_completed.store(true);
            } else {
                if (!context->split_completed.exchange(true)) {
                    fprintf(stderr,
                            "ERROR: split[%d] scan next failed: %s\n",
                            context->split_id,
                            context->client->get_error_string(ret));
                    context->error_occurred->store(true);
                }
            }
            // should put "split_request_count--" at end of the scope,
            // to prevent that split_request_count becomes 0 in the middle.
            context->split_request_count--;
        });

        if (context->count_hash_key) {
            // disable parallel scan if count_hash_key == true
            break;
        }
    }
}

struct node_desc
{
    std::string desc;
    dsn::rpc_address address;
    node_desc(const std::string &s, const dsn::rpc_address &n) : desc(s), address(n) {}
};
// type: all | replica-server | meta-server
inline bool fill_nodes(shell_context *sc, const std::string &type, std::vector<node_desc> &nodes)
{
    if (type == "all" || type == "meta-server") {
        for (auto &addr : sc->meta_list) {
            nodes.emplace_back("meta-server", addr);
        }
    }

    if (type == "all" || type == "replica-server") {
        std::map<dsn::rpc_address, dsn::replication::node_status::type> rs_nodes;
        ::dsn::error_code err =
            sc->ddl_client->list_nodes(dsn::replication::node_status::NS_ALIVE, rs_nodes);
        if (err != ::dsn::ERR_OK) {
            fprintf(stderr, "ERROR: list node failed: %s\n", err.to_string());
            return false;
        }
        for (auto &kv : rs_nodes) {
            nodes.emplace_back("replica-server", kv.first);
        }
    }

    return true;
}

inline std::vector<std::pair<bool, std::string>>
call_remote_command(shell_context *sc,
                    const std::vector<node_desc> &nodes,
                    const std::string &cmd,
                    const std::vector<std::string> &arguments)
{
    std::vector<std::pair<bool, std::string>> results;
    std::vector<dsn::task_ptr> tasks;
    tasks.resize(nodes.size());
    results.resize(nodes.size());
    for (int i = 0; i < nodes.size(); ++i) {
        auto callback = [&results, i](::dsn::error_code err, const std::string &resp) {
            if (err == ::dsn::ERR_OK) {
                results[i].first = true;
                results[i].second = resp;
            } else {
                results[i].first = false;
                results[i].second = err.to_string();
            }
        };
        tasks[i] = dsn::dist::cmd::async_call_remote(
            nodes[i].address, cmd, arguments, callback, std::chrono::milliseconds(5000));
    }
    for (int i = 0; i < nodes.size(); ++i) {
        tasks[i]->wait();
    }
    return results;
}

inline bool parse_app_pegasus_perf_counter_name(const std::string &name,
                                                int32_t &app_id,
                                                int32_t &partition_index,
                                                std::string &counter_name)
{
    std::string::size_type find = name.find_last_of('@');
    if (find == std::string::npos)
        return false;
    int n = sscanf(name.c_str() + find + 1, "%d.%d", &app_id, &partition_index);
    if (n != 2)
        return false;
    std::string::size_type find2 = name.find_last_of('*');
    if (find2 == std::string::npos)
        return false;
    counter_name = name.substr(find2 + 1, find - find2 - 1);
    return true;
}

inline bool parse_app_perf_counter_name(const std::string &name,
                                        std::string &app_name,
                                        std::string &counter_name)
{
    /**
     * name format:
     *   1.{node}*{section}*{counter_name}@{app_name}.{percent_line}
     *   2.{node}*{section}*{counter_name}@{app_name}
     */
    std::string::size_type find = name.find_last_of('@');
    if (find == std::string::npos)
        return false;

    std::string::size_type find2 = name.find_last_of('.');
    if (find2 == std::string::npos) {
        app_name = name.substr(find + 1);
    } else {
        app_name = name.substr(find + 1, find2 - find - 1);
    }

    std::string::size_type find3 = name.find_last_of('*');
    if (find3 == std::string::npos)
        return false;
    counter_name = name.substr(find3 + 1, find - find3 - 1);
    return true;
}

struct row_data
{
    row_data() = default;
    explicit row_data(const std::string &row_name) : row_name(row_name) {}

    double get_total_read_qps() const { return get_qps + multi_get_qps + scan_qps; }

    double get_total_write_qps() const
    {
        return put_qps + remove_qps + multi_put_qps + multi_remove_qps + check_and_set_qps +
               check_and_mutate_qps + incr_qps + duplicate_qps;
    }

    double get_total_read_bytes() const { return get_bytes + multi_get_bytes + scan_bytes; }

    double get_total_write_bytes() const
    {
        return put_bytes + multi_put_bytes + check_and_set_bytes + check_and_mutate_bytes;
    }

    void aggregate(const row_data &row)
    {
        get_qps += row.get_qps;
        multi_get_qps += row.multi_get_qps;
        put_qps += row.put_qps;
        multi_put_qps += row.multi_put_qps;
        remove_qps += row.remove_qps;
        multi_remove_qps += row.multi_remove_qps;
        incr_qps += row.incr_qps;
        check_and_set_qps += row.check_and_set_qps;
        check_and_mutate_qps += row.check_and_mutate_qps;
        scan_qps += row.scan_qps;
        duplicate_qps += row.duplicate_qps;
        dup_shipped_ops += row.dup_shipped_ops;
        dup_failed_shipping_ops += row.dup_failed_shipping_ops;
        recent_read_cu += row.recent_read_cu;
        recent_write_cu += row.recent_write_cu;
        recent_expire_count += row.recent_expire_count;
        recent_filter_count += row.recent_filter_count;
        recent_abnormal_count += row.recent_abnormal_count;
        recent_write_throttling_delay_count += row.recent_write_throttling_delay_count;
        recent_write_throttling_reject_count += row.recent_write_throttling_reject_count;
        recent_read_throttling_delay_count += row.recent_read_throttling_delay_count;
        recent_read_throttling_reject_count += row.recent_read_throttling_reject_count;
        storage_mb += row.storage_mb;
        storage_count += row.storage_count;
        rdb_block_cache_hit_count += row.rdb_block_cache_hit_count;
        rdb_block_cache_total_count += row.rdb_block_cache_total_count;
        rdb_index_and_filter_blocks_mem_usage += row.rdb_index_and_filter_blocks_mem_usage;
        rdb_memtable_mem_usage += row.rdb_memtable_mem_usage;
        rdb_estimate_num_keys += row.rdb_estimate_num_keys;
        rdb_bf_seek_negatives += row.rdb_bf_seek_negatives;
        rdb_bf_seek_total += row.rdb_bf_seek_total;
        rdb_bf_point_positive_true += row.rdb_bf_point_positive_true;
        rdb_bf_point_positive_total += row.rdb_bf_point_positive_total;
        rdb_bf_point_negatives += row.rdb_bf_point_negatives;
        backup_request_qps += row.backup_request_qps;
        backup_request_bytes += row.backup_request_bytes;
        get_bytes += row.get_bytes;
        multi_get_bytes += row.multi_get_bytes;
        scan_bytes += row.scan_bytes;
        put_bytes += row.put_bytes;
        multi_put_bytes += row.multi_put_bytes;
        check_and_set_bytes += row.check_and_set_bytes;
        check_and_mutate_bytes += row.check_and_mutate_bytes;
    }

    std::string row_name;
    int32_t app_id = 0;
    int32_t partition_count = 0;
    double get_qps = 0;
    double multi_get_qps = 0;
    double put_qps = 0;
    double multi_put_qps = 0;
    double remove_qps = 0;
    double multi_remove_qps = 0;
    double incr_qps = 0;
    double check_and_set_qps = 0;
    double check_and_mutate_qps = 0;
    double scan_qps = 0;
    double duplicate_qps = 0;
    double dup_shipped_ops = 0;
    double dup_failed_shipping_ops = 0;
    double recent_read_cu = 0;
    double recent_write_cu = 0;
    double recent_expire_count = 0;
    double recent_filter_count = 0;
    double recent_abnormal_count = 0;
    double recent_write_throttling_delay_count = 0;
    double recent_write_throttling_reject_count = 0;
    double recent_read_throttling_delay_count = 0;
    double recent_read_throttling_reject_count = 0;
    double storage_mb = 0;
    double storage_count = 0;
    double rdb_block_cache_hit_count = 0;
    double rdb_block_cache_total_count = 0;
    double rdb_index_and_filter_blocks_mem_usage = 0;
    double rdb_memtable_mem_usage = 0;
    double rdb_estimate_num_keys = 0;
    double rdb_bf_seek_negatives = 0;
    double rdb_bf_seek_total = 0;
    double rdb_bf_point_positive_true = 0;
    double rdb_bf_point_positive_total = 0;
    double rdb_bf_point_negatives = 0;
    double backup_request_qps = 0;
    double backup_request_bytes = 0;
    double get_bytes = 0;
    double multi_get_bytes = 0;
    double scan_bytes = 0;
    double put_bytes = 0;
    double multi_put_bytes = 0;
    double check_and_set_bytes = 0;
    double check_and_mutate_bytes = 0;
};

inline bool
update_app_pegasus_perf_counter(row_data &row, const std::string &counter_name, double value)
{
    if (counter_name == "get_qps")
        row.get_qps += value;
    else if (counter_name == "multi_get_qps")
        row.multi_get_qps += value;
    else if (counter_name == "put_qps")
        row.put_qps += value;
    else if (counter_name == "multi_put_qps")
        row.multi_put_qps += value;
    else if (counter_name == "remove_qps")
        row.remove_qps += value;
    else if (counter_name == "multi_remove_qps")
        row.multi_remove_qps += value;
    else if (counter_name == "incr_qps")
        row.incr_qps += value;
    else if (counter_name == "check_and_set_qps")
        row.check_and_set_qps += value;
    else if (counter_name == "check_and_mutate_qps")
        row.check_and_mutate_qps += value;
    else if (counter_name == "scan_qps")
        row.scan_qps += value;
    else if (counter_name == "duplicate_qps")
        row.duplicate_qps += value;
    else if (counter_name == "dup_shipped_ops")
        row.dup_shipped_ops += value;
    else if (counter_name == "dup_failed_shipping_ops")
        row.dup_failed_shipping_ops += value;
    else if (counter_name == "recent.read.cu")
        row.recent_read_cu += value;
    else if (counter_name == "recent.write.cu")
        row.recent_write_cu += value;
    else if (counter_name == "recent.expire.count")
        row.recent_expire_count += value;
    else if (counter_name == "recent.filter.count")
        row.recent_filter_count += value;
    else if (counter_name == "recent.abnormal.count")
        row.recent_abnormal_count += value;
    else if (counter_name == "recent.write.throttling.delay.count")
        row.recent_write_throttling_delay_count += value;
    else if (counter_name == "recent.write.throttling.reject.count")
        row.recent_write_throttling_reject_count += value;
    else if (counter_name == "recent.read.throttling.delay.count")
        row.recent_read_throttling_delay_count += value;
    else if (counter_name == "recent.read.throttling.reject.count")
        row.recent_read_throttling_reject_count += value;
    else if (counter_name == "disk.storage.sst(MB)")
        row.storage_mb += value;
    else if (counter_name == "disk.storage.sst.count")
        row.storage_count += value;
    else if (counter_name == "rdb.block_cache.hit_count")
        row.rdb_block_cache_hit_count += value;
    else if (counter_name == "rdb.block_cache.total_count")
        row.rdb_block_cache_total_count += value;
    else if (counter_name == "rdb.index_and_filter_blocks.memory_usage")
        row.rdb_index_and_filter_blocks_mem_usage += value;
    else if (counter_name == "rdb.memtable.memory_usage")
        row.rdb_memtable_mem_usage += value;
    else if (counter_name == "rdb.estimate_num_keys")
        row.rdb_estimate_num_keys += value;
    else if (counter_name == "rdb.bf_seek_negatives")
        row.rdb_bf_seek_negatives += value;
    else if (counter_name == "rdb.bf_seek_total")
        row.rdb_bf_seek_total += value;
    else if (counter_name == "rdb.bf_point_positive_true")
        row.rdb_bf_point_positive_true += value;
    else if (counter_name == "rdb.bf_point_positive_total")
        row.rdb_bf_point_positive_total += value;
    else if (counter_name == "rdb.bf_point_negatives")
        row.rdb_bf_point_negatives += value;
    else if (counter_name == "backup_request_qps")
        row.backup_request_qps += value;
    else if (counter_name == "backup_request_bytes")
        row.backup_request_bytes += value;
    else if (counter_name == "get_bytes")
        row.get_bytes += value;
    else if (counter_name == "multi_get_bytes")
        row.multi_get_bytes += value;
    else if (counter_name == "scan_bytes")
        row.scan_bytes += value;
    else if (counter_name == "put_bytes")
        row.put_bytes += value;
    else if (counter_name == "multi_put_bytes")
        row.multi_put_bytes += value;
    else if (counter_name == "check_and_set_bytes")
        row.check_and_set_bytes += value;
    else if (counter_name == "check_and_mutate_bytes")
        row.check_and_mutate_bytes += value;
    else
        return false;
    return true;
}

inline bool get_apps_and_nodes(shell_context *sc,
                               std::vector<::dsn::app_info> &apps,
                               std::vector<node_desc> &nodes)
{
    dsn::error_code err = sc->ddl_client->list_apps(dsn::app_status::AS_AVAILABLE, apps);
    if (err != dsn::ERR_OK) {
        derror("list apps failed, error = %s", err.to_string());
        return false;
    }
    if (!fill_nodes(sc, "replica-server", nodes)) {
        derror("get replica server node list failed");
        return false;
    }
    return true;
}

inline bool
get_app_partitions(shell_context *sc,
                   const std::vector<::dsn::app_info> &apps,
                   std::map<int32_t, std::vector<dsn::partition_configuration>> &app_partitions)
{
    for (const ::dsn::app_info &app : apps) {
        int32_t app_id = 0;
        int32_t partition_count = 0;
        dsn::error_code err = sc->ddl_client->list_app(
            app.app_name, app_id, partition_count, app_partitions[app.app_id]);
        if (err != ::dsn::ERR_OK) {
            derror("list app %s failed, error = %s", app.app_name.c_str(), err.to_string());
            return false;
        }
        dassert(app_id == app.app_id, "%d VS %d", app_id, app.app_id);
        dassert(partition_count == app.partition_count,
                "%d VS %d",
                partition_count,
                app.partition_count);
    }
    return true;
}

inline bool decode_node_perf_counter_info(const dsn::rpc_address &node_addr,
                                          const std::pair<bool, std::string> &result,
                                          dsn::perf_counter_info &info)
{
    if (!result.first) {
        derror("query perf counter info from node %s failed", node_addr.to_string());
        return false;
    }
    dsn::blob bb(result.second.data(), 0, result.second.size());
    if (!dsn::json::json_forwarder<dsn::perf_counter_info>::decode(bb, info)) {
        derror("decode perf counter info from node %s failed, result = %s",
               node_addr.to_string(),
               result.second.c_str());
        return false;
    }
    if (info.result != "OK") {
        derror("query perf counter info from node %s returns error, error = %s",
               node_addr.to_string(),
               info.result.c_str());
        return false;
    }
    return true;
}

// rows: key-app name, value-perf counters for each partition
inline bool get_app_partition_stat(shell_context *sc,
                                   std::map<std::string, std::vector<row_data>> &rows)
{
    // get apps and nodes
    std::vector<::dsn::app_info> apps;
    std::vector<node_desc> nodes;
    if (!get_apps_and_nodes(sc, apps, nodes)) {
        return false;
    }

    // get the relationship between app_id and app_name
    std::map<int32_t, std::string> app_id_name;
    std::map<std::string, int32_t> app_name_id;
    for (::dsn::app_info &app : apps) {
        app_id_name[app.app_id] = app.app_name;
        app_name_id[app.app_name] = app.app_id;
        rows[app.app_name].resize(app.partition_count);
    }

    // get app_id --> partitions
    std::map<int32_t, std::vector<dsn::partition_configuration>> app_partitions;
    if (!get_app_partitions(sc, apps, app_partitions)) {
        return false;
    }

    // get all of the perf counters with format ".*@.*"
    std::vector<std::pair<bool, std::string>> results =
        call_remote_command(sc, nodes, "perf-counters", {".*@.*"});

    for (int i = 0; i < nodes.size(); ++i) {
        // decode info of perf-counters on node i
        dsn::perf_counter_info info;
        if (!decode_node_perf_counter_info(nodes[i].address, results[i], info)) {
            return false;
        }

        for (dsn::perf_counter_metric &m : info.counters) {
            // get app_id/partition_id/counter_name/app_name from the name of perf-counter
            int32_t app_id_x, partition_index_x;
            std::string counter_name;
            std::string app_name;

            if (parse_app_pegasus_perf_counter_name(
                    m.name, app_id_x, partition_index_x, counter_name)) {
                // only primary partition will be counted
                auto find = app_partitions.find(app_id_x);
                if (find != app_partitions.end() &&
                    find->second[partition_index_x].primary == nodes[i].address) {
                    row_data &row = rows[app_id_name[app_id_x]][partition_index_x];
                    row.row_name = std::to_string(partition_index_x);
                    row.app_id = app_id_x;
                    update_app_pegasus_perf_counter(row, counter_name, m.value);
                }
            } else if (parse_app_perf_counter_name(m.name, app_name, counter_name)) {
                // if the app_name from perf-counter isn't existed(maybe the app was dropped), it
                // will be ignored.
                if (app_name_id.find(app_name) == app_name_id.end()) {
                    continue;
                }
                // perf-counter value will be set into partition index 0.
                row_data &row = rows[app_name][0];
                row.app_id = app_name_id[app_name];
                update_app_pegasus_perf_counter(row, counter_name, m.value);
            }
        }
    }
    return true;
}

inline bool
get_app_stat(shell_context *sc, const std::string &app_name, std::vector<row_data> &rows)
{
    std::vector<::dsn::app_info> apps;
    std::vector<node_desc> nodes;
    if (!get_apps_and_nodes(sc, apps, nodes))
        return false;

    ::dsn::app_info *app_info = nullptr;
    if (!app_name.empty()) {
        for (auto &app : apps) {
            if (app.app_name == app_name) {
                app_info = &app;
                break;
            }
        }
        if (app_info == nullptr) {
            derror("app %s not found", app_name.c_str());
            return false;
        }
    }

    std::vector<std::string> arguments;
    char tmp[256];
    if (app_name.empty()) {
        snprintf(tmp, 256, ".*@.*");
    } else {
        snprintf(tmp, 256, ".*@%d\\..*", app_info->app_id);
    }
    arguments.emplace_back(tmp);
    std::vector<std::pair<bool, std::string>> results =
        call_remote_command(sc, nodes, "perf-counters", arguments);

    if (app_name.empty()) {
        std::map<int32_t, std::vector<dsn::partition_configuration>> app_partitions;
        if (!get_app_partitions(sc, apps, app_partitions))
            return false;

        rows.resize(app_partitions.size());
        int idx = 0;
        std::map<int32_t, int> app_row_idx; // app_id --> row_idx
        for (::dsn::app_info &app : apps) {
            rows[idx].row_name = app.app_name;
            rows[idx].app_id = app.app_id;
            rows[idx].partition_count = app.partition_count;
            app_row_idx[app.app_id] = idx;
            idx++;
        }

        for (int i = 0; i < nodes.size(); ++i) {
            dsn::rpc_address node_addr = nodes[i].address;
            dsn::perf_counter_info info;
            if (!decode_node_perf_counter_info(node_addr, results[i], info))
                return false;
            for (dsn::perf_counter_metric &m : info.counters) {
                int32_t app_id_x, partition_index_x;
                std::string counter_name;
                if (!parse_app_pegasus_perf_counter_name(
                        m.name, app_id_x, partition_index_x, counter_name)) {
                    continue;
                }
                auto find = app_partitions.find(app_id_x);
                if (find == app_partitions.end())
                    continue;
                dsn::partition_configuration &pc = find->second[partition_index_x];
                if (pc.primary != node_addr)
                    continue;
                update_app_pegasus_perf_counter(rows[app_row_idx[app_id_x]], counter_name, m.value);
            }
        }
    } else {
        rows.resize(app_info->partition_count);
        for (int i = 0; i < app_info->partition_count; i++)
            rows[i].row_name = std::to_string(i);
        int32_t app_id = 0;
        int32_t partition_count = 0;
        std::vector<dsn::partition_configuration> partitions;
        dsn::error_code err =
            sc->ddl_client->list_app(app_name, app_id, partition_count, partitions);
        if (err != ::dsn::ERR_OK) {
            derror("list app %s failed, error = %s", app_name.c_str(), err.to_string());
            return false;
        }
        dassert(app_id == app_info->app_id, "%d VS %d", app_id, app_info->app_id);
        dassert(partition_count == app_info->partition_count,
                "%d VS %d",
                partition_count,
                app_info->partition_count);

        for (int i = 0; i < nodes.size(); ++i) {
            dsn::rpc_address node_addr = nodes[i].address;
            dsn::perf_counter_info info;
            if (!decode_node_perf_counter_info(node_addr, results[i], info))
                return false;
            for (dsn::perf_counter_metric &m : info.counters) {
                int32_t app_id_x, partition_index_x;
                std::string counter_name;
                bool parse_ret = parse_app_pegasus_perf_counter_name(
                    m.name, app_id_x, partition_index_x, counter_name);
                dassert(parse_ret, "name = %s", m.name.c_str());
                dassert(app_id_x == app_id, "name = %s", m.name.c_str());
                dassert(partition_index_x < partition_count, "name = %s", m.name.c_str());
                if (partitions[partition_index_x].primary != node_addr)
                    continue;
                update_app_pegasus_perf_counter(rows[partition_index_x], counter_name, m.value);
            }
        }
    }
    return true;
}

struct node_capacity_unit_stat
{
    // timestamp when node perf_counter_info has updated.
    std::string timestamp;
    std::string node_address;
    // mapping: app_id --> (read_cu, write_cu)
    std::map<int32_t, std::pair<int64_t, int64_t>> cu_value_by_app;

    std::string dump_to_json() const
    {
        std::map<int32_t, std::vector<int64_t>> values;
        for (auto &kv : cu_value_by_app) {
            auto &pair = kv.second;
            if (pair.first != 0 || pair.second != 0)
                values.emplace(kv.first, std::vector<int64_t>{pair.first, pair.second});
        }
        std::stringstream out;
        rapidjson::OStreamWrapper wrapper(out);
        dsn::json::JsonWriter writer(wrapper);
        dsn::json::json_encode(writer, values);
        return out.str();
    }
};

inline bool get_capacity_unit_stat(shell_context *sc,
                                   std::vector<node_capacity_unit_stat> &nodes_stat)
{
    std::vector<node_desc> nodes;
    if (!fill_nodes(sc, "replica-server", nodes)) {
        derror("get replica server node list failed");
        return false;
    }

    std::vector<std::pair<bool, std::string>> results =
        call_remote_command(sc, nodes, "perf-counters-by-substr", {".cu@"});

    nodes_stat.resize(nodes.size());
    for (int i = 0; i < nodes.size(); ++i) {
        dsn::rpc_address node_addr = nodes[i].address;
        dsn::perf_counter_info info;
        if (!decode_node_perf_counter_info(node_addr, results[i], info)) {
            dwarn("decode perf counter from node(%s) failed, just ignore it",
                  node_addr.to_string());
            continue;
        }
        nodes_stat[i].timestamp = info.timestamp_str;
        nodes_stat[i].node_address = node_addr.to_string();
        for (dsn::perf_counter_metric &m : info.counters) {
            int32_t app_id, pidx;
            std::string counter_name;
            bool r = parse_app_pegasus_perf_counter_name(m.name, app_id, pidx, counter_name);
            dassert(r, "name = %s", m.name.c_str());
            if (counter_name == "recent.read.cu") {
                nodes_stat[i].cu_value_by_app[app_id].first += (int64_t)m.value;
            } else if (counter_name == "recent.write.cu") {
                nodes_stat[i].cu_value_by_app[app_id].second += (int64_t)m.value;
            }
        }
    }
    return true;
}

struct app_storage_size_stat
{
    // timestamp when this stat is generated.
    std::string timestamp;
    // mapping: app_id --> [app_partition_count, stat_partition_count, storage_size_in_mb]
    std::map<int32_t, std::vector<int64_t>> st_value_by_app;

    std::string dump_to_json() const
    {
        std::stringstream out;
        rapidjson::OStreamWrapper wrapper(out);
        dsn::json::JsonWriter writer(wrapper);
        dsn::json::json_encode(writer, st_value_by_app);
        return out.str();
    }
};

inline bool get_storage_size_stat(shell_context *sc, app_storage_size_stat &st_stat)
{
    std::vector<::dsn::app_info> apps;
    std::vector<node_desc> nodes;
    if (!get_apps_and_nodes(sc, apps, nodes)) {
        derror("get apps and nodes failed");
        return false;
    }

    std::map<int32_t, std::vector<dsn::partition_configuration>> app_partitions;
    if (!get_app_partitions(sc, apps, app_partitions)) {
        derror("get app partitions failed");
        return false;
    }
    for (auto &kv : app_partitions) {
        auto &v = kv.second;
        for (auto &c : v) {
            // use partition_flags to record if this partition's storage size is calculated,
            // because `app_partitions' is a temporary variable, so we can re-use partition_flags.
            c.partition_flags = 0;
        }
    }

    std::vector<std::pair<bool, std::string>> results = call_remote_command(
        sc, nodes, "perf-counters-by-prefix", {"replica*app.pegasus*disk.storage.sst(MB)"});

    for (int i = 0; i < nodes.size(); ++i) {
        dsn::rpc_address node_addr = nodes[i].address;
        dsn::perf_counter_info info;
        if (!decode_node_perf_counter_info(node_addr, results[i], info)) {
            dwarn("decode perf counter from node(%s) failed, just ignore it",
                  node_addr.to_string());
            continue;
        }
        for (dsn::perf_counter_metric &m : info.counters) {
            int32_t app_id_x, partition_index_x;
            std::string counter_name;
            bool parse_ret = parse_app_pegasus_perf_counter_name(
                m.name, app_id_x, partition_index_x, counter_name);
            dassert(parse_ret, "name = %s", m.name.c_str());
            if (counter_name != "disk.storage.sst(MB)")
                continue;
            auto find = app_partitions.find(app_id_x);
            if (find == app_partitions.end()) // app id not found
                continue;
            dsn::partition_configuration &pc = find->second[partition_index_x];
            if (pc.primary != node_addr) // not primary replica
                continue;
            if (pc.partition_flags != 0) // already calculated
                continue;
            pc.partition_flags = 1;
            int64_t app_partition_count = find->second.size();
            auto st_it = st_stat.st_value_by_app
                             .emplace(app_id_x, std::vector<int64_t>{app_partition_count, 0, 0})
                             .first;
            st_it->second[1]++;          // stat_partition_count
            st_it->second[2] += m.value; // storage_size_in_mb
        }
    }

    char buf[20];
    dsn::utils::time_ms_to_date_time(dsn_now_ms(), buf, sizeof(buf));
    st_stat.timestamp = buf;
    return true;
}

inline configuration_proposal_action new_proposal_action(const dsn::rpc_address &target,
                                                         const dsn::rpc_address &node,
                                                         config_type::type type)
{
    configuration_proposal_action act;
    act.__set_target(target);
    act.__set_node(node);
    act.__set_type(type);
    return act;
}
