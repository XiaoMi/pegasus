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

#include "shell/commands.h"
#include "idl_utils.h"

static void
print_current_scan_state(const std::vector<std::unique_ptr<scan_data_context>> &contexts,
                         const std::string &stop_desc,
                         bool stat_size,
                         std::shared_ptr<rocksdb::Statistics> statistics,
                         bool count_hash_key);

void escape_sds_argv(int argc, sds *argv);
int mutation_check(int args_count, sds *args);
int load_mutations(shell_context *sc, pegasus::pegasus_client::mutations &mutations);

std::string unescape_str(const char *escaped);

bool data_operations(command_executor *e, shell_context *sc, arguments args)
{
    static std::map<std::string, executor> data_operations_map = {
        {"set", set_value},
        {"multi_set", multi_set_value},
        {"get", get_value},
        {"multi_get", multi_get_value},
        {"multi_get_range", multi_get_range},
        {"multi_get_sortkeys", multi_get_sortkeys},
        {"del", delete_value},
        {"multi_del", multi_del_value},
        {"multi_del_range", multi_del_range},
        {"incr", incr},
        {"check_and_set", check_and_set},
        {"check_and_mutate", check_and_mutate},
        {"exist", exist},
        {"count", sortkey_count},
        {"ttl", get_ttl},
        {"hash_scan", hash_scan},
        {"full_scan", full_scan},
        {"copy_data", copy_data},
        {"clear_data", clear_data},
        {"count_data", count_data}};

    if (args.argc <= 0) {
        return false;
    }

    auto iter = data_operations_map.find(args.argv[0]);
    dassert(iter != data_operations_map.end(), "filter should done earlier");
    executor func = iter->second;

    if (sc->current_app_name.empty()) {
        fprintf(stderr, "No app is using now\nUSAGE: use [app_name]\n");
        return true;
    }

    sc->pg_client = pegasus::pegasus_client_factory::get_client(sc->current_cluster_name.c_str(),
                                                                sc->current_app_name.c_str());
    if (sc->pg_client == nullptr) {
        fprintf(stderr,
                "get client error, cluster_name(%s), app_name(%s)\n",
                sc->current_cluster_name.c_str(),
                sc->current_app_name.c_str());
        return true;
    }

    return func(e, sc, args);
}

bool set_value(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc != 4 && args.argc != 5) {
        return false;
    }

    std::string hash_key = sds_to_string(args.argv[1]);
    std::string sort_key = sds_to_string(args.argv[2]);
    std::string value = sds_to_string(args.argv[3]);
    int32_t ttl = 0;
    if (args.argc == 5) {
        if (!dsn::buf2int32(args.argv[4], ttl)) {
            fprintf(stderr, "ERROR: parse %s as ttl failed\n", args.argv[4]);
            return false;
        }
        if (ttl <= 0) {
            fprintf(stderr, "ERROR: invalid ttl %s\n", args.argv[4]);
            return false;
        }
    }

    pegasus::pegasus_client::internal_info info;
    int ret = sc->pg_client->set(hash_key, sort_key, value, sc->timeout_ms, ttl, &info);
    if (ret != pegasus::PERR_OK) {
        fprintf(stderr, "ERROR: %s\n", sc->pg_client->get_error_string(ret));
    } else {
        fprintf(stderr, "OK\n");
    }

    fprintf(stderr, "\n");
    fprintf(stderr, "app_id          : %d\n", info.app_id);
    fprintf(stderr, "partition_index : %d\n", info.partition_index);
    fprintf(stderr, "decree          : %ld\n", info.decree);
    fprintf(stderr, "server          : %s\n", info.server.c_str());
    return true;
}

bool multi_set_value(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc < 4 || args.argc % 2 != 0) {
        return false;
    }

    std::string hash_key = sds_to_string(args.argv[1]);
    std::map<std::string, std::string> kvs;
    for (int i = 2; i < args.argc; i += 2) {
        std::string sort_key = sds_to_string(args.argv[i]);
        if (kvs.find(sort_key) != kvs.end()) {
            fprintf(stderr, "ERROR: duplicate sort key %s\n", sort_key.c_str());
            return true;
        }
        std::string value = sds_to_string(args.argv[i + 1]);
        kvs.emplace(std::move(sort_key), std::move(value));
    }
    pegasus::pegasus_client::internal_info info;

    int ret = sc->pg_client->multi_set(hash_key, kvs, sc->timeout_ms, 0, &info);
    if (ret != pegasus::PERR_OK) {
        fprintf(stderr, "ERROR: %s\n", sc->pg_client->get_error_string(ret));
    } else {
        fprintf(stderr, "OK\n");
    }

    fprintf(stderr, "\n");
    fprintf(stderr, "app_id          : %d\n", info.app_id);
    fprintf(stderr, "partition_index : %d\n", info.partition_index);
    fprintf(stderr, "decree          : %ld\n", info.decree);
    fprintf(stderr, "server          : %s\n", info.server.c_str());
    return true;
}

bool get_value(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc != 3)
        return false;
    std::string hash_key = sds_to_string(args.argv[1]);
    std::string sort_key = sds_to_string(args.argv[2]);
    std::string value;

    pegasus::pegasus_client::internal_info info;
    int ret = sc->pg_client->get(hash_key, sort_key, value, sc->timeout_ms, &info);
    if (ret != pegasus::PERR_OK) {
        if (ret == pegasus::PERR_NOT_FOUND) {
            fprintf(stderr, "Not found\n");
        } else {
            fprintf(stderr, "ERROR: %s\n", sc->pg_client->get_error_string(ret));
        }
    } else {
        fprintf(stderr, "\"%s\"\n", pegasus::utils::c_escape_string(value, sc->escape_all).c_str());
    }

    fprintf(stderr, "\n");
    fprintf(stderr, "app_id          : %d\n", info.app_id);
    fprintf(stderr, "partition_index : %d\n", info.partition_index);
    fprintf(stderr, "server          : %s\n", info.server.c_str());
    return true;
}

bool multi_get_value(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc < 2)
        return false;
    std::string hash_key = sds_to_string(args.argv[1]);
    std::set<std::string> sort_keys;
    if (args.argc > 2) {
        for (int i = 2; i < args.argc; i++) {
            std::string sort_key = sds_to_string(args.argv[i]);
            sort_keys.insert(sort_key);
        }
    }
    std::map<std::string, std::string> kvs;

    pegasus::pegasus_client::internal_info info;
    int ret = sc->pg_client->multi_get(hash_key, sort_keys, kvs, -1, -1, sc->timeout_ms, &info);
    if (ret != pegasus::PERR_OK && ret != pegasus::PERR_INCOMPLETE) {
        fprintf(stderr, "ERROR: %s\n", sc->pg_client->get_error_string(ret));
    } else {
        for (auto &kv : kvs) {
            fprintf(stderr,
                    "\"%s\" : \"%s\" => \"%s\"\n",
                    pegasus::utils::c_escape_string(hash_key, sc->escape_all).c_str(),
                    pegasus::utils::c_escape_string(kv.first, sc->escape_all).c_str(),
                    pegasus::utils::c_escape_string(kv.second, sc->escape_all).c_str());
        }
        fprintf(stderr,
                "\n%d key-value pairs got, fetch %s.\n",
                (int)kvs.size(),
                ret == pegasus::PERR_INCOMPLETE ? "not completed" : "completed");
    }

    fprintf(stderr, "\n");
    fprintf(stderr, "app_id          : %d\n", info.app_id);
    fprintf(stderr, "partition_index : %d\n", info.partition_index);
    fprintf(stderr, "server          : %s\n", info.server.c_str());
    return true;
}

bool multi_get_range(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc < 4)
        return false;

    std::string hash_key = sds_to_string(args.argv[1]);
    std::string start_sort_key = sds_to_string(args.argv[2]);
    std::string stop_sort_key = sds_to_string(args.argv[3]);
    pegasus::pegasus_client::multi_get_options options;
    std::string sort_key_filter_type_name("no_filter");
    int max_count = -1;

    static struct option long_options[] = {{"start_inclusive", required_argument, 0, 'a'},
                                           {"stop_inclusive", required_argument, 0, 'b'},
                                           {"sort_key_filter_type", required_argument, 0, 's'},
                                           {"sort_key_filter_pattern", required_argument, 0, 'y'},
                                           {"max_count", required_argument, 0, 'n'},
                                           {"no_value", no_argument, 0, 'i'},
                                           {"reverse", no_argument, 0, 'r'},
                                           {0, 0, 0, 0}};

    escape_sds_argv(args.argc, args.argv);
    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "a:b:s:y:n:ir", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'a':
            if (!dsn::buf2bool(optarg, options.start_inclusive)) {
                fprintf(stderr, "invalid start_inclusive param\n");
                return false;
            }
            break;
        case 'b':
            if (!dsn::buf2bool(optarg, options.stop_inclusive)) {
                fprintf(stderr, "invalid stop_inclusive param\n");
                return false;
            }
            break;
        case 's':
            options.sort_key_filter_type = (pegasus::pegasus_client::filter_type)type_from_string(
                ::dsn::apps::_filter_type_VALUES_TO_NAMES,
                std::string("ft_match_") + optarg,
                ::dsn::apps::filter_type::FT_NO_FILTER);
            if (options.sort_key_filter_type == pegasus::pegasus_client::FT_NO_FILTER) {
                fprintf(stderr, "invalid sort_key_filter_type param\n");
                return false;
            }
            sort_key_filter_type_name = optarg;
            break;
        case 'y':
            options.sort_key_filter_pattern = unescape_str(optarg);
            break;
        case 'n':
            if (!dsn::buf2int32(optarg, max_count)) {
                fprintf(stderr, "parse %s as max_count failed\n", optarg);
                return false;
            }
            break;
        case 'i':
            options.no_value = true;
            break;
        case 'r':
            options.reverse = true;
            break;
        default:
            return false;
        }
    }

    fprintf(stderr, "hash_key: \"%s\"\n", pegasus::utils::c_escape_string(hash_key).c_str());
    fprintf(stderr,
            "start_sort_key: \"%s\"\n",
            pegasus::utils::c_escape_string(start_sort_key).c_str());
    fprintf(stderr, "start_inclusive: %s\n", options.start_inclusive ? "true" : "false");
    fprintf(
        stderr, "stop_sort_key: \"%s\"\n", pegasus::utils::c_escape_string(stop_sort_key).c_str());
    fprintf(stderr, "stop_inclusive: %s\n", options.stop_inclusive ? "true" : "false");
    fprintf(stderr, "sort_key_filter_type: %s\n", sort_key_filter_type_name.c_str());
    if (options.sort_key_filter_type != pegasus::pegasus_client::FT_NO_FILTER) {
        fprintf(stderr,
                "sort_key_filter_pattern: \"%s\"\n",
                pegasus::utils::c_escape_string(options.sort_key_filter_pattern).c_str());
    }
    fprintf(stderr, "max_count: %d\n", max_count);
    fprintf(stderr, "no_value: %s\n", options.no_value ? "true" : "false");
    fprintf(stderr, "reverse: %s\n", options.reverse ? "true" : "false");
    fprintf(stderr, "\n");

    std::map<std::string, std::string> kvs;
    pegasus::pegasus_client::internal_info info;
    int ret = sc->pg_client->multi_get(hash_key,
                                       start_sort_key,
                                       stop_sort_key,
                                       options,
                                       kvs,
                                       max_count,
                                       -1,
                                       sc->timeout_ms,
                                       &info);
    if (ret != pegasus::PERR_OK && ret != pegasus::PERR_INCOMPLETE) {
        fprintf(stderr, "ERROR: %s\n", sc->pg_client->get_error_string(ret));
    } else {
        for (auto &kv : kvs) {
            fprintf(stderr,
                    "\"%s\" : \"%s\"",
                    pegasus::utils::c_escape_string(hash_key, sc->escape_all).c_str(),
                    pegasus::utils::c_escape_string(kv.first, sc->escape_all).c_str());
            if (!options.no_value) {
                fprintf(stderr,
                        " => \"%s\"",
                        pegasus::utils::c_escape_string(kv.second, sc->escape_all).c_str());
            }
            fprintf(stderr, "\n");
        }
        if (kvs.size() > 0) {
            fprintf(stderr, "\n");
        }
        fprintf(stderr,
                "%d key-value pairs got, fetch %s.\n",
                (int)kvs.size(),
                ret == pegasus::PERR_INCOMPLETE ? "not completed" : "completed");
    }

    fprintf(stderr, "\n");
    fprintf(stderr, "app_id          : %d\n", info.app_id);
    fprintf(stderr, "partition_index : %d\n", info.partition_index);
    fprintf(stderr, "server          : %s\n", info.server.c_str());
    return true;
}

bool multi_get_sortkeys(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc != 2)
        return false;
    std::string hash_key = sds_to_string(args.argv[1]);
    std::set<std::string> sort_keys;

    pegasus::pegasus_client::internal_info info;
    int ret = sc->pg_client->multi_get_sortkeys(hash_key, sort_keys, -1, -1, sc->timeout_ms, &info);
    if (ret != pegasus::PERR_OK && ret != pegasus::PERR_INCOMPLETE) {
        fprintf(stderr, "ERROR: %s\n", sc->pg_client->get_error_string(ret));
    } else {
        for (auto &sort_key : sort_keys) {
            fprintf(stderr,
                    "\"%s\"\n",
                    pegasus::utils::c_escape_string(sort_key, sc->escape_all).c_str());
        }
        fprintf(stderr,
                "\n%d sort keys got, fetch %s.\n",
                (int)sort_keys.size(),
                ret == pegasus::PERR_INCOMPLETE ? "not completed" : "completed");
    }

    fprintf(stderr, "\n");
    fprintf(stderr, "app_id          : %d\n", info.app_id);
    fprintf(stderr, "partition_index : %d\n", info.partition_index);
    fprintf(stderr, "server          : %s\n", info.server.c_str());
    return true;
}

bool delete_value(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc != 3) {
        return false;
    }

    std::string hash_key = sds_to_string(args.argv[1]);
    std::string sort_key = sds_to_string(args.argv[2]);
    pegasus::pegasus_client::internal_info info;
    int ret = sc->pg_client->del(hash_key, sort_key, sc->timeout_ms, &info);
    if (ret != pegasus::PERR_OK) {
        fprintf(stderr, "ERROR: %s\n", sc->pg_client->get_error_string(ret));
    } else {
        fprintf(stderr, "OK\n");
    }

    fprintf(stderr, "\n");
    fprintf(stderr, "app_id          : %d\n", info.app_id);
    fprintf(stderr, "partition_index : %d\n", info.partition_index);
    fprintf(stderr, "decree          : %ld\n", info.decree);
    fprintf(stderr, "server          : %s\n", info.server.c_str());
    return true;
}

bool multi_del_value(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc < 3)
        return false;
    std::string hash_key = sds_to_string(args.argv[1]);
    std::set<std::string> sort_keys;
    for (int i = 2; i < args.argc; i++) {
        std::string sort_key = sds_to_string(args.argv[i]);
        sort_keys.insert(sort_key);
    }

    pegasus::pegasus_client::internal_info info;
    int64_t deleted_count;
    int ret = sc->pg_client->multi_del(hash_key, sort_keys, deleted_count, sc->timeout_ms, &info);
    if (ret == pegasus::PERR_OK) {
        fprintf(stderr, "%" PRId64 " key-value pairs deleted.\n", deleted_count);
    } else if (ret == pegasus::PERR_INCOMPLETE) {
        fprintf(stderr, "%" PRId64 " key-value pairs deleted, but not completed.\n", deleted_count);
    } else {
        fprintf(stderr, "ERROR: %s\n", sc->pg_client->get_error_string(ret));
    }

    fprintf(stderr, "\n");
    fprintf(stderr, "app_id          : %d\n", info.app_id);
    fprintf(stderr, "partition_index : %d\n", info.partition_index);
    fprintf(stderr, "decree          : %ld\n", info.decree);
    fprintf(stderr, "server          : %s\n", info.server.c_str());
    return true;
}

bool multi_del_range(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc < 4)
        return false;

    std::string hash_key = sds_to_string(args.argv[1]);
    std::string start_sort_key = sds_to_string(args.argv[2]);
    std::string stop_sort_key = sds_to_string(args.argv[3]);
    pegasus::pegasus_client::scan_options options;
    options.no_value = true;
    options.timeout_ms = sc->timeout_ms;
    std::string sort_key_filter_type_name("no_filter");
    bool silent = false;
    FILE *file = stderr;
    int batch_del_count = 100;

    static struct option long_options[] = {{"start_inclusive", required_argument, 0, 'a'},
                                           {"stop_inclusive", required_argument, 0, 'b'},
                                           {"sort_key_filter_type", required_argument, 0, 's'},
                                           {"sort_key_filter_pattern", required_argument, 0, 'y'},
                                           {"output", required_argument, 0, 'o'},
                                           {"silent", no_argument, 0, 'i'},
                                           {0, 0, 0, 0}};

    escape_sds_argv(args.argc, args.argv);
    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "a:b:s:y:o:i", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'a':
            if (!dsn::buf2bool(optarg, options.start_inclusive)) {
                fprintf(stderr, "invalid start_inclusive param\n");
                return false;
            }
            break;
        case 'b':
            if (!dsn::buf2bool(optarg, options.stop_inclusive)) {
                fprintf(stderr, "invalid stop_inclusive param\n");
                return false;
            }
            break;
        case 's':
            options.sort_key_filter_type = (pegasus::pegasus_client::filter_type)type_from_string(
                ::dsn::apps::_filter_type_VALUES_TO_NAMES,
                std::string("ft_match_") + optarg,
                ::dsn::apps::filter_type::FT_NO_FILTER);
            if (options.sort_key_filter_type == pegasus::pegasus_client::FT_NO_FILTER) {
                fprintf(stderr, "invalid sort_key_filter_type param\n");
                return false;
            }
            sort_key_filter_type_name = optarg;
            break;
        case 'y':
            options.sort_key_filter_pattern = unescape_str(optarg);
            break;
        case 'o':
            file = fopen(optarg, "w");
            if (!file) {
                fprintf(stderr, "open filename %s failed\n", optarg);
                return false;
            }
            break;
        case 'i':
            silent = true;
            break;
        default:
            return false;
        }
    }

    fprintf(stderr, "hash_key: \"%s\"\n", pegasus::utils::c_escape_string(hash_key).c_str());
    fprintf(stderr,
            "start_sort_key: \"%s\"\n",
            pegasus::utils::c_escape_string(start_sort_key).c_str());
    fprintf(stderr, "start_inclusive: %s\n", options.start_inclusive ? "true" : "false");
    fprintf(
        stderr, "stop_sort_key: \"%s\"\n", pegasus::utils::c_escape_string(stop_sort_key).c_str());
    fprintf(stderr, "stop_inclusive: %s\n", options.stop_inclusive ? "true" : "false");
    fprintf(stderr, "sort_key_filter_type: %s\n", sort_key_filter_type_name.c_str());
    if (options.sort_key_filter_type != pegasus::pegasus_client::FT_NO_FILTER) {
        fprintf(stderr,
                "sort_key_filter_pattern: \"%s\"\n",
                pegasus::utils::c_escape_string(options.sort_key_filter_pattern).c_str());
    }
    fprintf(stderr, "silent: %s\n", silent ? "true" : "false");
    fprintf(stderr, "\n");

    int count = 0;
    bool error_occured = false;
    pegasus::pegasus_client::pegasus_scanner *scanner = nullptr;
    int ret = sc->pg_client->get_scanner(hash_key, start_sort_key, stop_sort_key, options, scanner);
    if (ret != pegasus::PERR_OK) {
        fprintf(file, "ERROR: get scanner failed: %s\n", sc->pg_client->get_error_string(ret));
        if (file != stderr) {
            fprintf(
                stderr, "ERROR: get scanner failed: %s\n", sc->pg_client->get_error_string(ret));
        }
        error_occured = true;
    } else {
        std::string tmp_hash_key;
        std::string sort_key;
        std::string value;
        pegasus::pegasus_client::internal_info info;
        std::set<std::string> sort_keys;
        while (true) {
            int scan_ret = scanner->next(tmp_hash_key, sort_key, value, &info);
            if (scan_ret != pegasus::PERR_SCAN_COMPLETE && scan_ret != pegasus::PERR_OK) {
                fprintf(file,
                        "ERROR: scan data failed: %s {app_id=%d, partition_index=%d, server=%s}\n",
                        sc->pg_client->get_error_string(scan_ret),
                        info.app_id,
                        info.partition_index,
                        info.server.c_str());
                if (file != stderr) {
                    fprintf(
                        stderr,
                        "ERROR: scan data failed: %s {app_id=%d, partition_index=%d, server=%s}\n",
                        sc->pg_client->get_error_string(scan_ret),
                        info.app_id,
                        info.partition_index,
                        info.server.c_str());
                }
                error_occured = true;
                break;
            }

            if (scan_ret == pegasus::PERR_OK) {
                sort_keys.emplace(std::move(sort_key));
            }

            if (sort_keys.size() > 0 &&
                (sort_keys.size() >= batch_del_count || scan_ret == pegasus::PERR_SCAN_COMPLETE)) {
                int64_t del_count;
                pegasus::pegasus_client::internal_info del_info;
                int del_ret = sc->pg_client->multi_del(
                    hash_key, sort_keys, del_count, sc->timeout_ms, &del_info);
                if (del_ret != pegasus::PERR_OK) {
                    fprintf(file,
                            "ERROR: delete data failed: %s {app_id=%d, partition_index=%d, "
                            "server=%s}\n",
                            sc->pg_client->get_error_string(del_ret),
                            del_info.app_id,
                            del_info.partition_index,
                            del_info.server.c_str());
                    if (file != stderr) {
                        fprintf(stderr,
                                "ERROR: delete data failed: %s {app_id=%d, partition_index=%d, "
                                "server=%s}\n",
                                sc->pg_client->get_error_string(del_ret),
                                del_info.app_id,
                                del_info.partition_index,
                                del_info.server.c_str());
                    }
                    error_occured = true;
                    break;
                } else {
                    count += del_count;
                    if (!silent) {
                        for (auto &k : sort_keys) {
                            fprintf(file,
                                    "Deleted: \"%s\"\n",
                                    pegasus::utils::c_escape_string(k, sc->escape_all).c_str());
                        }
                    }
                    sort_keys.clear();
                }
            }

            if (scan_ret == pegasus::PERR_SCAN_COMPLETE) {
                break;
            }
        }
    }

    if (scanner) {
        delete scanner;
    }

    if (file != stderr) {
        fclose(file);
    }

    if (error_occured) {
        fprintf(stderr, "\nTerminated for error, %d sort keys deleted.\n", count);
    } else {
        if (file == stderr && !silent && count > 0) {
            fprintf(stderr, "\n");
        }
        fprintf(stderr, "OK, %d sort keys deleted.\n", count);
    }
    return true;
}

bool incr(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc != 3 && args.argc != 4) {
        return false;
    }

    std::string hash_key = sds_to_string(args.argv[1]);
    std::string sort_key = sds_to_string(args.argv[2]);
    int64_t increment = 1;
    if (args.argc == 4) {
        if (!dsn::buf2int64(args.argv[3], increment)) {
            fprintf(stderr, "ERROR: invalid increment param\n");
            return false;
        }
    }

    int64_t new_value;
    pegasus::pegasus_client::internal_info info;
    int ret =
        sc->pg_client->incr(hash_key, sort_key, increment, new_value, sc->timeout_ms, 0, &info);
    if (ret != pegasus::PERR_OK) {
        fprintf(stderr, "ERROR: %s\n", sc->pg_client->get_error_string(ret));
    } else {
        fprintf(stderr, "%" PRId64 "\n", new_value);
    }

    fprintf(stderr, "\n");
    fprintf(stderr, "app_id          : %d\n", info.app_id);
    fprintf(stderr, "partition_index : %d\n", info.partition_index);
    fprintf(stderr, "decree          : %ld\n", info.decree);
    fprintf(stderr, "server          : %s\n", info.server.c_str());
    return true;
}

bool check_and_set(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc < 2)
        return false;

    std::string hash_key = sds_to_string(args.argv[1]);
    bool check_sort_key_provided = false;
    std::string check_sort_key;
    ::dsn::apps::cas_check_type::type check_type = ::dsn::apps::cas_check_type::CT_NO_CHECK;
    std::string check_type_name;
    bool check_operand_provided = false;
    std::string check_operand;
    bool set_sort_key_provided = false;
    std::string set_sort_key;
    bool set_value_provided = false;
    std::string set_value;
    pegasus::pegasus_client::check_and_set_options options;

    static struct option long_options[] = {{"check_sort_key", required_argument, 0, 'c'},
                                           {"check_type", required_argument, 0, 't'},
                                           {"check_operand", required_argument, 0, 'o'},
                                           {"set_sort_key", required_argument, 0, 's'},
                                           {"set_value", required_argument, 0, 'v'},
                                           {"set_value_ttl_seconds", required_argument, 0, 'l'},
                                           {"return_check_value", no_argument, 0, 'r'},
                                           {0, 0, 0, 0}};

    escape_sds_argv(args.argc, args.argv);
    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "c:t:o:s:v:l:r", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'c':
            check_sort_key_provided = true;
            check_sort_key = unescape_str(optarg);
            break;
        case 't':
            check_type = type_from_string(::dsn::apps::_cas_check_type_VALUES_TO_NAMES,
                                          std::string("ct_value_") + optarg,
                                          ::dsn::apps::cas_check_type::CT_NO_CHECK);
            if (check_type == ::dsn::apps::cas_check_type::CT_NO_CHECK) {
                fprintf(stderr, "ERROR: invalid check_type param\n");
                return false;
            }
            check_type_name = optarg;
            break;
        case 'o':
            check_operand_provided = true;
            check_operand = unescape_str(optarg);
            break;
        case 's':
            set_sort_key_provided = true;
            set_sort_key = unescape_str(optarg);
            break;
        case 'v':
            set_value_provided = true;
            set_value = unescape_str(optarg);
            break;
        case 'l':
            if (!dsn::buf2int32(optarg, options.set_value_ttl_seconds)) {
                fprintf(stderr, "ERROR: invalid set_value_ttl_seconds param\n");
                return false;
            }
            break;
        case 'r':
            options.return_check_value = true;
            break;
        default:
            return false;
        }
    }

    if (!check_sort_key_provided) {
        fprintf(stderr, "ERROR: check_sort_key not provided\n");
        return false;
    }
    if (check_type == ::dsn::apps::cas_check_type::CT_NO_CHECK) {
        fprintf(stderr, "ERROR: check_type not provided\n");
        return false;
    }
    if (!check_operand_provided && pegasus::cas_is_check_operand_needed(check_type)) {
        fprintf(stderr, "ERROR: check_operand not provided\n");
        return false;
    }
    if (!set_sort_key_provided) {
        fprintf(stderr, "ERROR: set_sort_key not provided\n");
        return false;
    }
    if (!set_value_provided) {
        fprintf(stderr, "ERROR: set_value not provided\n");
        return false;
    }

    fprintf(stderr, "hash_key: \"%s\"\n", pegasus::utils::c_escape_string(hash_key).c_str());
    fprintf(stderr,
            "check_sort_key: \"%s\"\n",
            pegasus::utils::c_escape_string(check_sort_key).c_str());
    fprintf(stderr, "check_type: %s\n", check_type_name.c_str());
    if (check_type >= ::dsn::apps::cas_check_type::CT_VALUE_MATCH_ANYWHERE) {
        fprintf(stderr,
                "check_operand: \"%s\"\n",
                pegasus::utils::c_escape_string(check_operand).c_str());
    }
    fprintf(
        stderr, "set_sort_key: \"%s\"\n", pegasus::utils::c_escape_string(set_sort_key).c_str());
    fprintf(stderr, "set_value: \"%s\"\n", pegasus::utils::c_escape_string(set_value).c_str());
    fprintf(stderr, "set_value_ttl_seconds: %d\n", options.set_value_ttl_seconds);
    fprintf(stderr, "return_check_value: %s\n", options.return_check_value ? "true" : "false");
    fprintf(stderr, "\n");

    pegasus::pegasus_client::check_and_set_results results;
    pegasus::pegasus_client::internal_info info;
    int ret = sc->pg_client->check_and_set(hash_key,
                                           check_sort_key,
                                           (pegasus::pegasus_client::cas_check_type)check_type,
                                           check_operand,
                                           set_sort_key,
                                           set_value,
                                           options,
                                           results,
                                           sc->timeout_ms,
                                           &info);
    if (ret != pegasus::PERR_OK) {
        fprintf(stderr, "ERROR: %s\n", sc->pg_client->get_error_string(ret));
    } else {
        if (results.set_succeed) {
            fprintf(stderr, "Set succeed.\n");
        } else {
            fprintf(stderr, "Set failed, because check not passed.\n");
        }
        if (results.check_value_returned) {
            fprintf(stderr, "\n");
            if (results.check_value_exist) {
                fprintf(
                    stderr,
                    "Check value: \"%s\"\n",
                    pegasus::utils::c_escape_string(results.check_value, sc->escape_all).c_str());
            } else {
                fprintf(stderr, "Check value not exist.\n");
            }
        }
    }

    fprintf(stderr, "\n");
    fprintf(stderr, "app_id          : %d\n", info.app_id);
    fprintf(stderr, "partition_index : %d\n", info.partition_index);
    fprintf(stderr, "decree          : %ld\n", info.decree);
    fprintf(stderr, "server          : %s\n", info.server.c_str());
    return true;
}

bool check_and_mutate(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc < 2)
        return false;

    std::string hash_key = sds_to_string(args.argv[1]);
    bool check_sort_key_provided = false;
    std::string check_sort_key;
    ::dsn::apps::cas_check_type::type check_type = ::dsn::apps::cas_check_type::CT_NO_CHECK;
    std::string check_type_name;
    bool check_operand_provided = false;
    std::string check_operand;
    pegasus::pegasus_client::mutations mutations;

    pegasus::pegasus_client::check_and_mutate_options options;
    static struct option long_options[] = {{"check_sort_key", required_argument, 0, 'c'},
                                           {"check_type", required_argument, 0, 't'},
                                           {"check_operand", required_argument, 0, 'o'},
                                           {"return_check_value", no_argument, 0, 'r'},
                                           {0, 0, 0, 0}};

    escape_sds_argv(args.argc, args.argv);
    std::string str;
    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "c:t:o:r", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'c':
            check_sort_key_provided = true;
            check_sort_key = unescape_str(optarg);
            break;
        case 't':
            check_type = type_from_string(::dsn::apps::_cas_check_type_VALUES_TO_NAMES,
                                          std::string("ct_value_") + optarg,
                                          ::dsn::apps::cas_check_type::CT_NO_CHECK);
            if (check_type == ::dsn::apps::cas_check_type::CT_NO_CHECK) {
                fprintf(stderr, "ERROR: invalid check_type param\n");
                return false;
            }
            check_type_name = optarg;
            break;
        case 'o':
            check_operand_provided = true;
            check_operand = unescape_str(optarg);
            break;
        case 'r':
            options.return_check_value = true;
            break;
        default:
            return false;
        }
    }

    if (!check_sort_key_provided) {
        fprintf(stderr, "ERROR: check_sort_key not provided\n");
        return false;
    }
    if (check_type == ::dsn::apps::cas_check_type::CT_NO_CHECK) {
        fprintf(stderr, "ERROR: check_type not provided\n");
        return false;
    }
    if (!check_operand_provided &&
        check_type >= ::dsn::apps::cas_check_type::CT_VALUE_MATCH_ANYWHERE) {
        fprintf(stderr, "ERROR: check_operand not provided\n");
        return false;
    }

    fprintf(stderr,
            "Load mutations, like\n"
            "  set <sort_key> <value> [ttl]\n"
            "  del <sort_key>\n"
            "Print \"ok\" to finish loading, \"abort\" to abort this command\n");
    if (load_mutations(sc, mutations)) {
        fprintf(stderr, "INFO: abort check_and_mutate command\n");
        return true;
    }
    if (mutations.is_empty()) {
        fprintf(stderr, "ERROR: mutations not provided\n");
        return false;
    }

    fprintf(stderr, "hash_key: \"%s\"\n", pegasus::utils::c_escape_string(hash_key).c_str());
    fprintf(stderr,
            "check_sort_key: \"%s\"\n",
            pegasus::utils::c_escape_string(check_sort_key).c_str());
    fprintf(stderr, "check_type: %s\n", check_type_name.c_str());
    if (check_type >= ::dsn::apps::cas_check_type::CT_VALUE_MATCH_ANYWHERE) {
        fprintf(stderr,
                "check_operand: \"%s\"\n",
                pegasus::utils::c_escape_string(check_operand).c_str());
    }
    fprintf(stderr, "return_check_value: %s\n", options.return_check_value ? "true" : "false");

    std::vector<pegasus::pegasus_client::mutate> copy_of_mutations;
    mutations.get_mutations(copy_of_mutations);
    fprintf(stderr, "mutations:\n");
    for (int i = 0; i < copy_of_mutations.size(); ++i) {
        if (copy_of_mutations[i].operation ==
            pegasus::pegasus_client::mutate::mutate_operation::MO_PUT) {
            fprintf(stderr,
                    "  mutation[%d].type: SET\n  mutation[%d].sort_key: \"%s\"\n  "
                    "mutation[%d].value: "
                    "\"%s\"\n  mutation[%d].expire_seconds: %d\n",
                    i,
                    i,
                    pegasus::utils::c_escape_string(copy_of_mutations[i].sort_key).c_str(),
                    i,
                    pegasus::utils::c_escape_string(copy_of_mutations[i].value).c_str(),
                    i,
                    copy_of_mutations[i].set_expire_ts_seconds);
        } else {
            fprintf(stderr,
                    "  mutation[%d].type: DEL\n  mutation[%d].sort_key: \"%s\"\n",
                    i,
                    i,
                    pegasus::utils::c_escape_string(copy_of_mutations[i].sort_key).c_str());
        }
    }
    fprintf(stderr, "\n");

    pegasus::pegasus_client::check_and_mutate_results results;
    pegasus::pegasus_client::internal_info info;
    int ret = sc->pg_client->check_and_mutate(hash_key,
                                              check_sort_key,
                                              (pegasus::pegasus_client::cas_check_type)check_type,
                                              check_operand,
                                              mutations,
                                              options,
                                              results,
                                              sc->timeout_ms,
                                              &info);
    if (ret != pegasus::PERR_OK) {
        fprintf(stderr, "ERROR: %s\n", sc->pg_client->get_error_string(ret));
    } else {
        if (results.mutate_succeed) {
            fprintf(stderr, "Mutate succeed.\n");
        } else {
            fprintf(stderr, "Mutate failed, because check not passed.\n");
        }
        if (results.check_value_returned) {
            fprintf(stderr, "\n");
            if (results.check_value_exist) {
                fprintf(
                    stderr,
                    "Check value: \"%s\"\n",
                    pegasus::utils::c_escape_string(results.check_value, sc->escape_all).c_str());
            } else {
                fprintf(stderr, "Check value not exist.\n");
            }
        }
    }

    fprintf(stderr, "\n");
    fprintf(stderr, "app_id          : %d\n", info.app_id);
    fprintf(stderr, "partition_index : %d\n", info.partition_index);
    fprintf(stderr, "decree          : %ld\n", info.decree);
    fprintf(stderr, "server          : %s\n", info.server.c_str());

    return true;
}

bool exist(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc != 3) {
        return false;
    }

    std::string hash_key = sds_to_string(args.argv[1]);
    std::string sort_key = sds_to_string(args.argv[2]);
    pegasus::pegasus_client::internal_info info;
    int ret = sc->pg_client->exist(hash_key, sort_key, sc->timeout_ms, &info);
    if (ret != pegasus::PERR_OK) {
        if (ret == pegasus::PERR_NOT_FOUND) {
            fprintf(stderr, "False\n");
        } else {
            fprintf(stderr, "ERROR: %s\n", sc->pg_client->get_error_string(ret));
        }
    } else {
        fprintf(stderr, "True\n");
    }

    fprintf(stderr, "\n");
    fprintf(stderr, "app_id          : %d\n", info.app_id);
    fprintf(stderr, "partition_index : %d\n", info.partition_index);
    fprintf(stderr, "server          : %s\n", info.server.c_str());
    return true;
}

bool sortkey_count(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc != 2) {
        return false;
    }

    std::string hash_key = sds_to_string(args.argv[1]);
    int64_t count;
    pegasus::pegasus_client::internal_info info;
    int ret = sc->pg_client->sortkey_count(hash_key, count, sc->timeout_ms, &info);
    if (ret != pegasus::PERR_OK) {
        fprintf(stderr, "ERROR: %s\n", sc->pg_client->get_error_string(ret));
    } else if (count == -1) {
        fprintf(stderr, "ERROR: it takes too long to count sortkey\n");
    } else {
        fprintf(stderr, "%" PRId64 "\n", count);
    }

    fprintf(stderr, "\n");
    fprintf(stderr, "app_id          : %d\n", info.app_id);
    fprintf(stderr, "partition_index : %d\n", info.partition_index);
    fprintf(stderr, "server          : %s\n", info.server.c_str());
    return true;
}

bool get_ttl(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc != 3) {
        return false;
    }

    std::string hash_key = sds_to_string(args.argv[1]);
    std::string sort_key = sds_to_string(args.argv[2]);
    int ttl_seconds;
    pegasus::pegasus_client::internal_info info;
    int ret = sc->pg_client->ttl(hash_key, sort_key, ttl_seconds, sc->timeout_ms, &info);
    if (ret != pegasus::PERR_OK) {
        if (ret == pegasus::PERR_NOT_FOUND) {
            fprintf(stderr, "Not found\n");
        } else {
            fprintf(stderr, "ERROR: %s\n", sc->pg_client->get_error_string(ret));
        }
    } else {
        if (ttl_seconds == -1) {
            fprintf(stderr, "Infinite\n");
        } else if (ttl_seconds == -2) {
            fprintf(stderr, "Not found\n");
        } else {
            fprintf(stderr, "%d\n", ttl_seconds);
        }
    }

    fprintf(stderr, "\n");
    fprintf(stderr, "app_id          : %d\n", info.app_id);
    fprintf(stderr, "partition_index : %d\n", info.partition_index);
    fprintf(stderr, "server          : %s\n", info.server.c_str());
    return true;
}

bool hash_scan(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc < 4)
        return false;

    std::string hash_key = sds_to_string(args.argv[1]);
    std::string start_sort_key = sds_to_string(args.argv[2]);
    std::string stop_sort_key = sds_to_string(args.argv[3]);

    int32_t max_count = -1;
    bool detailed = false;
    FILE *file = stderr;
    int32_t timeout_ms = sc->timeout_ms;
    std::string sort_key_filter_type_name("no_filter");
    std::string value_filter_type_name("no_filter");
    pegasus::pegasus_client::filter_type value_filter_type = pegasus::pegasus_client::FT_NO_FILTER;
    std::string value_filter_pattern;
    pegasus::pegasus_client::scan_options options;

    static struct option long_options[] = {{"detailed", no_argument, 0, 'd'},
                                           {"max_count", required_argument, 0, 'n'},
                                           {"timeout_ms", required_argument, 0, 't'},
                                           {"output", required_argument, 0, 'o'},
                                           {"start_inclusive", required_argument, 0, 'a'},
                                           {"stop_inclusive", required_argument, 0, 'b'},
                                           {"sort_key_filter_type", required_argument, 0, 's'},
                                           {"sort_key_filter_pattern", required_argument, 0, 'y'},
                                           {"value_filter_type", required_argument, 0, 'v'},
                                           {"value_filter_pattern", required_argument, 0, 'z'},
                                           {"no_value", no_argument, 0, 'i'},
                                           {0, 0, 0, 0}};

    escape_sds_argv(args.argc, args.argv);
    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "dn:t:o:a:b:s:y:v:z:i", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'd':
            detailed = true;
            break;
        case 'n':
            if (!dsn::buf2int32(optarg, max_count)) {
                fprintf(stderr, "ERROR: parse %s as max_count failed\n", optarg);
                return false;
            }
            break;
        case 't':
            if (!dsn::buf2int32(optarg, timeout_ms)) {
                fprintf(stderr, "ERROR: parse %s as timeout_ms failed\n", optarg);
                return false;
            }
            break;
        case 'o':
            file = fopen(optarg, "w");
            if (!file) {
                fprintf(stderr, "ERROR: open filename %s failed\n", optarg);
                return false;
            }
            break;
        case 'a':
            if (!dsn::buf2bool(optarg, options.start_inclusive)) {
                fprintf(stderr, "ERROR: invalid start_inclusive param\n");
                return false;
            }
            break;
        case 'b':
            if (!dsn::buf2bool(optarg, options.stop_inclusive)) {
                fprintf(stderr, "ERROR: invalid stop_inclusive param\n");
                return false;
            }
            break;
        case 's':
            options.sort_key_filter_type = parse_filter_type(optarg, false);
            if (options.sort_key_filter_type == pegasus::pegasus_client::FT_NO_FILTER) {
                fprintf(stderr, "ERROR: invalid sort_key_filter_type param\n");
                return false;
            }
            sort_key_filter_type_name = optarg;
            break;
        case 'y':
            options.sort_key_filter_pattern = unescape_str(optarg);
            break;
        case 'v':
            value_filter_type = parse_filter_type(optarg, true);
            if (value_filter_type == pegasus::pegasus_client::FT_NO_FILTER) {
                fprintf(stderr, "ERROR: invalid value_filter_type param\n");
                return false;
            }
            value_filter_type_name = optarg;
            break;
        case 'z':
            value_filter_pattern = unescape_str(optarg);
            break;
        case 'i':
            options.no_value = true;
            break;
        default:
            return false;
        }
    }

    if (value_filter_type != pegasus::pegasus_client::FT_NO_FILTER && options.no_value) {
        fprintf(stderr, "ERROR: no_value should not be set when value_filter_type is set\n");
        return false;
    }

    fprintf(stderr, "hash_key: \"%s\"\n", pegasus::utils::c_escape_string(hash_key).c_str());
    fprintf(stderr,
            "start_sort_key: \"%s\"\n",
            pegasus::utils::c_escape_string(start_sort_key).c_str());
    fprintf(stderr, "start_inclusive: %s\n", options.start_inclusive ? "true" : "false");
    fprintf(
        stderr, "stop_sort_key: \"%s\"\n", pegasus::utils::c_escape_string(stop_sort_key).c_str());
    fprintf(stderr, "stop_inclusive: %s\n", options.stop_inclusive ? "true" : "false");
    fprintf(stderr, "sort_key_filter_type: %s\n", sort_key_filter_type_name.c_str());
    if (options.sort_key_filter_type != pegasus::pegasus_client::FT_NO_FILTER) {
        fprintf(stderr,
                "sort_key_filter_pattern: \"%s\"\n",
                pegasus::utils::c_escape_string(options.sort_key_filter_pattern).c_str());
    }
    fprintf(stderr, "value_filter_type: %s\n", value_filter_type_name.c_str());
    if (value_filter_type != pegasus::pegasus_client::FT_NO_FILTER) {
        fprintf(stderr,
                "value_filter_pattern: \"%s\"\n",
                pegasus::utils::c_escape_string(value_filter_pattern).c_str());
    }
    fprintf(stderr, "max_count: %d\n", max_count);
    fprintf(stderr, "timout_ms: %d\n", timeout_ms);
    fprintf(stderr, "detailed: %s\n", detailed ? "true" : "false");
    fprintf(stderr, "no_value: %s\n", options.no_value ? "true" : "false");
    fprintf(stderr, "\n");

    int count = 0;
    pegasus::pegasus_client::pegasus_scanner *scanner = nullptr;
    options.timeout_ms = timeout_ms;
    int ret = sc->pg_client->get_scanner(hash_key, start_sort_key, stop_sort_key, options, scanner);
    if (ret != pegasus::PERR_OK) {
        fprintf(file, "ERROR: get scanner failed: %s\n", sc->pg_client->get_error_string(ret));
        if (file != stderr) {
            fprintf(
                stderr, "ERROR: get scanner failed: %s\n", sc->pg_client->get_error_string(ret));
        }
    } else {
        std::string hash_key;
        std::string sort_key;
        std::string value;
        pegasus::pegasus_client::internal_info info;
        while ((max_count <= 0 || count < max_count) &&
               !(ret = scanner->next(hash_key, sort_key, value, &info))) {
            if (!validate_filter(value_filter_type, value_filter_pattern, value))
                continue;
            fprintf(file,
                    "\"%s\" : \"%s\"",
                    pegasus::utils::c_escape_string(hash_key, sc->escape_all).c_str(),
                    pegasus::utils::c_escape_string(sort_key, sc->escape_all).c_str());
            if (!options.no_value) {
                fprintf(file,
                        " => \"%s\"",
                        pegasus::utils::c_escape_string(value, sc->escape_all).c_str());
            }
            if (detailed) {
                fprintf(file,
                        " {app_id=%d, partition_index=%d, server=%s}",
                        info.app_id,
                        info.partition_index,
                        info.server.c_str());
            }
            fprintf(file, "\n");
            count++;
        }
        if (ret != pegasus::PERR_SCAN_COMPLETE && ret != pegasus::PERR_OK) {
            fprintf(file,
                    "ERROR: %s {app_id=%d, partition_index=%d, server=%s}\n",
                    sc->pg_client->get_error_string(ret),
                    info.app_id,
                    info.partition_index,
                    info.server.c_str());
            if (file != stderr) {
                fprintf(stderr,
                        "ERROR: %s {app_id=%d, partition_index=%d, server=%s}\n",
                        sc->pg_client->get_error_string(ret),
                        info.app_id,
                        info.partition_index,
                        info.server.c_str());
            }
        }
    }

    if (scanner) {
        delete scanner;
    }

    if (file != stderr) {
        fclose(file);
    }

    if (file == stderr && count > 0) {
        fprintf(stderr, "\n");
    }
    fprintf(stderr, "%d key-value pairs got.\n", count);
    return true;
}

bool full_scan(command_executor *e, shell_context *sc, arguments args)
{
    static struct option long_options[] = {{"detailed", no_argument, 0, 'd'},
                                           {"max_count", required_argument, 0, 'n'},
                                           {"partition", required_argument, 0, 'p'},
                                           {"timeout_ms", required_argument, 0, 't'},
                                           {"output", required_argument, 0, 'o'},
                                           {"hash_key_filter_type", required_argument, 0, 'h'},
                                           {"hash_key_filter_pattern", required_argument, 0, 'x'},
                                           {"sort_key_filter_type", required_argument, 0, 's'},
                                           {"sort_key_filter_pattern", required_argument, 0, 'y'},
                                           {"value_filter_type", required_argument, 0, 'v'},
                                           {"value_filter_pattern", required_argument, 0, 'z'},
                                           {"no_value", no_argument, 0, 'i'},
                                           {0, 0, 0, 0}};

    int32_t max_count = -1;
    bool detailed = false;
    FILE *file = stderr;
    int32_t timeout_ms = sc->timeout_ms;
    int32_t partition = -1;
    std::string hash_key_filter_type_name("no_filter");
    std::string sort_key_filter_type_name("no_filter");
    pegasus::pegasus_client::filter_type sort_key_filter_type =
        pegasus::pegasus_client::FT_NO_FILTER;
    std::string sort_key_filter_pattern;
    std::string value_filter_type_name("no_filter");
    pegasus::pegasus_client::filter_type value_filter_type = pegasus::pegasus_client::FT_NO_FILTER;
    std::string value_filter_pattern;
    pegasus::pegasus_client::scan_options options;

    escape_sds_argv(args.argc, args.argv);
    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(
            args.argc, args.argv, "dn:p:t:o:h:x:s:y:v:z:i", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'd':
            detailed = true;
            break;
        case 'n':
            if (!dsn::buf2int32(optarg, max_count)) {
                fprintf(stderr, "ERROR: parse %s as max_count failed\n", optarg);
                return false;
            }
            break;
        case 'p':
            if (!dsn::buf2int32(optarg, partition)) {
                fprintf(stderr, "ERROR: parse %s as partition id failed\n", optarg);
                return false;
            }
            if (partition < 0) {
                fprintf(stderr, "ERROR: invalid partition param, should > 0\n");
                return false;
            }
            break;
        case 't':
            if (!dsn::buf2int32(optarg, timeout_ms)) {
                fprintf(stderr, "ERROR: parse %s as timeout_ms failed\n", optarg);
                return false;
            }
            break;
        case 'o':
            file = fopen(optarg, "w");
            if (!file) {
                fprintf(stderr, "ERROR: open filename %s failed\n", optarg);
                return false;
            }
            break;
        case 'h':
            options.hash_key_filter_type = parse_filter_type(optarg, false);
            if (options.hash_key_filter_type == pegasus::pegasus_client::FT_NO_FILTER) {
                fprintf(stderr, "ERROR: invalid hash_key_filter_type param\n");
                return false;
            }
            hash_key_filter_type_name = optarg;
            break;
        case 'x':
            options.hash_key_filter_pattern = unescape_str(optarg);
            break;
        case 's':
            sort_key_filter_type = parse_filter_type(optarg, true);
            if (sort_key_filter_type == pegasus::pegasus_client::FT_NO_FILTER) {
                fprintf(stderr, "ERROR: invalid sort_key_filter_type param\n");
                return false;
            }
            sort_key_filter_type_name = optarg;
            break;
        case 'y':
            sort_key_filter_pattern = unescape_str(optarg);
            break;
        case 'v':
            value_filter_type = parse_filter_type(optarg, true);
            if (value_filter_type == pegasus::pegasus_client::FT_NO_FILTER) {
                fprintf(stderr, "ERROR: invalid value_filter_type param\n");
                return false;
            }
            value_filter_type_name = optarg;
            break;
        case 'z':
            value_filter_pattern = unescape_str(optarg);
            break;
        case 'i':
            options.no_value = true;
            break;
        default:
            return false;
        }
    }

    if (value_filter_type != pegasus::pegasus_client::FT_NO_FILTER && options.no_value) {
        fprintf(stderr, "ERROR: no_value should not be set when value_filter_type is set\n");
        return false;
    }

    fprintf(stderr,
            "partition: %s\n",
            partition >= 0 ? boost::lexical_cast<std::string>(partition).c_str() : "all");
    fprintf(stderr, "hash_key_filter_type: %s\n", hash_key_filter_type_name.c_str());
    if (options.hash_key_filter_type != pegasus::pegasus_client::FT_NO_FILTER) {
        fprintf(stderr,
                "hash_key_filter_pattern: \"%s\"\n",
                pegasus::utils::c_escape_string(options.hash_key_filter_pattern).c_str());
    }
    fprintf(stderr, "sort_key_filter_type: %s\n", sort_key_filter_type_name.c_str());
    if (sort_key_filter_type != pegasus::pegasus_client::FT_NO_FILTER) {
        fprintf(stderr,
                "sort_key_filter_pattern: \"%s\"\n",
                pegasus::utils::c_escape_string(sort_key_filter_pattern).c_str());
    }
    fprintf(stderr, "value_filter_type: %s\n", value_filter_type_name.c_str());
    if (value_filter_type != pegasus::pegasus_client::FT_NO_FILTER) {
        fprintf(stderr,
                "value_filter_pattern: \"%s\"\n",
                pegasus::utils::c_escape_string(value_filter_pattern).c_str());
    }
    fprintf(stderr, "max_count: %d\n", max_count);
    fprintf(stderr, "timout_ms: %d\n", timeout_ms);
    fprintf(stderr, "detailed: %s\n", detailed ? "true" : "false");
    fprintf(stderr, "no_value: %s\n", options.no_value ? "true" : "false");
    fprintf(stderr, "\n");

    int count = 0;
    std::vector<pegasus::pegasus_client::pegasus_scanner *> scanners;
    options.timeout_ms = timeout_ms;
    if (sort_key_filter_type != pegasus::pegasus_client::FT_NO_FILTER) {
        if (sort_key_filter_type == pegasus::pegasus_client::FT_MATCH_EXACT)
            options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
        else
            options.sort_key_filter_type = sort_key_filter_type;
        options.sort_key_filter_pattern = sort_key_filter_pattern;
    }
    int ret = sc->pg_client->get_unordered_scanners(10000, options, scanners);
    if (ret != pegasus::PERR_OK) {
        fprintf(file, "ERROR: %s\n", sc->pg_client->get_error_string(ret));
        if (file != stderr) {
            fprintf(stderr, "ERROR: %s\n", sc->pg_client->get_error_string(ret));
        }
    } else if (partition >= 0 && partition >= (int)scanners.size()) {
        fprintf(file,
                "ERROR: partition %d out of range, should be in range of [0,%d]\n",
                partition,
                (int)scanners.size() - 1);
        if (file != stderr) {
            fprintf(stderr,
                    "ERROR: partition %d out of range, should be in range of [0,%d]\n",
                    partition,
                    (int)scanners.size() - 1);
        }
    } else {
        for (int i = 0; i < scanners.size(); i++) {
            if (partition >= 0 && partition != i)
                continue;
            std::string hash_key;
            std::string sort_key;
            std::string value;
            pegasus::pegasus_client::internal_info info;
            pegasus::pegasus_client::pegasus_scanner *scanner = scanners[i];
            while ((max_count <= 0 || count < max_count) &&
                   !(ret = scanner->next(hash_key, sort_key, value, &info))) {
                if (sort_key_filter_type == pegasus::pegasus_client::FT_MATCH_EXACT &&
                    sort_key.length() > sort_key_filter_pattern.length())
                    continue;
                if (!validate_filter(value_filter_type, value_filter_pattern, value))
                    continue;
                fprintf(file,
                        "\"%s\" : \"%s\"",
                        pegasus::utils::c_escape_string(hash_key, sc->escape_all).c_str(),
                        pegasus::utils::c_escape_string(sort_key, sc->escape_all).c_str());
                if (!options.no_value) {
                    fprintf(file,
                            " => \"%s\"",
                            pegasus::utils::c_escape_string(value, sc->escape_all).c_str());
                }
                if (detailed) {
                    fprintf(file,
                            " {app_id=%d, partition_index=%d, server=%s}",
                            info.app_id,
                            info.partition_index,
                            info.server.c_str());
                }
                fprintf(file, "\n");
                count++;
            }
            if (ret != pegasus::PERR_SCAN_COMPLETE && ret != pegasus::PERR_OK) {
                fprintf(file,
                        "ERROR: %s {app_id=%d, partition_index=%d, server=%s}\n",
                        sc->pg_client->get_error_string(ret),
                        info.app_id,
                        info.partition_index,
                        info.server.c_str());
                if (file != stderr) {
                    fprintf(stderr,
                            "ERROR: %s {app_id=%d, partition_index=%d, server=%s}\n",
                            sc->pg_client->get_error_string(ret),
                            info.app_id,
                            info.partition_index,
                            info.server.c_str());
                }
            }
        }
    }

    for (auto scanner : scanners) {
        delete scanner;
    }

    if (file != stderr) {
        fclose(file);
    }

    if (file == stderr && count > 0) {
        fprintf(stderr, "\n");
    }
    fprintf(stderr, "%d key-value pairs got.\n", count);
    return true;
}

bool copy_data(command_executor *e, shell_context *sc, arguments args)
{
    static struct option long_options[] = {{"target_cluster_name", required_argument, 0, 'c'},
                                           {"target_app_name", required_argument, 0, 'a'},
                                           {"partition", required_argument, 0, 'p'},
                                           {"max_batch_count", required_argument, 0, 'b'},
                                           {"timeout_ms", required_argument, 0, 't'},
                                           {"hash_key_filter_type", required_argument, 0, 'h'},
                                           {"hash_key_filter_pattern", required_argument, 0, 'x'},
                                           {"sort_key_filter_type", required_argument, 0, 's'},
                                           {"sort_key_filter_pattern", required_argument, 0, 'y'},
                                           {"value_filter_type", required_argument, 0, 'v'},
                                           {"value_filter_pattern", required_argument, 0, 'z'},
                                           {"no_overwrite", no_argument, 0, 'n'},
                                           {"no_value", no_argument, 0, 'i'},
                                           {"geo_data", no_argument, 0, 'g'},
                                           {0, 0, 0, 0}};

    std::string target_cluster_name;
    std::string target_app_name;
    std::string target_geo_app_name;
    int32_t partition = -1;
    int max_batch_count = 500;
    int timeout_ms = sc->timeout_ms;
    bool is_geo_data = false;
    bool no_overwrite = false;
    std::string hash_key_filter_type_name("no_filter");
    std::string sort_key_filter_type_name("no_filter");
    pegasus::pegasus_client::filter_type sort_key_filter_type =
        pegasus::pegasus_client::FT_NO_FILTER;
    std::string sort_key_filter_pattern;
    std::string value_filter_type_name("no_filter");
    pegasus::pegasus_client::filter_type value_filter_type = pegasus::pegasus_client::FT_NO_FILTER;
    std::string value_filter_pattern;
    pegasus::pegasus_client::scan_options options;

    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(
            args.argc, args.argv, "c:a:p:b:t:h:x:s:y:v:z:nig", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'c':
            target_cluster_name = optarg;
            break;
        case 'a':
            target_app_name = optarg;
            target_geo_app_name = target_app_name + "_geo";
            break;
        case 'p':
            if (!dsn::buf2int32(optarg, partition)) {
                fprintf(stderr, "ERROR: parse %s as partition failed\n", optarg);
                return false;
            }
            if (partition < 0) {
                fprintf(stderr, "ERROR: partition should be greater than 0\n");
                return false;
            }
            break;
        case 'b':
            if (!dsn::buf2int32(optarg, max_batch_count)) {
                fprintf(stderr, "ERROR: parse %s as max_batch_count failed\n", optarg);
                return false;
            }
            break;
        case 't':
            if (!dsn::buf2int32(optarg, timeout_ms)) {
                fprintf(stderr, "ERROR: parse %s as timeout_ms failed\n", optarg);
                return false;
            }
            break;
        case 'h':
            options.hash_key_filter_type = parse_filter_type(optarg, false);
            if (options.hash_key_filter_type == pegasus::pegasus_client::FT_NO_FILTER) {
                fprintf(stderr, "ERROR: invalid hash_key_filter_type param\n");
                return false;
            }
            hash_key_filter_type_name = optarg;
            break;
        case 'x':
            options.hash_key_filter_pattern = unescape_str(optarg);
            break;
        case 's':
            sort_key_filter_type = parse_filter_type(optarg, true);
            if (sort_key_filter_type == pegasus::pegasus_client::FT_NO_FILTER) {
                fprintf(stderr, "ERROR: invalid sort_key_filter_type param\n");
                return false;
            }
            sort_key_filter_type_name = optarg;
            break;
        case 'y':
            sort_key_filter_pattern = unescape_str(optarg);
            break;
        case 'v':
            value_filter_type = parse_filter_type(optarg, true);
            if (value_filter_type == pegasus::pegasus_client::FT_NO_FILTER) {
                fprintf(stderr, "ERROR: invalid value_filter_type param\n");
                return false;
            }
            value_filter_type_name = optarg;
            break;
        case 'z':
            value_filter_pattern = unescape_str(optarg);
            break;
        case 'n':
            no_overwrite = true;
            break;
        case 'i':
            options.no_value = true;
            break;
        case 'g':
            is_geo_data = true;
            break;
        default:
            return false;
        }
    }

    if (target_cluster_name.empty()) {
        fprintf(stderr, "ERROR: target_cluster_name not specified\n");
        return false;
    }

    if (target_app_name.empty()) {
        fprintf(stderr, "ERROR: target_app_name not specified\n");
        return false;
    }

    if (max_batch_count <= 1) {
        fprintf(stderr, "ERROR: max_batch_count should be greater than 1\n");
        return false;
    }

    if (timeout_ms <= 0) {
        fprintf(stderr, "ERROR: timeout_ms should be greater than 0\n");
        return false;
    }

    if (value_filter_type != pegasus::pegasus_client::FT_NO_FILTER && options.no_value) {
        fprintf(stderr, "ERROR: no_value should not be set when value_filter_type is set\n");
        return false;
    }

    fprintf(stderr, "INFO: source_cluster_name = %s\n", sc->pg_client->get_cluster_name());
    fprintf(stderr, "INFO: source_app_name = %s\n", sc->pg_client->get_app_name());
    fprintf(stderr, "INFO: target_cluster_name = %s\n", target_cluster_name.c_str());
    fprintf(stderr, "INFO: target_app_name = %s\n", target_app_name.c_str());
    if (is_geo_data) {
        fprintf(stderr, "INFO: target_geo_app_name = %s\n", target_geo_app_name.c_str());
    }
    fprintf(stderr,
            "INFO: partition = %s\n",
            partition >= 0 ? boost::lexical_cast<std::string>(partition).c_str() : "all");
    fprintf(stderr, "INFO: max_batch_count = %d\n", max_batch_count);
    fprintf(stderr, "INFO: timeout_ms = %d\n", timeout_ms);
    fprintf(stderr, "INFO: hash_key_filter_type = %s\n", hash_key_filter_type_name.c_str());
    if (options.hash_key_filter_type != pegasus::pegasus_client::FT_NO_FILTER) {
        fprintf(stderr,
                "INFO: hash_key_filter_pattern = \"%s\"\n",
                pegasus::utils::c_escape_string(options.hash_key_filter_pattern).c_str());
    }
    fprintf(stderr, "INFO: sort_key_filter_type = %s\n", sort_key_filter_type_name.c_str());
    if (sort_key_filter_type != pegasus::pegasus_client::FT_NO_FILTER) {
        fprintf(stderr,
                "INFO: sort_key_filter_pattern = \"%s\"\n",
                pegasus::utils::c_escape_string(sort_key_filter_pattern).c_str());
    }
    fprintf(stderr, "INFO: value_filter_type = %s\n", value_filter_type_name.c_str());
    if (value_filter_type != pegasus::pegasus_client::FT_NO_FILTER) {
        fprintf(stderr,
                "INFO: value_filter_pattern = \"%s\"\n",
                pegasus::utils::c_escape_string(value_filter_pattern).c_str());
    }
    fprintf(stderr, "INFO: no_overwrite = %s\n", no_overwrite ? "true" : "false");
    fprintf(stderr, "INFO: no_value = %s\n", options.no_value ? "true" : "false");

    if (target_cluster_name == sc->pg_client->get_cluster_name() &&
        target_app_name == sc->pg_client->get_app_name()) {
        fprintf(stderr, "ERROR: source app and target app is the same\n");
        return true;
    }

    pegasus::pegasus_client *target_client = pegasus::pegasus_client_factory::get_client(
        target_cluster_name.c_str(), target_app_name.c_str());
    if (target_client == nullptr) {
        fprintf(stderr, "ERROR: get target app client failed\n");
        return true;
    }

    int ret = target_client->exist("a", "b");
    if (ret != pegasus::PERR_OK && ret != pegasus::PERR_NOT_FOUND) {
        fprintf(
            stderr, "ERROR: test target app failed: %s\n", target_client->get_error_string(ret));
        return true;
    }

    std::unique_ptr<pegasus::geo::geo_client> target_geo_client;
    if (is_geo_data) {
        target_geo_client.reset(new pegasus::geo::geo_client("config.ini",
                                                             target_cluster_name.c_str(),
                                                             target_app_name.c_str(),
                                                             target_geo_app_name.c_str()));
    }

    std::vector<pegasus::pegasus_client::pegasus_scanner *> raw_scanners;
    options.timeout_ms = timeout_ms;
    if (sort_key_filter_type != pegasus::pegasus_client::FT_NO_FILTER) {
        if (sort_key_filter_type == pegasus::pegasus_client::FT_MATCH_EXACT)
            options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
        else
            options.sort_key_filter_type = sort_key_filter_type;
        options.sort_key_filter_pattern = sort_key_filter_pattern;
    }
    ret = sc->pg_client->get_unordered_scanners(INT_MAX, options, raw_scanners);
    if (ret != pegasus::PERR_OK) {
        fprintf(stderr,
                "ERROR: open source app scanner failed: %s\n",
                sc->pg_client->get_error_string(ret));
        return true;
    }
    fprintf(stderr,
            "INFO: open source app scanner succeed, partition_count = %d\n",
            (int)raw_scanners.size());

    std::vector<pegasus::pegasus_client::pegasus_scanner_wrapper> scanners;
    for (auto p : raw_scanners)
        scanners.push_back(p->get_smart_wrapper());
    raw_scanners.clear();

    if (partition != -1) {
        if (partition >= scanners.size()) {
            fprintf(stderr, "ERROR: invalid partition param: %d\n", partition);
            return true;
        }
        pegasus::pegasus_client::pegasus_scanner_wrapper s = std::move(scanners[partition]);
        scanners.clear();
        scanners.push_back(std::move(s));
    }
    int split_count = scanners.size();
    fprintf(stderr, "INFO: prepare scanners succeed, split_count = %d\n", split_count);

    std::atomic_bool error_occurred(false);
    std::vector<std::unique_ptr<scan_data_context>> contexts;
    for (int i = 0; i < split_count; i++) {
        scan_data_context *context = new scan_data_context(is_geo_data ? SCAN_GEN_GEO : SCAN_COPY,
                                                           i,
                                                           max_batch_count,
                                                           timeout_ms,
                                                           scanners[i],
                                                           target_client,
                                                           target_geo_client.get(),
                                                           &error_occurred);
        context->set_sort_key_filter(sort_key_filter_type, sort_key_filter_pattern);
        context->set_value_filter(value_filter_type, value_filter_pattern);
        if (no_overwrite)
            context->set_no_overwrite();
        contexts.emplace_back(context);
        dsn::tasking::enqueue(LPC_SCAN_DATA, nullptr, std::bind(scan_data_next, context));
    }

    // wait thread complete
    int sleep_seconds = 0;
    long last_total_rows = 0;
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        sleep_seconds++;
        int completed_split_count = 0;
        long cur_total_rows = 0;
        for (int i = 0; i < split_count; i++) {
            cur_total_rows += contexts[i]->split_rows.load();
            if (contexts[i]->split_request_count.load() == 0)
                completed_split_count++;
        }
        if (error_occurred.load()) {
            fprintf(stderr,
                    "INFO: processed for %d seconds, (%d/%d) splits, total %ld rows, last second "
                    "%ld rows, error occurred, terminating...\n",
                    sleep_seconds,
                    completed_split_count,
                    split_count,
                    cur_total_rows,
                    cur_total_rows - last_total_rows);
        } else {
            fprintf(stderr,
                    "INFO: processed for %d seconds, (%d/%d) splits, total %ld rows, last second "
                    "%ld rows\n",
                    sleep_seconds,
                    completed_split_count,
                    split_count,
                    cur_total_rows,
                    cur_total_rows - last_total_rows);
        }
        if (completed_split_count == split_count)
            break;
        last_total_rows = cur_total_rows;
    }

    if (error_occurred.load()) {
        fprintf(stderr, "ERROR: error occurred, processing terminated\n");
    }

    long total_rows = 0;
    for (int i = 0; i < split_count; i++) {
        fprintf(stderr, "INFO: split[%d]: %ld rows\n", i, contexts[i]->split_rows.load());
        total_rows += contexts[i]->split_rows.load();
    }

    fprintf(stderr,
            "\nCopy %s, total %ld rows.\n",
            error_occurred.load() ? "terminated" : "done",
            total_rows);

    return true;
}

bool clear_data(command_executor *e, shell_context *sc, arguments args)
{
    static struct option long_options[] = {{"partition", required_argument, 0, 'p'},
                                           {"max_batch_count", required_argument, 0, 'b'},
                                           {"timeout_ms", required_argument, 0, 't'},
                                           {"hash_key_filter_type", required_argument, 0, 'h'},
                                           {"hash_key_filter_pattern", required_argument, 0, 'x'},
                                           {"sort_key_filter_type", required_argument, 0, 's'},
                                           {"sort_key_filter_pattern", required_argument, 0, 'y'},
                                           {"value_filter_type", required_argument, 0, 'v'},
                                           {"value_filter_pattern", required_argument, 0, 'z'},
                                           {"force", no_argument, 0, 'f'},
                                           {0, 0, 0, 0}};

    int32_t partition = -1;
    int max_batch_count = 500;
    int timeout_ms = sc->timeout_ms;
    std::string hash_key_filter_type_name("no_filter");
    std::string sort_key_filter_type_name("no_filter");
    pegasus::pegasus_client::filter_type sort_key_filter_type =
        pegasus::pegasus_client::FT_NO_FILTER;
    std::string sort_key_filter_pattern;
    std::string value_filter_type_name("no_filter");
    pegasus::pegasus_client::filter_type value_filter_type = pegasus::pegasus_client::FT_NO_FILTER;
    std::string value_filter_pattern;
    bool force = false;
    pegasus::pegasus_client::scan_options options;

    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "p:b:t:h:x:s:y:v:z:f", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'p':
            if (!dsn::buf2int32(optarg, partition)) {
                fprintf(stderr, "ERROR: parse %s as partition failed\n", optarg);
                return false;
            }
            if (partition < 0) {
                fprintf(stderr, "ERROR: partition should be greater than 0\n");
                return false;
            }
            break;
        case 'b':
            if (!dsn::buf2int32(optarg, max_batch_count)) {
                fprintf(stderr, "ERROR: parse %s as max_batch_count failed\n", optarg);
                return false;
            }
            break;
        case 't':
            if (!dsn::buf2int32(optarg, timeout_ms)) {
                fprintf(stderr, "ERROR: parse %s as timeout_ms failed\n", optarg);
                return false;
            }
            break;
        case 'h':
            options.hash_key_filter_type = parse_filter_type(optarg, false);
            if (options.hash_key_filter_type == pegasus::pegasus_client::FT_NO_FILTER) {
                fprintf(stderr, "ERROR: invalid hash_key_filter_type param\n");
                return false;
            }
            hash_key_filter_type_name = optarg;
            break;
        case 'x':
            options.hash_key_filter_pattern = unescape_str(optarg);
            break;
        case 's':
            sort_key_filter_type = parse_filter_type(optarg, true);
            if (sort_key_filter_type == pegasus::pegasus_client::FT_NO_FILTER) {
                fprintf(stderr, "ERROR: invalid sort_key_filter_type param\n");
                return false;
            }
            sort_key_filter_type_name = optarg;
            break;
        case 'y':
            sort_key_filter_pattern = unescape_str(optarg);
            break;
        case 'v':
            value_filter_type = parse_filter_type(optarg, true);
            if (value_filter_type == pegasus::pegasus_client::FT_NO_FILTER) {
                fprintf(stderr, "ERROR: invalid value_filter_type param\n");
                return false;
            }
            value_filter_type_name = optarg;
            break;
        case 'z':
            value_filter_pattern = unescape_str(optarg);
            break;
        case 'f':
            force = true;
            break;
        default:
            return false;
        }
    }

    if (max_batch_count <= 1) {
        fprintf(stderr, "ERROR: max_batch_count should be greater than 1\n");
        return false;
    }

    if (timeout_ms <= 0) {
        fprintf(stderr, "ERROR: timeout_ms should be greater than 0\n");
        return false;
    }

    fprintf(stderr, "INFO: cluster_name = %s\n", sc->pg_client->get_cluster_name());
    fprintf(stderr, "INFO: app_name = %s\n", sc->pg_client->get_app_name());
    fprintf(stderr,
            "INFO: partition = %s\n",
            partition >= 0 ? boost::lexical_cast<std::string>(partition).c_str() : "all");
    fprintf(stderr, "INFO: max_batch_count = %d\n", max_batch_count);
    fprintf(stderr, "INFO: timeout_ms = %d\n", timeout_ms);
    fprintf(stderr, "INFO: hash_key_filter_type = %s\n", hash_key_filter_type_name.c_str());
    if (options.hash_key_filter_type != pegasus::pegasus_client::FT_NO_FILTER) {
        fprintf(stderr,
                "INFO: hash_key_filter_pattern = \"%s\"\n",
                pegasus::utils::c_escape_string(options.hash_key_filter_pattern).c_str());
    }
    fprintf(stderr, "INFO: sort_key_filter_type = %s\n", sort_key_filter_type_name.c_str());
    if (sort_key_filter_type != pegasus::pegasus_client::FT_NO_FILTER) {
        fprintf(stderr,
                "INFO: sort_key_filter_pattern = \"%s\"\n",
                pegasus::utils::c_escape_string(sort_key_filter_pattern).c_str());
    }
    fprintf(stderr, "INFO: value_filter_type = %s\n", value_filter_type_name.c_str());
    if (value_filter_type != pegasus::pegasus_client::FT_NO_FILTER) {
        fprintf(stderr,
                "INFO: value_filter_pattern = \"%s\"\n",
                pegasus::utils::c_escape_string(value_filter_pattern).c_str());
    }
    fprintf(stderr, "INFO: force = %s\n", force ? "true" : "false");

    if (!force) {
        fprintf(stderr,
                "ERROR: be careful to clear data!!! Please specify --force if you are "
                "determined to do.\n");
        return false;
    }

    std::vector<pegasus::pegasus_client::pegasus_scanner *> raw_scanners;
    options.timeout_ms = timeout_ms;
    if (sort_key_filter_type != pegasus::pegasus_client::FT_NO_FILTER) {
        if (sort_key_filter_type == pegasus::pegasus_client::FT_MATCH_EXACT)
            options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
        else
            options.sort_key_filter_type = sort_key_filter_type;
        options.sort_key_filter_pattern = sort_key_filter_pattern;
    }
    if (value_filter_type != pegasus::pegasus_client::FT_NO_FILTER)
        options.no_value = false;
    else
        options.no_value = true;
    int ret = sc->pg_client->get_unordered_scanners(INT_MAX, options, raw_scanners);
    if (ret != pegasus::PERR_OK) {
        fprintf(
            stderr, "ERROR: open app scanner failed: %s\n", sc->pg_client->get_error_string(ret));
        return true;
    }
    fprintf(
        stderr, "INFO: open app scanner succeed, partition_count = %d\n", (int)raw_scanners.size());

    std::vector<pegasus::pegasus_client::pegasus_scanner_wrapper> scanners;
    for (auto p : raw_scanners)
        scanners.push_back(p->get_smart_wrapper());
    raw_scanners.clear();

    if (partition != -1) {
        if (partition >= scanners.size()) {
            fprintf(stderr, "ERROR: invalid partition param: %d\n", partition);
            return true;
        }
        pegasus::pegasus_client::pegasus_scanner_wrapper s = std::move(scanners[partition]);
        scanners.clear();
        scanners.push_back(std::move(s));
    }
    int split_count = scanners.size();
    fprintf(stderr, "INFO: prepare scanners succeed, split_count = %d\n", split_count);

    std::atomic_bool error_occurred(false);
    std::vector<std::unique_ptr<scan_data_context>> contexts;
    for (int i = 0; i < split_count; i++) {
        scan_data_context *context = new scan_data_context(SCAN_CLEAR,
                                                           i,
                                                           max_batch_count,
                                                           timeout_ms,
                                                           scanners[i],
                                                           sc->pg_client,
                                                           nullptr,
                                                           &error_occurred);
        context->set_sort_key_filter(sort_key_filter_type, sort_key_filter_pattern);
        context->set_value_filter(value_filter_type, value_filter_pattern);
        contexts.emplace_back(context);
        dsn::tasking::enqueue(LPC_SCAN_DATA, nullptr, std::bind(scan_data_next, context));
    }

    int sleep_seconds = 0;
    long last_total_rows = 0;
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        sleep_seconds++;
        int completed_split_count = 0;
        long cur_total_rows = 0;
        for (int i = 0; i < split_count; i++) {
            cur_total_rows += contexts[i]->split_rows.load();
            if (contexts[i]->split_request_count.load() == 0)
                completed_split_count++;
        }
        if (error_occurred.load()) {
            fprintf(stderr,
                    "INFO: processed for %d seconds, (%d/%d) splits, total %ld rows, last second "
                    "%ld rows, error occurred, terminating...\n",
                    sleep_seconds,
                    completed_split_count,
                    split_count,
                    cur_total_rows,
                    cur_total_rows - last_total_rows);
        } else {
            fprintf(stderr,
                    "INFO: processed for %d seconds, (%d/%d) splits, total %ld rows, last second "
                    "%ld rows\n",
                    sleep_seconds,
                    completed_split_count,
                    split_count,
                    cur_total_rows,
                    cur_total_rows - last_total_rows);
        }
        if (completed_split_count == split_count)
            break;
        last_total_rows = cur_total_rows;
    }

    if (error_occurred.load()) {
        fprintf(stderr, "ERROR: error occurred, terminate processing\n");
    }

    long total_rows = 0;
    for (int i = 0; i < split_count; i++) {
        fprintf(stderr, "INFO: split[%d]: %ld rows\n", i, contexts[i]->split_rows.load());
        total_rows += contexts[i]->split_rows.load();
    }

    fprintf(stderr,
            "\nClear %s, total %ld rows.\n",
            error_occurred.load() ? "terminated" : "done",
            total_rows);

    return true;
}

bool count_data(command_executor *e, shell_context *sc, arguments args)
{
    static struct option long_options[] = {{"precise", no_argument, 0, 'c'},
                                           {"partition", required_argument, 0, 'p'},
                                           {"max_batch_count", required_argument, 0, 'b'},
                                           {"timeout_ms", required_argument, 0, 't'},
                                           {"hash_key_filter_type", required_argument, 0, 'h'},
                                           {"hash_key_filter_pattern", required_argument, 0, 'x'},
                                           {"sort_key_filter_type", required_argument, 0, 's'},
                                           {"sort_key_filter_pattern", required_argument, 0, 'y'},
                                           {"value_filter_type", required_argument, 0, 'v'},
                                           {"value_filter_pattern", required_argument, 0, 'z'},
                                           {"diff_hash_key", no_argument, 0, 'd'},
                                           {"stat_size", no_argument, 0, 'a'},
                                           {"top_count", required_argument, 0, 'n'},
                                           {"run_seconds", required_argument, 0, 'r'},
                                           {"count_only", required_argument, 0, 'f'},
                                           {0, 0, 0, 0}};

    // "count_data" usually need scan all online records to get precise result, which may affect
    // cluster availability, so here define precise = false defaultly and it will return estimate
    // count immediately.
    bool precise = false;
    bool need_scan = false;
    int32_t partition = -1;
    int max_batch_count = 500;
    int timeout_ms = sc->timeout_ms;
    std::string hash_key_filter_type_name("no_filter");
    std::string sort_key_filter_type_name("no_filter");
    pegasus::pegasus_client::filter_type sort_key_filter_type =
        pegasus::pegasus_client::FT_NO_FILTER;
    std::string sort_key_filter_pattern;
    std::string value_filter_type_name("no_filter");
    pegasus::pegasus_client::filter_type value_filter_type = pegasus::pegasus_client::FT_NO_FILTER;
    std::string value_filter_pattern;
    bool diff_hash_key = false;
    bool stat_size = false;
    int top_count = 0;
    int run_seconds = 0;
    bool count_only = false;
    pegasus::pegasus_client::scan_options options;
    uint64_t req_time = dsn_now_ms();

    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(
            args.argc, args.argv, "cp:b:t:h:x:s:y:v:z:dan:r:f", long_options, &option_index);
        if (c == -1)
            break;
        // input any valid parameter means you want to get precise count by scanning.
        need_scan = true;
        switch (c) {
        case 'c':
            precise = true;
            break;
        case 'p':
            if (!dsn::buf2int32(optarg, partition)) {
                fprintf(stderr, "ERROR: parse %s as partition failed\n", optarg);
                return false;
            }
            if (partition < 0) {
                fprintf(stderr, "ERROR: partition should be greater than 0\n");
                return false;
            }
            break;
        case 'b':
            if (!dsn::buf2int32(optarg, max_batch_count)) {
                fprintf(stderr, "ERROR: parse %s as max_batch_count failed\n", optarg);
                return false;
            }
            break;
        case 't':
            if (!dsn::buf2int32(optarg, timeout_ms)) {
                fprintf(stderr, "ERROR: parse %s as timeout_ms failed\n", optarg);
                return false;
            }
            break;
        case 'h':
            options.hash_key_filter_type = parse_filter_type(optarg, false);
            if (options.hash_key_filter_type == pegasus::pegasus_client::FT_NO_FILTER) {
                fprintf(stderr, "ERROR: invalid hash_key_filter_type param\n");
                return false;
            }
            hash_key_filter_type_name = optarg;
            break;
        case 'x':
            options.hash_key_filter_pattern = unescape_str(optarg);
            break;
        case 's':
            sort_key_filter_type = parse_filter_type(optarg, true);
            if (sort_key_filter_type == pegasus::pegasus_client::FT_NO_FILTER) {
                fprintf(stderr, "ERROR: invalid sort_key_filter_type param\n");
                return false;
            }
            sort_key_filter_type_name = optarg;
            break;
        case 'y':
            sort_key_filter_pattern = unescape_str(optarg);
            break;
        case 'v':
            value_filter_type = parse_filter_type(optarg, true);
            if (value_filter_type == pegasus::pegasus_client::FT_NO_FILTER) {
                fprintf(stderr, "ERROR: invalid value_filter_type param\n");
                return false;
            }
            value_filter_type_name = optarg;
            break;
        case 'z':
            value_filter_pattern = unescape_str(optarg);
            break;
        case 'd':
            diff_hash_key = true;
            break;
        case 'a':
            stat_size = true;
            break;
        case 'n':
            if (!dsn::buf2int32(optarg, top_count)) {
                fprintf(stderr, "parse %s as top_count failed\n", optarg);
                return false;
            }
            break;
        case 'r':
            if (!dsn::buf2int32(optarg, run_seconds)) {
                fprintf(stderr, "parse %s as run_seconds failed\n", optarg);
                return false;
            }
            break;
        case 'f':
            count_only = true;
            break;
        default:
            return false;
        }
    }

    if (!precise) {
        if (need_scan) {
            fprintf(stderr,
                    "ERROR: you must input [-c|--precise] flag when you expect to get precise "
                    "result by scaning all record online\n");
            return false;
        }

        // get estimate key number
        std::vector<row_data> rows;
        std::string app_name = sc->pg_client->get_app_name();
        if (!get_app_stat(sc, app_name, rows)) {
            fprintf(stderr, "ERROR: query app stat from server failed");
            return true;
        }

        rows.resize(rows.size() + 1);
        row_data &sum = rows.back();
        sum.row_name = "(total:" + std::to_string(rows.size() - 1) + ")";
        for (int i = 0; i < rows.size() - 1; ++i) {
            const row_data &row = rows[i];
            sum.rdb_estimate_num_keys += row.rdb_estimate_num_keys;
        }

        ::dsn::utils::table_printer tp("count_data");
        tp.add_title("pidx");
        tp.add_column("estimate_count");
        for (const row_data &row : rows) {
            tp.add_row(row.row_name);
            tp.append_data(row.rdb_estimate_num_keys);
        }

        tp.output(std::cout, tp_output_format::kTabular);
        return true;
    }

    if (max_batch_count <= 1) {
        fprintf(stderr, "ERROR: max_batch_count should be greater than 1\n");
        return false;
    }

    if (timeout_ms <= 0) {
        fprintf(stderr, "ERROR: timeout_ms should be greater than 0\n");
        return false;
    }

    if (top_count < 0) {
        fprintf(stderr, "ERROR: top_count should be no less than 0\n");
        return false;
    }

    if (run_seconds < 0) {
        fprintf(stderr, "ERROR: run_seconds should be no less than 0\n");
        return false;
    }

    fprintf(stderr, "INFO: cluster_name = %s\n", sc->pg_client->get_cluster_name());
    fprintf(stderr, "INFO: app_name = %s\n", sc->pg_client->get_app_name());
    fprintf(stderr,
            "INFO: partition = %s\n",
            partition >= 0 ? boost::lexical_cast<std::string>(partition).c_str() : "all");
    fprintf(stderr, "INFO: max_batch_count = %d\n", max_batch_count);
    fprintf(stderr, "INFO: timeout_ms = %d\n", timeout_ms);
    fprintf(stderr, "INFO: hash_key_filter_type = %s\n", hash_key_filter_type_name.c_str());
    if (options.hash_key_filter_type != pegasus::pegasus_client::FT_NO_FILTER) {
        fprintf(stderr,
                "INFO: hash_key_filter_pattern = \"%s\"\n",
                pegasus::utils::c_escape_string(options.hash_key_filter_pattern).c_str());
    }
    fprintf(stderr, "INFO: sort_key_filter_type = %s\n", sort_key_filter_type_name.c_str());
    if (sort_key_filter_type != pegasus::pegasus_client::FT_NO_FILTER) {
        fprintf(stderr,
                "INFO: sort_key_filter_pattern = \"%s\"\n",
                pegasus::utils::c_escape_string(sort_key_filter_pattern).c_str());
    }
    fprintf(stderr, "INFO: value_filter_type = %s\n", value_filter_type_name.c_str());
    if (value_filter_type != pegasus::pegasus_client::FT_NO_FILTER) {
        fprintf(stderr,
                "INFO: value_filter_pattern = \"%s\"\n",
                pegasus::utils::c_escape_string(value_filter_pattern).c_str());
    }
    fprintf(stderr, "INFO: diff_hash_key = %s\n", diff_hash_key ? "true" : "false");
    fprintf(stderr, "INFO: stat_size = %s\n", stat_size ? "true" : "false");
    fprintf(stderr, "INFO: top_count = %d\n", top_count);
    fprintf(stderr, "INFO: run_seconds = %d\n", run_seconds);
    fprintf(stderr, "INFO: count_only = %d\n", count_only);

    std::vector<pegasus::pegasus_client::pegasus_scanner *> raw_scanners;
    options.timeout_ms = timeout_ms;
    if (sort_key_filter_type != pegasus::pegasus_client::FT_NO_FILTER) {
        if (sort_key_filter_type == pegasus::pegasus_client::FT_MATCH_EXACT)
            options.sort_key_filter_type = pegasus::pegasus_client::FT_MATCH_PREFIX;
        else
            options.sort_key_filter_type = sort_key_filter_type;
        options.sort_key_filter_pattern = sort_key_filter_pattern;
    }
    if (stat_size || value_filter_type != pegasus::pegasus_client::FT_NO_FILTER)
        options.no_value = false;
    else
        options.no_value = true;
    options.batch_size = max_batch_count;
    options.count_only = count_only;
    int ret = sc->pg_client->get_unordered_scanners(INT_MAX, options, raw_scanners);
    if (ret != pegasus::PERR_OK) {
        fprintf(
            stderr, "ERROR: open app scanner failed: %s\n", sc->pg_client->get_error_string(ret));
        return true;
    }
    fprintf(
        stderr, "INFO: open app scanner succeed, partition_count = %d\n", (int)raw_scanners.size());

    std::vector<pegasus::pegasus_client::pegasus_scanner_wrapper> scanners;
    for (auto p : raw_scanners)
        scanners.push_back(p->get_smart_wrapper());
    raw_scanners.clear();

    if (partition != -1) {
        if (partition >= scanners.size()) {
            fprintf(stderr, "ERROR: invalid partition param: %d\n", partition);
            return true;
        }
        pegasus::pegasus_client::pegasus_scanner_wrapper s = std::move(scanners[partition]);
        scanners.clear();
        scanners.push_back(std::move(s));
    }
    int split_count = scanners.size();
    fprintf(stderr, "INFO: prepare scanners succeed, split_count = %d\n", split_count);

    std::atomic_bool error_occurred(false);
    std::vector<std::unique_ptr<scan_data_context>> contexts;
    std::shared_ptr<rocksdb::Statistics> statistics = rocksdb::CreateDBStatistics();
    for (int i = 0; i < split_count; i++) {
        scan_data_context *context = new scan_data_context(SCAN_COUNT,
                                                           i,
                                                           max_batch_count,
                                                           timeout_ms,
                                                           scanners[i],
                                                           sc->pg_client,
                                                           nullptr,
                                                           &error_occurred,
                                                           stat_size,
                                                           statistics,
                                                           top_count,
                                                           diff_hash_key);
        context->set_sort_key_filter(sort_key_filter_type, sort_key_filter_pattern);
        context->set_value_filter(value_filter_type, value_filter_pattern);
        context->count_only = count_only;
        contexts.emplace_back(context);
        dsn::tasking::enqueue(LPC_SCAN_DATA, nullptr, std::bind(scan_data_next, context));
    }

    int sleep_seconds = 0;
    long last_total_rows = 0;
    bool stopped_by_wait_seconds = false;
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        sleep_seconds++;
        if (run_seconds > 0 && !stopped_by_wait_seconds && sleep_seconds >= run_seconds) {
            // here use compare-and-swap primitive:
            // - if error_occurred is already set true by scanners as error occured, then
            //   stopped_by_wait_seconds will be assigned as false.
            // - else, error_occurred will be set true, and stopped_by_wait_seconds will be
            //   assigned as true.
            bool expected = false;
            stopped_by_wait_seconds = error_occurred.compare_exchange_strong(expected, true);
        }
        int completed_split_count = 0;
        long cur_total_rows = 0;
        long cur_total_hash_key_count = 0;
        for (int i = 0; i < split_count; i++) {
            cur_total_rows += contexts[i]->split_rows.load();
            if (diff_hash_key)
                cur_total_hash_key_count += contexts[i]->split_hash_key_count.load();
            if (contexts[i]->split_request_count.load() == 0)
                completed_split_count++;
        }
        char hash_key_count_str[100];
        hash_key_count_str[0] = '\0';
        if (diff_hash_key) {
            sprintf(hash_key_count_str, " (%ld hash keys)", cur_total_hash_key_count);
        }
        if (!stopped_by_wait_seconds && error_occurred.load()) {
            fprintf(stderr,
                    "INFO: processed for %d seconds, (%d/%d) splits, total %ld rows%s, last second "
                    "%ld rows, error occurred, terminating...\n",
                    sleep_seconds,
                    completed_split_count,
                    split_count,
                    cur_total_rows,
                    hash_key_count_str,
                    cur_total_rows - last_total_rows);
        } else {
            fprintf(stderr,
                    "INFO: processed for %d seconds, (%d/%d) splits, total %ld rows%s, last second "
                    "%ld rows\n",
                    sleep_seconds,
                    completed_split_count,
                    split_count,
                    cur_total_rows,
                    hash_key_count_str,
                    cur_total_rows - last_total_rows);
        }
        if (completed_split_count == split_count)
            break;
        last_total_rows = cur_total_rows;
        if (stat_size && sleep_seconds % 10 == 0) {
            print_current_scan_state(contexts, "partially", stat_size, statistics, diff_hash_key);
        }
    }

    if (error_occurred.load()) {
        if (stopped_by_wait_seconds) {
            fprintf(stderr, "INFO: reached run seconds, terminate processing\n");
        } else {
            fprintf(stderr, "ERROR: error occurred, terminate processing\n");
        }
    }

    std::string stop_desc;
    if (error_occurred.load()) {
        if (stopped_by_wait_seconds) {
            stop_desc = "terminated as run time used out";
        } else {
            stop_desc = "terminated as error occurred";
        }
    } else {
        stop_desc = "done";
    }

    print_current_scan_state(contexts, stop_desc, stat_size, statistics, diff_hash_key);

    if (stat_size) {
        if (top_count > 0) {
            top_container::top_heap heap;
            for (int i = 0; i < split_count; i++) {
                top_container::top_heap &h = contexts[i]->top_rows.all();
                while (!h.empty()) {
                    heap.push(h.top());
                    h.pop();
                }
            }
            for (int i = 1; i <= top_count && !heap.empty(); i++) {
                const top_container::top_heap_item &item = heap.top();
                fprintf(stderr,
                        "[top][%d].hash_key = \"%s\"\n",
                        i,
                        pegasus::utils::c_escape_string(item.hash_key, sc->escape_all).c_str());
                fprintf(stderr,
                        "[top][%d].sort_key = \"%s\"\n",
                        i,
                        pegasus::utils::c_escape_string(item.sort_key, sc->escape_all).c_str());
                fprintf(stderr, "[top][%d].row_size = %ld\n", i, item.row_size);
                heap.pop();
            }
        }
    }

    return true;
}

std::string unescape_str(const char *escaped)
{
    std::string dst, src = escaped;
    dassert(pegasus::utils::c_unescape_string(src, dst) >= 0, "");
    return dst;
}

void escape_sds_argv(int argc, sds *argv)
{
    for (int i = 0; i < argc; i++) {
        const size_t dest_len = sdslen(argv[i]) * 4 + 1; // Maximum possible expansion
        sds new_arg = sdsnewlen(NULL, dest_len);
        pegasus::utils::c_escape_string(argv[i], sdslen(argv[i]), new_arg, dest_len);
        sdsfree(argv[i]);
        argv[i] = new_arg;
    }
}

int load_mutations(shell_context *sc, pegasus::pegasus_client::mutations &mutations)
{
    while (true) {
        int arg_count = 0;
        sds *args = scanfCommand(&arg_count);
        auto cleanup = dsn::defer([args, arg_count] { sdsfreesplitres(args, arg_count); });
        escape_sds_argv(arg_count, args);

        std::string sort_key, value;
        int ttl = 0;
        int status = mutation_check(arg_count, args);
        switch (status) {
        case -1:
            fprintf(stderr, "INFO: abort loading\n");
            return -1;
        case 0:
            fprintf(stderr, "INFO: load mutations done.\n\n");
            return 0;
        case 1: // SET
            ttl = 0;
            if (arg_count == 4) {
                if (!dsn::buf2int32(args[3], ttl)) {
                    fprintf(stderr,
                            "ERROR: parse \"%s\" as ttl failed, "
                            "print \"ok\" to finish loading, print \"abort\" to abort this "
                            "command\n",
                            args[3]);
                    break;
                }
                if (ttl <= 0) {
                    fprintf(stderr,
                            "ERROR: invalid ttl %s, "
                            "print \"ok\" to finish loading, print \"abort\" to abort this "
                            "command\n",
                            args[3]);
                    break;
                }
            }
            sort_key = unescape_str(args[1]);
            value = unescape_str(args[2]);
            fprintf(stderr,
                    "LOAD: set sortkey \"%s\", value \"%s\", ttl %d\n",
                    pegasus::utils::c_escape_string(sort_key, sc->escape_all).c_str(),
                    pegasus::utils::c_escape_string(value, sc->escape_all).c_str(),
                    ttl);
            mutations.set(std::move(sort_key), std::move(value), ttl);
            break;
        case 2: // DEL
            sort_key = unescape_str(args[1]);
            fprintf(stderr,
                    "LOAD: del sortkey \"%s\"\n",
                    pegasus::utils::c_escape_string(sort_key, sc->escape_all).c_str());
            mutations.del(std::move(sort_key));
            break;
        default:
            fprintf(stderr, "ERROR: invalid mutation, print \"ok\" to finish loading\n");
            break;
        }
    }
    return 0;
}

int mutation_check(int args_count, sds *args)
{
    int ret = -2;
    if (args_count > 0) {
        std::string op = unescape_str(args[0]);
        if (op == "abort")
            ret = -1;
        else if (op == "ok")
            ret = 0;
        else if (op == "set" && (args_count == 3 || args_count == 4))
            ret = 1;
        else if (op == "del" && args_count == 2)
            ret = 2;
    }
    return ret;
}

static void
print_current_scan_state(const std::vector<std::unique_ptr<scan_data_context>> &contexts,
                         const std::string &stop_desc,
                         bool stat_size,
                         std::shared_ptr<rocksdb::Statistics> statistics,
                         bool count_hash_key)
{
    long total_rows = 0;
    long total_hash_key_count = 0;
    for (const auto &context : contexts) {
        long rows = context->split_rows.load();
        total_rows += rows;
        fprintf(stderr, "INFO: split[%d]: %ld rows", context->split_id, rows);
        if (count_hash_key) {
            long hash_key_count = context->split_hash_key_count.load();
            total_hash_key_count += hash_key_count;
            fprintf(stderr, " (%ld hash keys)\n", hash_key_count);
        } else {
            fprintf(stderr, "\n");
        }
    }
    fprintf(stderr, "Count %s, total %ld rows.", stop_desc.c_str(), total_rows);
    if (count_hash_key) {
        fprintf(stderr, " (%ld hash keys)\n", total_hash_key_count);
    } else {
        fprintf(stderr, "\n");
    }

    if (stat_size) {
        fprintf(stderr,
                "\n============================[hash_key_size]============================\n"
                "%s=======================================================================",
                statistics->getHistogramString(static_cast<uint32_t>(histogram_type::HASH_KEY_SIZE))
                    .c_str());
        fprintf(stderr,
                "\n============================[sort_key_size]============================\n"
                "%s=======================================================================",
                statistics->getHistogramString(static_cast<uint32_t>(histogram_type::SORT_KEY_SIZE))
                    .c_str());
        fprintf(stderr,
                "\n==============================[value_size]=============================\n"
                "%s=======================================================================",
                statistics->getHistogramString(static_cast<uint32_t>(histogram_type::VALUE_SIZE))
                    .c_str());
        fprintf(stderr,
                "\n===============================[row_size]==============================\n"
                "%s=======================================================================\n\n",
                statistics->getHistogramString(static_cast<uint32_t>(histogram_type::ROW_SIZE))
                    .c_str());
    }
}

bool calculate_hash_value(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc != 3) {
        return false;
    }
    std::string hash_key = sds_to_string(args.argv[1]);
    std::string sort_key = sds_to_string(args.argv[2]);

    ::dsn::blob key;
    pegasus::pegasus_generate_key(key, hash_key, sort_key);
    uint64_t key_hash = pegasus::pegasus_key_hash(key);

    ::dsn::utils::table_printer tp;
    tp.add_row_name_and_data("key_hash", key_hash);

    if (!sc->current_app_name.empty()) {
        int32_t app_id;
        int32_t partition_count;
        std::vector<::dsn::partition_configuration> partitions;
        ::dsn::error_code err =
            sc->ddl_client->list_app(sc->current_app_name, app_id, partition_count, partitions);
        if (err != ::dsn::ERR_OK) {
            std::cout << "list app [" << sc->current_app_name
                      << "] failed, error=" << err.to_string() << std::endl;
            return true;
        }
        uint64_t partition_index = key_hash % (uint64_t)partition_count;
        tp.add_row_name_and_data("app_name", sc->current_app_name);
        tp.add_row_name_and_data("app_id", app_id);
        tp.add_row_name_and_data("partition_count", partition_count);
        tp.add_row_name_and_data("partition_index", partition_index);
        if (partitions.size() > partition_index) {
            ::dsn::partition_configuration &pc = partitions[partition_index];
            tp.add_row_name_and_data("primary", pc.primary.to_string());

            std::ostringstream oss;
            for (int i = 0; i < pc.secondaries.size(); ++i) {
                if (i != 0)
                    oss << ",";
                oss << pc.secondaries[i].to_string();
            }
            tp.add_row_name_and_data("secondaries", oss.str());
        }
    }
    tp.output(std::cout);
    return true;
}
