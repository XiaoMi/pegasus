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

#include <pegasus/version.h>
#include <dsn/utility/strings.h>
#include <setjmp.h>
#include <signal.h>
#include <algorithm>
#include "args.h"
#include "command_executor.h"
#include "commands.h"
#include "base/pegasus_const.h"

std::map<std::string, command_executor *> s_commands_map;
shell_context s_global_context;
size_t s_max_name_length = 0;
size_t s_option_width = 70;

void print_help();
bool help_info(command_executor *e, shell_context *sc, arguments args)
{
    print_help();
    return true;
}

static command_executor commands[] = {
    {
        "help", "print help info", "", help_info,
    },
    {
        "version", "get the shell version", "", version,
    },
    {
        "cluster_info",
        "get the information of the cluster",
        "[-r|--resolve_ip] [-o|--output file_name] [-j|--json]",
        query_cluster_info,
    },
    {
        "app",
        "get the partition information for some specific app",
        "<app_name> [-d|--detailed] [-r|--resolve_ip] [-o|--output file_name] [-j|--json]",
        query_app,
    },
    {
        "app_disk",
        "get the disk usage information for some specific app",
        "<app_name> [-d|--detailed] [-r|--resolve_ip] [-j|--json] [-o|--output file_name]",
        app_disk,
    },
    {
        "ls",
        "list all apps",
        "[-a|-all] [-d|--detailed] [-j|--json] [-o|--output file_name]"
        "[-s|--status all|available|creating|dropping|dropped]",
        ls_apps,
    },
    {
        "nodes",
        "get the node status for this cluster",
        "[-d|--detailed] [-j|--json] [-r|--resolve_ip] [-u|--resource_usage]"
        "[-o|--output file_name] [-s|--status all|alive|unalive] [-q|--qps]",
        ls_nodes,
    },
    {
        "create",
        "create an app",
        "<app_name> [-p|--partition_count num] [-r|--replica_count num] "
        "[-e|--envs k1=v1,k2=v2...]",
        create_app,
    },
    {
        "drop", "drop an app", "<app_name> [-r|--reserve_seconds num]", drop_app,
    },
    {
        "recall", "recall an app", "<app_id> [new_app_name]", recall_app,
    },
    {
        "set_meta_level",
        "set the meta function level: stopped, blind, freezed, steady, lively",
        "<stopped|blind|freezed|steady|lively>",
        set_meta_level,
    },
    {
        "get_meta_level", "get the meta function level", "", get_meta_level,
    },
    {
        "balance",
        "send explicit balancer request for the cluster",
        "<-g|--gpid appid.pidx> <-p|--type move_pri|copy_pri|copy_sec> <-f|--from from_address> "
        "<-t|--to to_address>",
        balance,
    },
    {
        "propose",
        "send configuration proposals to cluster",
        "[-f|--force] "
        "<-g|--gpid appid.pidx> <-p|--type ASSIGN_PRIMARY|ADD_SECONDARY|DOWNGRADE_TO_INACTIVE...> "
        "<-t|--target node_to_exec_command> <-n|--node node_to_be_affected> ",
        propose,
    },
    {
        "use",
        "set the current app used for the data access commands",
        "[app_name]",
        use_app_as_current,
    },
    {
        "cc", "change to the specified cluster", "[cluster_name]", cc_command,
    },
    {
        "escape_all",
        "if escape all characters when printing key/value bytes",
        "[true|false]",
        process_escape_all,
    },
    {
        "timeout",
        "default timeout in milliseconds for read/write operations",
        "[time_in_ms]",
        process_timeout,
    },
    {
        "hash",
        "calculate the hash result for some hash key",
        "<hash_key> <sort_key>",
        calculate_hash_value,
    },
    {
        "set", "set value", "<hash_key> <sort_key> <value> [ttl_in_seconds]", data_operations,
    },
    {
        "multi_set",
        "set multiple values for a single hash key",
        "<hash_key> <sort_key> <value> [sort_key value...]",
        data_operations,
    },
    {
        "get", "get value", "<hash_key> <sort_key>", data_operations,
    },
    {
        "multi_get",
        "get multiple values for a single hash key",
        "<hash_key> [sort_key...]",
        data_operations,
    },
    {
        "multi_get_range",
        "get multiple values under sort key range for a single hash key",
        "<hash_key> <start_sort_key> <stop_sort_key> "
        "[-a|--start_inclusive true|false] [-b|--stop_inclusive true|false] "
        "[-s|--sort_key_filter_type anywhere|prefix|postfix] "
        "[-y|--sort_key_filter_pattern str] "
        "[-n|--max_count num] [-i|--no_value] [-r|--reverse]",
        data_operations,
    },
    {
        "multi_get_sortkeys",
        "get multiple sort keys for a single hash key",
        "<hash_key>",
        data_operations,
    },
    {
        "del", "delete a key", "<hash_key> <sort_key>", data_operations,
    },
    {
        "multi_del",
        "delete multiple values for a single hash key",
        "<hash_key> <sort_key> [sort_key...]",
        data_operations,
    },
    {
        "multi_del_range",
        "delete multiple values under sort key range for a single hash key",
        "<hash_key> <start_sort_key> <stop_sort_key> "
        "[-a|--start_inclusive true|false] [-b|--stop_inclusive true|false] "
        "[-s|--sort_key_filter_type anywhere|prefix|postfix] "
        "[-y|--sort_key_filter_pattern str] "
        "[-o|--output file_name] [-i|--silent]",
        data_operations,
    },
    {
        "incr",
        "atomically increment value of a key",
        "<hash_key> <sort_key> [increment]",
        data_operations,
    },
    {
        "check_and_set",
        "atomically check and set value",
        "<hash_key> "
        "[-c|--check_sort_key str] "
        "[-t|--check_type not_exist|not_exist_or_empty|exist|not_empty] "
        "[match_anywhere|match_prefix|match_postfix] "
        "[bytes_less|bytes_less_or_equal|bytes_equal|bytes_greater_or_equal|bytes_greater] "
        "[int_less|int_less_or_equal|int_equal|int_greater_or_equal|int_greater] "
        "[-o|--check_operand str] "
        "[-s|--set_sort_key str] "
        "[-v|--set_value str] "
        "[-l|--set_value_ttl_seconds num] "
        "[-r|--return_check_value]",
        data_operations,
    },
    {
        "check_and_mutate",
        "atomically check and mutate",
        "<hash_key> "
        "[-c|--check_sort_key str] "
        "[-t|--check_type not_exist|not_exist_or_empty|exist|not_empty] "
        "[match_anywhere|match_prefix|match_postfix] "
        "[bytes_less|bytes_less_or_equal|bytes_equal|bytes_greater_or_equal|bytes_greater] "
        "[int_less|int_less_or_equal|int_equal|int_greater_or_equal|int_greater] "
        "[-o|--check_operand str] "
        "[-r|--return_check_value]",
        data_operations,
    },
    {
        "exist", "check value exist", "<hash_key> <sort_key>", data_operations,
    },
    {
        "count", "get sort key count for a single hash key", "<hash_key>", data_operations,
    },
    {
        "ttl", "query ttl for a specific key", "<hash_key> <sort_key>", data_operations,
    },
    {
        "hash_scan",
        "scan all sorted keys for a single hash key",
        "<hash_key> <start_sort_key> <stop_sort_key> "
        "[-a|--start_inclusive true|false] [-b|--stop_inclusive true|false] "
        "[-s|--sort_key_filter_type anywhere|prefix|postfix] "
        "[-y|--sort_key_filter_pattern str] "
        "[-v|--value_filter_type anywhere|prefix|postfix|exact] "
        "[-z|--value_filter_pattern str] "
        "[-o|--output file_name] [-n|--max_count num] [-t|--timeout_ms num] "
        "[-d|--detailed] [-i|--no_value]",
        data_operations,
    },
    {
        "full_scan",
        "scan all hash keys",
        "[-h|--hash_key_filter_type anywhere|prefix|postfix] "
        "[-x|--hash_key_filter_pattern str] "
        "[-s|--sort_key_filter_type anywhere|prefix|postfix|exact] "
        "[-y|--sort_key_filter_pattern str] "
        "[-v|--value_filter_type anywhere|prefix|postfix|exact] "
        "[-z|--value_filter_pattern str] "
        "[-o|--output file_name] [-n|--max_count num] [-t|--timeout_ms num] "
        "[-d|--detailed] [-i|--no_value] [-p|--partition num]",
        data_operations,
    },
    {
        "copy_data",
        "copy app data",
        "<-c|--target_cluster_name str> <-a|--target_app_name str> "
        "[-p|--partition num] [-b|--max_batch_count num] [-t|--timeout_ms num] "
        "[-h|--hash_key_filter_type anywhere|prefix|postfix] "
        "[-x|--hash_key_filter_pattern str] "
        "[-s|--sort_key_filter_type anywhere|prefix|postfix|exact] "
        "[-y|--sort_key_filter_pattern str] "
        "[-v|--value_filter_type anywhere|prefix|postfix|exact] "
        "[-z|--value_filter_pattern str] "
        "[-n|--no_overwrite] [-i|--no_value] [-g|--geo_data]",
        data_operations,
    },
    {
        "clear_data",
        "clear app data",
        "[-p|--partition num] [-b|--max_batch_count num] [-t|--timeout_ms num] "
        "[-h|--hash_key_filter_type anywhere|prefix|postfix] "
        "[-x|--hash_key_filter_pattern str] "
        "[-s|--sort_key_filter_type anywhere|prefix|postfix|exact] "
        "[-y|--sort_key_filter_pattern str] "
        "[-v|--value_filter_type anywhere|prefix|postfix|exact] "
        "[-z|--value_filter_pattern str] "
        "[-f|--force]",
        data_operations,
    },
    {
        "count_data",
        "get app row count",
        "[-c|--precise][-p|--partition num] "
        "[-b|--max_batch_count num][-t|--timeout_ms num] "
        "[-h|--hash_key_filter_type anywhere|prefix|postfix] "
        "[-x|--hash_key_filter_pattern str] "
        "[-s|--sort_key_filter_type anywhere|prefix|postfix|exact] "
        "[-y|--sort_key_filter_pattern str] "
        "[-v|--value_filter_type anywhere|prefix|postfix|exact] "
        "[-z|--value_filter_pattern str][-d|--diff_hash_key] "
        "[-a|--stat_size] [-n|--top_count num] [-r|--run_seconds num] "
        "[-f|--count_only]",
        data_operations,
    },
    {
        "remote_command",
        "send remote command to servers",
        "[-t all|meta-server|replica-server] [-r|--resolve_ip] [-l ip:port,ip:port...]"
        "<command> [arguments...]",
        remote_command,
    },
    {
        "server_info",
        "get info of servers",
        "[-t all|meta-server|replica-server] [-l ip:port,ip:port...] [-r|--resolve_ip]",
        server_info,
    },
    {
        "server_stat",
        "get stat of servers",
        "[-t all|meta-server|replica-server] [-l ip:port,ip:port...] [-r|--resolve_ip]",
        server_stat,
    },
    {
        "app_stat",
        "get stat of apps",
        "[-a|--app_name str] [-q|--only_qps] [-u|--only_usage] [-j|--json] "
        "[-o|--output file_name]",
        app_stat,
    },
    {
        "flush_log",
        "flush log of servers",
        "[-t all|meta-server|replica-server] [-l ip:port,ip:port...][-r|--resolve_ip]",
        flush_log,
    },
    {
        "local_get", "get value from local db", "<db_path> <hash_key> <sort_key>", local_get,
    },
    {
        "rdb_key_str2hex",
        "transform the given hashkey and sortkey to rocksdb raw key in hex representation",
        "<hash_key> <sort_key>",
        rdb_key_str2hex,
    },
    {
        "rdb_key_hex2str",
        "transform the given rocksdb raw key in hex representation to hash key and sort key",
        "<rdb_key_in_hex>",
        rdb_key_hex2str,
    },
    {
        "rdb_value_hex2str",
        "parse the given rocksdb raw value in hex representation",
        "<value_in_hex>",
        rdb_value_hex2str,
    },
    {
        "sst_dump",
        "dump sstable dir or files",
        "[--command=check|scan|none|raw] <--file=data_dir_OR_sst_file> "
        "[--from=user_key] [--to=user_key] [--read_num=num] [--show_properties] [--pegasus_data]",
        sst_dump,
    },
    {
        "mlog_dump",
        "dump mutation log dir",
        "<-i|--input log_dir> [-o|--output file_name] [-d|--detailed]",
        mlog_dump,
    },
    {
        "recover",
        "control the meta to recover the system from given nodes",
        "[-f|--node_list_file file_name] [-s|--node_list_str str] "
        "[-w|--wait_seconds num] "
        "[-b|--skip_bad_nodes] [-l|--skip_lost_partitions] [-o|--output file_name]",
        recover,
    },
    {
        "add_backup_policy",
        "add new cold backup policy",
        "<-p|--policy_name str> <-b|--backup_provider_type str> <-a|--app_ids 1,2...> "
        "<-i|--backup_interval_seconds num> <-s|--start_time hour:minute> "
        "<-c|--backup_history_cnt num>",
        add_backup_policy,
    },
    {"ls_backup_policy", "list the names of the subsistent backup policies", "", ls_backup_policy},
    {
        "query_backup_policy",
        "query subsistent backup policy and last backup infos",
        "<-p|--policy_name p1,p2...> [-b|--backup_info_cnt num]",
        query_backup_policy,
    },
    {
        "modify_backup_policy",
        "modify the backup policy",
        "<-p|--policy_name str> [-a|--add_app 1,2...] [-r|--remove_app 1,2...] "
        "[-i|--backup_interval_seconds num] [-c|--backup_history_count num] "
        "[-s|--start_time hour:minute]",
        modify_backup_policy,
    },
    {
        "disable_backup_policy",
        "stop policy continue backup",
        "<-p|--policy_name str>",
        disable_backup_policy,
    },
    {
        "enable_backup_policy",
        "start backup policy to backup again",
        "<-p|--policy_name str>",
        enable_backup_policy,
    },
    {
        "restore_app",
        "restore app from backup media",
        "<-c|--old_cluster_name str> <-p|--old_policy_name str> <-a|--old_app_name str> "
        "<-i|--old_app_id id> <-t|--timestamp/backup_id timestamp> "
        "<-b|--backup_provider_type str> [-n|--new_app_name str] [-s|--skip_bad_partition]",
        restore,
    },
    {
        "query_restore_status",
        "query restore status",
        "<restore_app_id> [-d|--detailed]",
        query_restore_status,
    },
    {
        "get_app_envs", "get current app envs", "[-j|--json]", get_app_envs,
    },
    {
        "set_app_envs", "set current app envs", "<key> <value> [key value...]", set_app_envs,
    },
    {
        "del_app_envs", "delete current app envs", "<key> [key...]", del_app_envs,
    },
    {
        "clear_app_envs", "clear current app envs", "[-a|--all] [-p|--prefix str]", clear_app_envs,
    },
    {
        "ddd_diagnose",
        "diagnose three-dead partitions",
        "[-g|--gpid appid|appid.pidx] [-d|--diagnose] [-a|--auto_diagnose] "
        "[-s|--skip_prompt] [-o|--output file_name]",
        ddd_diagnose,
    },
    {"add_dup", "add duplication", "<app_name> <remote_cluster_name> [-f|--freezed]", add_dup},
    {"query_dup", "query duplication info", "<app_name> [-d|--detail]", query_dup},
    {"remove_dup", "remove duplication", "<app_name> <dup_id>", remove_dup},
    {"start_dup", "start duplication", "<app_name> <dup_id>", start_dup},
    {"pause_dup", "pause duplication", "<app_name> <dup_id>", pause_dup},
    {"set_dup_fail_mode",
     "set fail_mode of duplication",
     "<app_name> <dup_id> <slow|skip>",
     set_dup_fail_mode},
    {
        "start_bulk_load",
        "start app bulk load",
        "<-a --app_name str> <-c --cluster_name str> <-p --file_provider_type str> <-r "
        "--root_path>",
        start_bulk_load,
    },
    {
        "query_bulk_load_status",
        "query app bulk load status",
        "<-a --app_name str> [-i --partition_index num] [-d --detailed]",
        query_bulk_load_status,
    },
    {
        "pause_bulk_load", "pause app bulk load", "<-a --app_name str>", pause_bulk_load,
    },
    {
        "restart_bulk_load", "restart app bulk load", "<-a --app_name str>", restart_bulk_load,
    },
    {
        "cancel_bulk_load",
        "cancel app bulk load",
        "<-a --app_name str> [-f --forced]",
        cancel_bulk_load,
    },
    {
        "detect_hotkey",
        "start or stop hotkey detection on a replica of a replica server",
        "<-a|--app_id num> "
        "<-p|--partition_index num> "
        "<-t|--hotkey_type read|write> "
        "<-c|--detect_action start|stop|query> "
        "<-d|--address str>",
        detect_hotkey,
    },
    {
        "exit", "exit shell", "", exit_shell,
    },
    {
        nullptr, nullptr, nullptr, nullptr,
    }};

void print_help(command_executor *e, size_t name_width, size_t option_width)
{
    std::vector<std::string> lines;
    std::string options(e->option_usage);
    int line_start = 0;
    int line_end = -1;
    int i;
    for (i = 0; i < options.size(); i++) {
        if (i - line_start >= option_width && line_end >= line_start) {
            std::string s = options.substr(line_start, line_end - line_start + 1);
            std::string r = dsn::utils::trim_string((char *)s.c_str());
            if (!r.empty())
                lines.push_back(r);
            line_start = line_end + 2;
        }
        if ((options[i] == ']' || options[i] == '>') && i < options.size() - 1 &&
            options[i + 1] == ' ') {
            line_end = i;
        }
    }
    line_end = i - 1;
    if (line_end >= line_start) {
        std::string s = options.substr(line_start, line_end - line_start + 1);
        std::string r = dsn::utils::trim_string((char *)s.c_str());
        if (!r.empty())
            lines.push_back(r);
    }

    std::cout << "\t" << e->name << std::string(name_width + 2 - strlen(e->name), ' ');
    if (lines.empty()) {
        std::cout << std::endl;
    } else {
        for (int k = 0; k < lines.size(); k++) {
            if (k != 0)
                std::cout << "\t" << std::string(name_width + 2, ' ');
            std::cout << lines[k] << std::endl;
        }
    }
}

void print_help()
{
    std::cout << "Usage:" << std::endl;
    for (int i = 0; commands[i].name != nullptr; ++i) {
        print_help(&commands[i], s_max_name_length, s_option_width);
    }
}

void register_all_commands()
{
    for (int i = 0; commands[i].name != nullptr; ++i) {
        auto pr = s_commands_map.emplace(commands[i].name, &commands[i]);
        dassert(pr.second, "the command '%s' is already registered!!!", commands[i].name);
        s_max_name_length = std::max(s_max_name_length, strlen(commands[i].name));
    }
}

void execute_command(command_executor *e, int argc, sds *str_args)
{
    if (!e->exec(e, &s_global_context, {argc, str_args})) {
        printf("USAGE: ");
        print_help(e, s_max_name_length, s_option_width);
    }
}

/* Linenoise completion callback. */
static void completionCallback(const char *buf, linenoiseCompletions *lc)
{
    for (int i = 0; commands[i].name != nullptr; ++i) {
        const command_executor &c = commands[i];

        size_t matchlen = strlen(buf);
        if (strncasecmp(buf, c.name, matchlen) == 0) {
            linenoiseAddCompletion(lc, c.name);
        }
    }
}

/* Linenoise hints callback. */
static char *hintsCallback(const char *buf, int *color, int *bold)
{
    int argc;
    sds *argv = sdssplitargs(buf, &argc);
    auto cleanup = dsn::defer([argc, argv]() { sdsfreesplitres(argv, argc); });

    /* Check if the argument list is empty and return ASAP. */
    if (argc == 0) {
        return nullptr;
    }

    size_t buflen = strlen(buf);
    bool endWithSpace = buflen && isspace(buf[buflen - 1]);

    for (int i = 0; commands[i].name != nullptr; ++i) {
        if (strcasecmp(argv[0], commands[i].name) == 0) {
            *color = 90;
            *bold = 0;
            sds hint = sdsnew(commands[i].option_usage);

            /* Add an initial space if needed. */
            if (!endWithSpace) {
                sds newhint = sdsnewlen(" ", 1);
                newhint = sdscatsds(newhint, hint);
                sdsfree(hint);
                hint = newhint;
            }

            return hint;
        }
    }
    return nullptr;
}

/* Linenoise free hints callback. */
static void freeHintsCallback(void *ptr) { sdsfree((sds)ptr); }

/*extern*/ void check_in_cluster(std::string cluster_name)
{
    s_global_context.current_cluster_name = cluster_name;
    std::string server_list =
        dsn_config_get_value_string(pegasus::PEGASUS_CLUSTER_SECTION_NAME.c_str(),
                                    s_global_context.current_cluster_name.c_str(),
                                    "",
                                    "");

    dsn::replication::replica_helper::load_meta_servers(
        s_global_context.meta_list,
        pegasus::PEGASUS_CLUSTER_SECTION_NAME.c_str(),
        cluster_name.c_str());
    s_global_context.ddl_client =
        dsn::make_unique<dsn::replication::replication_ddl_client>(s_global_context.meta_list);

    // get real cluster name from zk
    std::string name;
    ::dsn::error_code err = s_global_context.ddl_client->cluster_name(1000, name);
    if (err == dsn::ERR_OK) {
        cluster_name = name;
    }
    std::cout << "The cluster name is: " << cluster_name << std::endl;
    std::cout << "The cluster meta list is: " << server_list << std::endl;
}

void initialize(int argc, char **argv)
{
    std::cout << "Pegasus Shell " << PEGASUS_VERSION << std::endl;
    std::cout << "Type \"help\" for more information." << std::endl;
    std::cout << "Type \"Ctrl-D\" or \"Ctrl-C\" to exit the shell." << std::endl;
    std::cout << std::endl;

    std::string config_file = argc > 1 ? argv[1] : "config.ini";
    if (!pegasus::pegasus_client_factory::initialize(config_file.c_str())) {
        std::cout << "ERROR: init pegasus failed: " << config_file << std::endl;
        dsn_exit(-1);
    } else {
        std::cout << "The config file is: " << config_file << std::endl;
    }

    std::string cluster_name = argc > 2 ? argv[2] : "mycluster";
    check_in_cluster(cluster_name);

    linenoiseSetMultiLine(1);
    linenoiseSetCompletionCallback(completionCallback);
    linenoiseSetHintsCallback(hintsCallback);
    linenoiseSetFreeHintsCallback(freeHintsCallback);
    linenoiseHistoryLoad(".shell-history");

    register_all_commands();
}

void run()
{
    while (true) {
        int arg_count = 0;
        sds *args = scanfCommand(&arg_count);
        auto cleanup = dsn::defer([args, arg_count] { sdsfreesplitres(args, arg_count); });

        if (args == nullptr) {
            printf("Invalid argument(s)\n");
            continue;
        }

        if (arg_count > 0) {
            auto iter = s_commands_map.find(args[0]);
            if (iter != s_commands_map.end()) {
                // command executions(e.g. check_and_mutate) may have the different hints, so cancel
                // the commands hints temporarily
                linenoiseSetHintsCallback(nullptr);
                execute_command(iter->second, arg_count, args);
                linenoiseSetHintsCallback(hintsCallback);
            } else {
                std::cout << "ERROR: invalid subcommand '" << args[0] << "'" << std::endl;
                print_help();
            }
        }
    }
}

int main(int argc, char **argv)
{
    initialize(argc, argv);
    run();
    return 0;
}
