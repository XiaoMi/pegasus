// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <vector>
#include <bitset>
#include <thread>
#include <iostream>
#include <cstdio>
#include <unistd.h>
#include <chrono>
#include <thread>
#include <atomic>
#include <memory>
#include <sys/time.h>
#include <dsn/dist/remote_command.h>

#include "partition_kill_testor.h"

namespace pegasus {
namespace test {
partition_kill_testor::partition_kill_testor(const char *config_file) : kill_testor(config_file) {}

void partition_kill_testor::Run()
{
    ddebug("begin the kill-partition");
    while (true) {
        if (!check_cluster_status()) {
            ddebug("check_cluster_status() failed");
        } else {
            run();
        }
        ddebug("sleep %d seconds before checking", kill_interval_seconds);
        sleep(kill_interval_seconds);
    }
}

void partition_kill_testor::run()
{
    if (partitions.size() == 0) {
        ddebug("partitions empty");
        return;
    }

    int random_num = generate_one_number(0, partitions.size() - 1);
    std::vector<int> random_indexs;
    generate_random(random_indexs, random_num, 0, partitions.size() - 1);

    std::vector<dsn::task_ptr> tasks(random_num);
    std::vector<std::pair<bool, std::string>> results(random_num);

    std::vector<std::string> arguments(2);
    for (int i = 0; i < random_indexs.size(); ++i) {
        int index = random_indexs[i];
        const auto &p = partitions[index];

        arguments[0] = to_string(p.pid.get_app_id());
        arguments[1] = to_string(p.pid.get_partition_index());

        auto callback = [&results, i](::dsn::error_code err, const std::string &resp) {
            if (err == ::dsn::ERR_OK) {
                results[i].first = true;
                results[i].second = resp;
            } else {
                results[i].first = false;
                results[i].second = err.to_string();
            }
        };
        tasks[i] = dsn::dist::cmd::async_call_remote(p.primary,
                                                     "replica.kill_partition",
                                                     arguments,
                                                     callback,
                                                     std::chrono::milliseconds(5000));
    }

    for (int i = 0; i < tasks.size(); ++i) {
        tasks[i]->wait();
    }

    int failed = 0;
    for (int i = 0; i < results.size(); ++i) {
        if (!results[i].first) {
            failed++;
        }
    }

    if (failed > 0) {
        derror("call replica.kill_partition failed");
    }
}
} // namespace test
} // namespace pegasus
