// Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <dsn/c/app_model.h>
#include "config.h"

namespace pegasus {
namespace test {
const config &config::get_instance()
{
    static config instance;
    return instance;
}

config::config()
{
    hashkey_size = (int32_t)dsn_config_get_value_uint64(
        "pegasus.benchmark", "hashkey_size", 0, "size of each hashkey");
    sortkey_size = (int32_t)dsn_config_get_value_uint64(
        "pegasus.benchmark", "sortkey_size", 0, "size of each sortkey");
    pegasus_cluster_name = dsn_config_get_value_string(
        "pegasus.benchmark", "pegasus_cluster_name", "", "pegasus cluster name");
    pegasus_app_name = dsn_config_get_value_string(
        "pegasus.benchmark", "pegasus_app_name", "", "pegasus app name");
    pegasus_timeout_ms = (int32_t)dsn_config_get_value_uint64(
        "pegasus.benchmark", "pegasus_timeout_ms", 0, "pegasus read/write timeout in milliseconds");
    benchmarks = dsn_config_get_value_string(
        "pegasus.benchmark",
        "benchmarks",
        "fillrandom_pegasus,readrandom_pegasus,deleterandom_pegasus",
        "Comma-separated list of operations to run in the specified order. Available benchmarks:\n"
        "\tfillrandom_pegasus       -- pegasus write N values in random key order\n"
        "\treadrandom_pegasus       -- pegasus read N times in random order\n"
        "\tdeleterandom_pegasus     -- pegasus delete N keys in random order\n");
    num = (int32_t)dsn_config_get_value_uint64(
        "pegasus.benchmark", "num", 0, "Number of key/values to place in database");
    threads = (int32_t)dsn_config_get_value_uint64(
        "pegasus.benchmark", "threads", 0, "Number of concurrent threads to run");
    value_size = (int32_t)dsn_config_get_value_uint64(
        "pegasus.benchmark", "value_size", 0, "Size of each value");
    thread_status_per_interval =
        (int32_t)dsn_config_get_value_uint64("pegasus.benchmark",
                                             "thread_status_per_interval",
                                             0,
                                             "Takes and report a snapshot of the "
                                             "current status of each thread when "
                                             "this is greater than 0");
    seed = (int32_t)dsn_config_get_value_uint64(
        "pegasus.benchmark", "seed", 0, "Seed base for random generators");

    env = rocksdb::Env::Default();
}
} // namespace test
} // namespace pegasus
