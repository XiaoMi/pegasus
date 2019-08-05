// Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <monitoring/histogram.h>
#include "reporter_agent.h"

namespace pegasus {
namespace test {
class combined_stats;
class stats
{
public:
    stats();
    void set_reporter_agent(reporter_agent *reporter_agent);
    void start(int id);
    void merge(const stats &other);
    void stop();
    void add_message(const std::string &msg);
    void set_id(int id);
    void set_exclude_from_merge();
    void print_thread_status();
    void reset_last_op_time();
    void finished_ops(void *db_with_cfh, void *db, int64_t num_ops, enum operation_type op_type);
    void add_bytes(int64_t n);
    void report(const std::string &name);

private:
    int id_;
    uint64_t start_;
    uint64_t finish_;
    double seconds_;
    uint64_t done_;
    uint64_t last_report_done_;
    uint64_t next_report_;
    uint64_t bytes_;
    uint64_t last_op_finish_;
    uint64_t last_report_finish_;
    std::unordered_map<operation_type, std::shared_ptr<rocksdb::HistogramImpl>, std::hash<unsigned char>>
            hist_;
    std::string message_;
    bool exclude_from_merge_;
    reporter_agent *reporter_agent_; // does not own
    friend class combined_stats;
};

class combined_stats
{
public:
    void add_stats(const stats &stat);
    void report(const std::string &bench_name);

private:
    double calc_avg(std::vector<double> data);
    double calc_median(std::vector<double> data);

    std::vector<double> throughput_ops_;
    std::vector<double> throughput_mbs_;
};
}
}


