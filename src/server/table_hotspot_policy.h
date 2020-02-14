// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include "hotspot_partition_data.h"

#include <algorithm>
#include <gtest/gtest_prod.h>
#include <dsn/perf_counter/perf_counter.h>

static const int MAX_STORE_SIZE = 100;

namespace pegasus {
namespace server {
class hotspot_policy
{
public:
    virtual void
    // hotspot_app_data store the historical data which related to hotspot
    // it uses rolling queue to save one app's data
    // vector is used saving the partitions' data of this app
    // hotspot_partition_data is used to save data of one partition
    analysis_hotspot_data(const std::queue<std::vector<hotspot_partition_data>> &hotspot_app_data,
                          std::vector<::dsn::perf_counter_wrapper> &hot_points) = 0;
};

class hotspot_algo_qps_skew : public hotspot_policy
{
public:
    void
    analysis_hotspot_data(const std::queue<std::vector<hotspot_partition_data>> &hotspot_app_data,
                          std::vector<::dsn::perf_counter_wrapper> &hot_points)
    {
        const auto& anly_data = hotspot_app_data.back();
        double min_total_qps = 1.0, min_total_cu = 1.0;
        for (int i = 0; i < hotspot_app_data.back().size(); i++) {
            min_total_qps = std::min(min_total_qps, std::max(anly_data[i].total_qps, 1.0));
        }
        dassert(anly_data.size() == hot_points.size(), "partittion counts error, please check");
        for (int i = 0; i < hot_points.size(); i++) {
            hot_points[i]->set(anly_data[i].total_qps / min_total_qps);
        }
    }
};

// hotspot_calculator is used to find the hotspot in Pegasus
class hotspot_calculator
{
public:
    hotspot_calculator(const std::string &app_name, const int &partition_num)
        : app_name(app_name), _hotpot_points(partition_num)
    {
        init_perf_counter(partition_num);
    }
    void aggregate(const std::vector<row_data> &partitions);
    void start_alg();
    void init_perf_counter(const int &perf_counter_count);

    const std::string app_name;

private:
    hotspot_policy *_policy;
    std::vector<::dsn::perf_counter_wrapper> _hotpot_points;
    std::queue<std::vector<hotspot_partition_data>> hotspot_app_data;
    
    FRIEND_TEST(table_hotspot_policy, hotspot_algo_qps_skew);
};
} // namespace server
} // namespace pegasus
