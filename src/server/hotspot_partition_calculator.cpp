// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "hotspot_partition_calculator.h"

#include <algorithm>
#include <math.h>
#include <dsn/dist/fmt_logging.h>
#include <dsn/utility/flags.h>

namespace pegasus {
namespace server {

DSN_DEFINE_int64("pegasus.collector",
                 max_hotspot_store_size,
                 100,
                 "the max count of historical data "
                 "stored in calculator, The FIFO "
                 "queue design is used to "
                 "eliminate outdated historical "
                 "data");

void hotspot_partition_calculator::data_aggregate(const std::vector<row_data> &partitions)
{
    while (_historical_data.size() > FLAGS_max_hotspot_store_size - 1) {
        _historical_data.pop();
    }
    std::vector<hotspot_partition_data> temp(partitions.size());
    for (int i = 0; i < partitions.size(); i++) {
        temp[i] = std::move(hotspot_partition_data(partitions[i]));
    }
    _historical_data.emplace(temp);
}

void hotspot_partition_calculator::init_perf_counter(int partition_count)
{
    std::string counter_name;
    std::string counter_desc;
    for (int i = 0; i < partition_count; i++) {
        string partition_desc = _app_name + '.' + std::to_string(i);
        counter_name = fmt::format("app.stat.hotspots@{}", partition_desc);
        counter_desc = fmt::format("statistic the hotspots of app {}", partition_desc);
        _hot_points[i].init_app_counter(
            "app.pegasus", counter_name.c_str(), COUNTER_TYPE_NUMBER, counter_desc.c_str());
    }
}

void hotspot_partition_calculator::data_analyse()
{
    dassert(_historical_data.back().size() == _hot_points.size(),
            "partition counts error, please check");
    std::vector<double> data_samples;
    data_samples.reserve(_historical_data.size() * _hot_points.size());
    auto temp_data = _historical_data;
    double total = 0, standard_deviation = 0, average_number = 0;
    int sample_count = 0;
    while (!temp_data.empty()) {
        for (const auto &partition_data : temp_data.front()) {
            if (partition_data.total_qps - 1.00 > 0) {
                data_samples.push_back(partition_data.total_qps);
                total += partition_data.total_qps;
                sample_count++;
            }
        }
        temp_data.pop();
    }
    if (sample_count == 0) {
        ddebug("_historical_data size == 0");
        return;
    }
    average_number = total / sample_count;
    for (const auto &data_sample : data_samples) {
        standard_deviation += pow((data_sample - average_number), 2);
    }
    standard_deviation = sqrt(standard_deviation / sample_count);
    const auto &anly_data = _historical_data.back();
    for (int i = 0; i < _hot_points.size(); i++) {
        double hot_point = (anly_data[i].total_qps - average_number) / standard_deviation;
        // perf_counter->set can only be unsigned __int64
        // use ceil to guarantee conversion results
        hot_point = ceil(std::max(hot_point, double(0)));
        _hot_points[i]->set(hot_point);
    }
}

} // namespace server
} // namespace pegasus
