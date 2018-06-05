// Copyright (c) 2018-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dsn/utility/string_view.h>
#include <dsn/tool-api/task_tracker.h>
#include <s2/s2latlng.h>
#include <s2/s2latlng_rect.h>
#include <s2/util/units/length-units.h>
#include <pegasus/client.h>

namespace pegasus {

class geo
{
public:
    enum class SortType
    {
        random = 0,
        nearest = 1
    };

public:
    int init(dsn::string_view config_file,
             dsn::string_view cluster_name,
             dsn::string_view common_app_name,
             dsn::string_view geo_app_name);

    int set_with_geo(const std::string &hashkey,
                     const std::string &sortkey,
                     const std::string &value,
                     int timeout_milliseconds = 5000,
                     int ttl_seconds = 0,
                     pegasus_client::internal_info *info = nullptr);
    int search_radial(double lat_degrees,
                      double lng_degrees,
                      double radius_m,
                      int count,
                      SortType sort_type,
                      std::vector<std::pair<std::string, double>> &result);

private:
    int extract_latlng(const std::string &value, S2LatLng &latlng);
    int set_common_data(const std::string &hashkey,
                        const std::string &sortkey,
                        const std::string &value,
                        int timeout_milliseconds,
                        int ttl_seconds,
                        pegasus_client::internal_info *info);
    int set_geo_data(const S2LatLng &latlng,
                     const std::string &key,
                     const std::string &value,
                     int timeout_milliseconds,
                     int ttl_seconds);
    int scan_next(const S2LatLng &center,
                  util::units::Meters radius,
                  int count,
                  std::vector<std::pair<std::string, double>> &result,
                  const pegasus_client::pegasus_scanner_wrapper &wrap_scanner);
    int scan_data(const std::string &hash_key,
                  const std::string &start_sort_key,
                  const std::string &stop_sort_key,
                  const S2LatLng &center,
                  util::units::Meters radius,
                  int count,
                  std::vector<std::pair<std::string, double>> &result);

private:
    // edge length at about 2km, cell id at this level is hash-key in pegasus
    static const int min_level = 12;
    // edge length at about 150m, cell id at this level is prefix of sort-key in pegasus, and
    // convenient for scan operation
    static const int max_level = 16;
    static const int max_retry_times = 0;

    dsn::task_tracker _tracker;
    pegasus_client *_common_data_client = nullptr;
    pegasus_client *_geo_data_client = nullptr;
};

} // namespace pegasus
