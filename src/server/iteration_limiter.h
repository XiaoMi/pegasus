// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dsn/dist/replication/replication.codes.h>
#include "pegasus_server_impl.h"

namespace pegasus {
namespace server {

class iteration_limiter
{
public:
    iteration_limiter(uint32_t max_iteration_count,
                      uint64_t max_iteration_size,
                      uint64_t threshold_time_ms)
    {
        _max_count = max_iteration_count;
        _max_size = max_iteration_size;
        _module_num = _max_count <= 10 ? 1 : _max_count / 10;
        _max_duration_time = threshold_time_ms > 0 ? threshold_time_ms * 1e6 : 0;
        _iteration_start_time_ns = dsn_now_ns();
    }

    bool valid()
    {
        if (_iteration_count >= _max_count) {
            return false;
        }
        if (_max_size > 0 && _iteration_size >= _max_size) {
            return false;
        }
        return true;
    }

    // during rocksdb iteration, if iteration_count % module_num == 0, we will check if iteration
    // exceed time threshold, which means we at most check ten times during iteration
    bool time_check()
    {
        if (_max_duration_time > 0 && _iteration_count % _module_num == 0 &&
            dsn_now_ns() - _iteration_start_time_ns > _max_duration_time) {
            _exceed_limit = true;
            _iteration_duration_time_ns = dsn_now_ns() - _iteration_start_time_ns;
            return false;
        }
        return true;
    }

    void time_check_after_incomplete_scan()
    {
        if (_max_duration_time > 0 &&
            dsn_now_ns() - _iteration_start_time_ns > _max_duration_time) {
            _exceed_limit = true;
            _iteration_duration_time_ns = dsn_now_ns() - _iteration_start_time_ns;
        }
    }

    void add_count() { ++_iteration_count; }
    void add_size(uint64_t size) { _iteration_size += size; }

    bool exceed_limit() { return _exceed_limit; }
    uint32_t get_iteration_count() { return _iteration_count; }
    uint64_t duration_time() { return _iteration_duration_time_ns; }
    uint64_t max_duration_time() { return _max_duration_time; }

private:
    bool _exceed_limit{false};

    uint32_t _iteration_count{0};
    uint64_t _iteration_size{0};
    uint64_t _iteration_start_time_ns{0};
    uint64_t _iteration_duration_time_ns{0};

    uint32_t _max_count{0};
    uint64_t _max_size{0};
    uint64_t _max_duration_time{0};
    int32_t _module_num{1};
};
} // namespace server
} // namespace pegasus
