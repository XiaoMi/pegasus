// Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "reporter_agent.h"

namespace pegasus {
namespace test {
reporter_agent::reporter_agent(rocksdb::Env *env,
                               const std::string &fname,
                               uint64_t report_interval_secs)
    : env_(env),
      total_ops_done_(0),
      last_report_(0),
      report_interval_secs_(report_interval_secs),
      stop_(false)
{
    auto s = env_->NewWritableFile(fname, &report_file_, rocksdb::EnvOptions());
    if (s.ok()) {
        s = report_file_->Append(header() + "\n");
    }
    if (s.ok()) {
        s = report_file_->Flush();
    }
    if (!s.ok()) {
        fprintf(stderr, "Can't open %s: %s\n", fname.c_str(), s.ToString().c_str());
        abort();
    }

    reporting_thread_ = std::thread([&]() { sleep_and_report(); });
}

reporter_agent::~reporter_agent()
{
    {
        // notify reporting thread
        std::unique_lock<std::mutex> lk(mutex_);
        stop_ = true;
        stop_cv_.notify_all();
    }
    reporting_thread_.join();
}

// thread safe
void reporter_agent::report_finished_ops(int64_t num_ops) { total_ops_done_.fetch_add(num_ops); }

std::string reporter_agent::header() const { return "secs_elapsed,interval_qps"; }

void reporter_agent::sleep_and_report()
{
    uint64_t kmicros_in_second = 1000 * 1000;
    auto time_started = env_->NowMicros();
    while (true) {
        {
            // wait the main process to stop
            std::unique_lock<std::mutex> lk(mutex_);
            if (stop_ || stop_cv_.wait_for(lk, std::chrono::seconds(report_interval_secs_), [&]() {
                    return stop_;
                })) {
                // stopping
                break;
            }
            // else -> timeout, which means time for a report!
        }

        // round the seconds elapsed
        auto secs_elapsed =
            (env_->NowMicros() - time_started + kmicros_in_second / 2) / kmicros_in_second;
        auto total_ops_done_snapshot = total_ops_done_.load();
        std::string report = std::to_string(secs_elapsed) + "," +
                             std::to_string(total_ops_done_snapshot - last_report_) + "\n";

        // write to report file
        auto s = report_file_->Append(report);
        if (s.ok()) {
            s = report_file_->Flush();
        }
        if (!s.ok()) {
            fprintf(stderr,
                    "Can't write to report file (%s), stopping the reporting\n",
                    s.ToString().c_str());
            break;
        }
        last_report_ = total_ops_done_snapshot;
    }
}
} // namespace test
} // namespace pegasus
