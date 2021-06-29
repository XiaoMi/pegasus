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

#include "pegasus_client_impl.h"
#include "base/pegasus_const.h"

using namespace ::dsn;
using namespace pegasus;

namespace pegasus {
namespace client {

pegasus_client_impl::pegasus_scanner_impl::pegasus_scanner_impl(::dsn::apps::rrdb_client *client,
                                                                std::vector<uint64_t> &&hash,
                                                                const scan_options &options,
                                                                bool validate_partition_hash)
    : pegasus_scanner_impl(client, std::move(hash), options, _min, _max, validate_partition_hash)
{
    _options.start_inclusive = true;
    _options.stop_inclusive = false;
}

pegasus_client_impl::pegasus_scanner_impl::pegasus_scanner_impl(::dsn::apps::rrdb_client *client,
                                                                std::vector<uint64_t> &&hash,
                                                                const scan_options &options,
                                                                const ::dsn::blob &start_key,
                                                                const ::dsn::blob &stop_key,
                                                                bool validate_partition_hash)
    : _client(client),
      _start_key(start_key),
      _stop_key(stop_key),
      _options(options),
      _splits_hash(std::move(hash)),
      _p(-1),
      _context(SCAN_CONTEXT_ID_COMPLETED),
      _rpc_started(false),
      _validate_partition_hash(validate_partition_hash)
{
}

int pegasus_client_impl::pegasus_scanner_impl::next(std::string &hashkey,
                                                    std::string &sortkey,
                                                    std::string &value,
                                                    internal_info *info)
{
    ::dsn::utils::notify_event op_completed;
    int ret = -1;
    auto callback = [&](
        int err, std::string &&hash, std::string &&sort, std::string &&str, internal_info &&ii) {
        ret = err;
        hashkey = std::move(hash);
        sortkey = std::move(sort);
        value = std::move(str);
        if (info) {
            (*info) = std::move(ii);
        }
        op_completed.notify();
    };
    async_next(std::move(callback));
    op_completed.wait();
    return ret;
}

void pegasus_client_impl::pegasus_scanner_impl::async_next(async_scan_next_callback_t &&callback)
{
    _lock.lock();
    if (_queue.empty()) {
        _queue.emplace_back(std::move(callback));
        _async_next_internal();
        // do not unlock() to ensure that other callbacks won't be executed in the this caller's
        // thread
    } else {
        // rpc in-progress; callback will be executed when rpc finished
        _queue.emplace_back(std::move(callback));
        _lock.unlock();
    }
}

bool pegasus_client_impl::pegasus_scanner_impl::safe_destructible() const
{
    ::dsn::zauto_lock l(_lock);
    return _queue.empty();
}

pegasus_client::pegasus_scanner_wrapper
pegasus_client_impl::pegasus_scanner_impl::get_smart_wrapper()
{
    return std::make_shared<pegasus_scanner_impl_wrapper>(this);
}

// rpc won't be executed concurrently
void pegasus_client_impl::pegasus_scanner_impl::_async_next_internal()
{
    // _lock will be locked out of the while block
    dassert(!_queue.empty(), "queue should not be empty when _async_next_internal start");

    std::list<async_scan_next_callback_t> temp;
    while (true) {
        while (++_p >= _kvs.size()) {
            if (_context == SCAN_CONTEXT_ID_COMPLETED) {
                // reach the end of one partition
                if (_splits_hash.empty()) {
                    // all completed
                    swap(_queue, temp);
                    _lock.unlock();
                    // ATTENTION: after unlock, member variables can not be used anymore
                    for (auto &callback : temp) {
                        if (callback) {
                            internal_info info;
                            info.app_id = -1;
                            info.partition_index = -1;
                            info.decree = -1;
                            callback(PERR_SCAN_COMPLETE,
                                     std::string(),
                                     std::string(),
                                     std::string(),
                                     std::move(info));
                        }
                    }
                    return;
                } else {
                    _hash = _splits_hash.back();
                    _splits_hash.pop_back();
                    _split_reset();
                }
            } else if (_context == SCAN_CONTEXT_ID_NOT_EXIST) {
                // no valid context_id found
                _lock.unlock();
                _start_scan();
                return;
            } else {
                // valid context_id
                _lock.unlock();
                _next_batch();
                return;
            }
        }

        // valid data got
        std::string hash_key, sort_key;
        pegasus_restore_key(_kvs[_p].key, hash_key, sort_key);
        std::string value(_kvs[_p].value.data(), _kvs[_p].value.length());

        auto &callback = _queue.front();
        if (callback) {
            internal_info info(_info);
            _lock.unlock();
            callback(PERR_OK,
                     std::move(hash_key),
                     std::move(sort_key),
                     std::move(value),
                     std::move(info));
            _lock.lock();
            if (_queue.size() == 1) {
                // keep the last callback until exit this function
                std::swap(temp, _queue);
                _lock.unlock();
                return;
            } else {
                _queue.pop_front();
            }
        }
    }
}

void pegasus_client_impl::pegasus_scanner_impl::_next_batch()
{
    ::dsn::apps::scan_request req;
    req.context_id = _context;

    dassert(!_rpc_started, "");
    _rpc_started = true;
    _client->scan(req,
                  [this](::dsn::error_code err,
                         dsn::message_ex *req,
                         dsn::message_ex *resp) mutable { _on_scan_response(err, req, resp); },
                  std::chrono::milliseconds(_options.timeout_ms),
                  _hash);
}

void pegasus_client_impl::pegasus_scanner_impl::_start_scan()
{
    ::dsn::apps::get_scanner_request req;
    if (_kvs.empty()) {
        req.start_key = _start_key;
        req.start_inclusive = _options.start_inclusive;
    } else {
        req.start_key = _kvs.back().key;
        req.start_inclusive = false;
    }
    req.stop_key = _stop_key;
    req.stop_inclusive = _options.stop_inclusive;
    req.batch_size = _options.batch_size;
    req.hash_key_filter_type = (dsn::apps::filter_type::type)_options.hash_key_filter_type;
    req.hash_key_filter_pattern = ::dsn::blob(
        _options.hash_key_filter_pattern.data(), 0, _options.hash_key_filter_pattern.size());
    req.sort_key_filter_type = (dsn::apps::filter_type::type)_options.sort_key_filter_type;
    req.sort_key_filter_pattern = ::dsn::blob(
        _options.sort_key_filter_pattern.data(), 0, _options.sort_key_filter_pattern.size());
    req.no_value = _options.no_value;
    req.__set_validate_partition_hash(_validate_partition_hash);
    req.__set_count_only(_options.count_only);

    dassert(!_rpc_started, "");
    _rpc_started = true;
    _client->get_scanner(
        req,
        [this](::dsn::error_code err, dsn::message_ex *req, dsn::message_ex *resp) mutable {
            _on_scan_response(err, req, resp);
        },
        std::chrono::milliseconds(_options.timeout_ms),
        _hash);
}

void pegasus_client_impl::pegasus_scanner_impl::_on_scan_response(::dsn::error_code err,
                                                                  dsn::message_ex *req,
                                                                  dsn::message_ex *resp)
{
    dassert(_rpc_started, "");
    _rpc_started = false;
    ::dsn::apps::scan_response response;
    if (err == ERR_OK) {
        ::dsn::unmarshall(resp, response);
        _info.app_id = response.app_id;
        _info.partition_index = response.partition_index;
        _info.decree = -1;
        _info.server = response.server;

        if (response.error == 0) {
            _lock.lock();
            _kvs = std::move(response.kvs);
            _p = -1;
            _context = response.context_id;
            _async_next_internal();
            return;
        } else if (get_rocksdb_server_error(response.error) == PERR_NOT_FOUND) {
            _lock.lock();
            _context = SCAN_CONTEXT_ID_NOT_EXIST;
            _async_next_internal();
            return;
        }
    } else {
        _info.app_id = -1;
        _info.partition_index = -1;
        _info.decree = -1;
        _info.server = "";
    }

    // error occured
    auto ret =
        get_client_error(err == ERR_OK ? get_rocksdb_server_error(response.error) : int(err));
    internal_info info = _info;
    std::list<async_scan_next_callback_t> temp;
    _lock.lock();
    std::swap(_queue, temp);
    _lock.unlock();
    // ATTENTION: after unlock with empty queue,  memebers variables can not be used anymore

    for (auto &callback : temp) {
        if (callback) {
            callback(ret, std::string(), std::string(), std::string(), internal_info(info));
        }
    }
}

void pegasus_client_impl::pegasus_scanner_impl::_split_reset()
{
    _kvs.clear();
    _p = -1;
    _context = SCAN_CONTEXT_ID_NOT_EXIST;
}

pegasus_client_impl::pegasus_scanner_impl::~pegasus_scanner_impl()
{
    dsn::zauto_lock l(_lock);

    dassert(!_rpc_started, "all scan-rpc should be completed here");
    dassert(_queue.empty(), "queue should be empty");

    if (_client) {
        if (_context >= SCAN_CONTEXT_ID_VALID_MIN)
            _client->clear_scanner(_context, _hash);
        _client = nullptr;
    }
}

void pegasus_client_impl::pegasus_scanner_impl_wrapper::async_next(
    async_scan_next_callback_t &&callback)
{
    // wrap shared_ptr _p with callback
    _p->async_next([ __p = _p, user_callback = std::move(callback) ](int error_code,
                                                                     std::string &&hash_key,
                                                                     std::string &&sort_key,
                                                                     std::string &&value,
                                                                     internal_info &&info) {
        user_callback(error_code,
                      std::move(hash_key),
                      std::move(sort_key),
                      std::move(value),
                      std::move(info));
    });
}

const char pegasus_client_impl::pegasus_scanner_impl::_holder[] = {'\x00', '\x00', '\xFF', '\xFF'};
const ::dsn::blob pegasus_client_impl::pegasus_scanner_impl::_min = ::dsn::blob(_holder, 0, 2);
const ::dsn::blob pegasus_client_impl::pegasus_scanner_impl::_max = ::dsn::blob(_holder, 2, 2);
} // namespace client
} // namespace pegasus
