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

#include "value_schema_v1.h"

#include <dsn/utility/endians.h>
#include <dsn/dist/fmt_logging.h>
#include <dsn/c/api_utilities.h>
#include <dsn/utility/smart_pointers.h>

namespace pegasus {
std::unique_ptr<value_field> value_schema_v1::extract_field(dsn::string_view value,
                                                            value_field_type type)
{
    std::unique_ptr<value_field> field = nullptr;
    switch (type) {
    case value_field_type::EXPIRE_TIMESTAMP:
        field = extract_timestamp(value);
        break;
    case value_field_type::TIME_TAG:
        field = extract_time_tag(value);
        break;
    default:
        dassert_f(false, "Unsupported field type: {}", type);
    }
    return field;
}

dsn::blob value_schema_v1::extract_user_data(std::string &&value)
{
    auto *s = new std::string(std::move(value));
    dsn::data_input input(*s);
    input.skip(sizeof(uint32_t));
    input.skip(sizeof(uint64_t));
    dsn::string_view view = input.read_str();

    // tricky code to avoid memory copy
    dsn::blob user_data;
    std::shared_ptr<char> buf(const_cast<char *>(view.data()), [s](char *) { delete s; });
    user_data.assign(std::move(buf), 0, static_cast<unsigned int>(view.length()));
    return user_data;
}

void value_schema_v1::update_field(std::string &value, std::unique_ptr<value_field> field)
{
    auto type = field->type();
    switch (field->type()) {
    case value_field_type::EXPIRE_TIMESTAMP:
        update_expire_ts(value, std::move(field));
        break;
    default:
        dassert_f(false, "Unsupported update field type: {}", type);
    }
}

rocksdb::SliceParts value_schema_v1::generate_value(const value_params &params)
{
    auto expire_ts_field = static_cast<expire_timestamp_field *>(
        params.fields[value_field_type::EXPIRE_TIMESTAMP].get());
    auto timetag_field =
        static_cast<time_tag_field *>(params.fields[value_field_type::TIME_TAG].get());
    auto data_field =
        static_cast<user_data_field *>(params.fields[value_field_type::USER_DATA].get());
    if (dsn_unlikely(expire_ts_field == nullptr || data_field == nullptr ||
                     timetag_field == nullptr)) {
        dassert_f(false, "USER_DATA or EXPIRE_TIMESTAMP or TIME_TAG is not provided");
        return {nullptr, 0};
    }

    params.write_buf.resize(sizeof(uint32_t) + sizeof(uint64_t));
    dsn::data_output(params.write_buf)
        .write_u32(expire_ts_field->expire_ts)
        .write_u64(timetag_field->time_tag);
    params.write_slices.clear();
    params.write_slices.emplace_back(params.write_buf.data(), params.write_buf.size());

    dsn::string_view user_data = data_field->user_data;
    if (user_data.length() > 0) {
        params.write_slices.emplace_back(user_data.data(), user_data.length());
    }
    return {&params.write_slices[0], static_cast<int>(params.write_slices.size())};
}

std::unique_ptr<value_field> value_schema_v1::extract_timestamp(dsn::string_view value)
{
    uint32_t expire_ts = dsn::data_input(value).read_u32();
    return dsn::make_unique<expire_timestamp_field>(expire_ts);
}

std::unique_ptr<value_field> value_schema_v1::extract_time_tag(dsn::string_view value)
{
    dsn::data_input input(value);
    input.skip(sizeof(uint32_t));
    uint64_t time_tag = input.read_u64();
    return dsn::make_unique<time_tag_field>(time_tag);
}

void value_schema_v1::update_expire_ts(std::string &value, std::unique_ptr<value_field> field)
{
    dassert_f(value.length() >= sizeof(uint32_t), "value must include 'expire_ts' header");
    auto expire_field = static_cast<expire_timestamp_field *>(field.get());

    auto new_expire_ts = dsn::endian::hton(expire_field->expire_ts);
    memcpy(const_cast<char *>(value.data()), &new_expire_ts, sizeof(uint32_t));
}

} // namespace pegasus
