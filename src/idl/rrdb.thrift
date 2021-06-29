/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

include "dsn.thrift"

namespace cpp dsn.apps

enum filter_type
{
    FT_NO_FILTER,
    FT_MATCH_ANYWHERE,
    FT_MATCH_PREFIX,
    FT_MATCH_POSTFIX
}

enum cas_check_type
{
    CT_NO_CHECK,

    // (1~4) appearance
    CT_VALUE_NOT_EXIST,               // value is not exist
    CT_VALUE_NOT_EXIST_OR_EMPTY,      // value is not exist or value is empty
    CT_VALUE_EXIST,                   // value is exist
    CT_VALUE_NOT_EMPTY,               // value is exist and not empty

    // (5~7) match
    CT_VALUE_MATCH_ANYWHERE,          // operand matches anywhere in value
    CT_VALUE_MATCH_PREFIX,            // operand matches prefix in value
    CT_VALUE_MATCH_POSTFIX,           // operand matches postfix in value

    // (8~12) bytes compare
    CT_VALUE_BYTES_LESS,              // bytes compare: value < operand
    CT_VALUE_BYTES_LESS_OR_EQUAL,     // bytes compare: value <= operand
    CT_VALUE_BYTES_EQUAL,             // bytes compare: value == operand
    CT_VALUE_BYTES_GREATER_OR_EQUAL,  // bytes compare: value >= operand
    CT_VALUE_BYTES_GREATER,           // bytes compare: value > operand

    // (13~17) int compare: first transfer bytes to int64 by atoi(); then compare by int value
    CT_VALUE_INT_LESS,                // int compare: value < operand
    CT_VALUE_INT_LESS_OR_EQUAL,       // int compare: value <= operand
    CT_VALUE_INT_EQUAL,               // int compare: value == operand
    CT_VALUE_INT_GREATER_OR_EQUAL,    // int compare: value >= operand
    CT_VALUE_INT_GREATER              // int compare: value > operand
}

enum mutate_operation
{
    MO_PUT,
    MO_DELETE
}

struct update_request
{
    1:dsn.blob      key;
    2:dsn.blob      value;
    3:i32           expire_ts_seconds;
}

struct update_response
{
    1:i32           error;
    2:i32           app_id;
    3:i32           partition_index;
    4:i64           decree;
    5:string        server;
}

struct read_response
{
    1:i32           error;
    2:dsn.blob      value;
    3:i32           app_id;
    4:i32           partition_index;
    6:string        server;
}

struct ttl_response
{
    1:i32           error;
    2:i32           ttl_seconds;
    3:i32           app_id;
    4:i32           partition_index;
    6:string        server;
}

struct count_response
{
    1:i32           error;
    2:i64           count;
    3:i32           app_id;
    4:i32           partition_index;
    6:string        server;
}

struct key_value
{
    1:dsn.blob      key;
    2:dsn.blob      value;
}

struct multi_put_request
{
    1:dsn.blob      hash_key;
    2:list<key_value> kvs; // sort_key => value
    3:i32           expire_ts_seconds;
}

struct multi_remove_request
{
    1:dsn.blob      hash_key;
    2:list<dsn.blob> sort_keys; // should not be empty
    3:i64           max_count; // deprecated
}

struct multi_remove_response
{
    1:i32           error;
    2:i64           count; // deleted count
    3:i32           app_id;
    4:i32           partition_index;
    5:i64           decree;
    6:string        server;
}

struct multi_get_request
{
    1:dsn.blob      hash_key;
    2:list<dsn.blob> sort_keys; // not empty means only fetch specified sortkeys
    3:i32           max_kv_count; // <= 0 means no limit
    4:i32           max_kv_size; // <= 0 means no limit
    5:bool          no_value; // not return value, only return sortkeys
    6:dsn.blob      start_sortkey;
    7:dsn.blob      stop_sortkey; // empty means fetch to the last sort key
    8:bool          start_inclusive;
    9:bool          stop_inclusive;
    10:filter_type  sort_key_filter_type;
    11:dsn.blob     sort_key_filter_pattern;
    12:bool         reverse; // if search in reverse direction
}

struct multi_get_response
{
    1:i32           error;
    2:list<key_value> kvs; // sort_key => value; ascending ordered by sort_key
    3:i32           app_id;
    4:i32           partition_index;
    6:string        server;
}

struct incr_request
{
    1:dsn.blob      key;
    2:i64           increment;
    3:i32           expire_ts_seconds; // 0 means keep original ttl
                                       // >0 means reset to new ttl
                                       // <0 means reset to no ttl
}

struct incr_response
{
    1:i32           error;
    2:i64           new_value;
    3:i32           app_id;
    4:i32           partition_index;
    5:i64           decree;
    6:string        server;
}

struct check_and_set_request
{
    1:dsn.blob       hash_key;
    2:dsn.blob       check_sort_key;
    3:cas_check_type check_type;
    4:dsn.blob       check_operand;
    5:bool           set_diff_sort_key; // if set different sort key with check_sort_key
    6:dsn.blob       set_sort_key; // used only if set_diff_sort_key is true
    7:dsn.blob       set_value;
    8:i32            set_expire_ts_seconds;
    9:bool           return_check_value;
}

struct check_and_set_response
{
    1:i32            error; // return kTryAgain if check not passed.
                            // return kInvalidArgument if check type is int compare and
                            // check_operand/check_value is not integer or out of range.
    2:bool           check_value_returned;
    3:bool           check_value_exist; // used only if check_value_returned is true
    4:dsn.blob       check_value; // used only if check_value_returned and check_value_exist is true
    5:i32            app_id;
    6:i32            partition_index;
    7:i64            decree;
    8:string         server;
}

struct mutate
{
    1:mutate_operation operation;
    2:dsn.blob         sort_key;
    3:dsn.blob         value; // set null if operation is MO_DELETE
    4:i32              set_expire_ts_seconds; // set 0 if operation is MO_DELETE
}

struct check_and_mutate_request
{
    1:dsn.blob       hash_key;
    2:dsn.blob       check_sort_key;
    3:cas_check_type check_type;
    4:dsn.blob       check_operand;
    5:list<mutate>   mutate_list;
    6:bool           return_check_value;
}

struct check_and_mutate_response
{
    1:i32            error; // return kTryAgain if check not passed.
                            // return kInvalidArgument if check type is int compare and
                            // check_operand/check_value is not integer or out of range.
    2:bool           check_value_returned;
    3:bool           check_value_exist; // used only if check_value_returned is true
    4:dsn.blob       check_value; // used only if check_value_returned and check_value_exist is true
    5:i32            app_id;
    6:i32            partition_index;
    7:i64            decree;
    8:string         server;
}

struct get_scanner_request
{
    1:dsn.blob  start_key;
    2:dsn.blob  stop_key;
    3:bool      start_inclusive;
    4:bool      stop_inclusive;
    5:i32       batch_size;
    6:bool      no_value; // not return value, only return sortkeys
    7:filter_type  hash_key_filter_type;
    8:dsn.blob     hash_key_filter_pattern;
    9:filter_type  sort_key_filter_type;
    10:dsn.blob    sort_key_filter_pattern;
    11:optional bool    validate_partition_hash;
    12:optional bool    count_only;
}

struct scan_request
{
    1:i64           context_id;
}

struct scan_response
{
    1:i32           error;
    2:list<key_value> kvs;
    3:i64           context_id;
    4:i32           app_id;
    5:i32           partition_index;
    6:string        server;
}

struct duplicate_request
{
    // The timestamp of this write.
    1: optional i64 timestamp

    // The code to identify this write.
    2: optional dsn.task_code task_code

    // The binary form of the write.
    3: optional dsn.blob raw_message

    // ID of the cluster where this write comes from.
    4: optional byte cluster_id

    // Whether to compare the timetag of old value with the new write's.
    5: optional bool verify_timetag
}

struct duplicate_response
{
    1: optional i32 error;

    // hints on the reason why this duplicate failed.
    2: optional string error_hint;
}

service rrdb
{
    update_response put(1:update_request update);
    update_response multi_put(1:multi_put_request request);
    update_response remove(1:dsn.blob key);
    multi_remove_response multi_remove(1:multi_remove_request request);
    incr_response incr(1:incr_request request);
    check_and_set_response check_and_set(1:check_and_set_request request);
    check_and_mutate_response check_and_mutate(1:check_and_mutate_request request);
    read_response get(1:dsn.blob key);
    multi_get_response multi_get(1:multi_get_request request);
    count_response sortkey_count(1:dsn.blob hash_key);
    ttl_response ttl(1:dsn.blob key);

    scan_response get_scanner(1:get_scanner_request request);
    scan_response scan(1:scan_request request);
    oneway void clear_scanner(1:i64 context_id);
}

