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

#pragma once
#include <dsn/dist/replication/replication.codes.h>

namespace dsn {
namespace apps {
DEFINE_STORAGE_WRITE_RPC_CODE(RPC_RRDB_RRDB_PUT, ALLOW_BATCH, IS_IDEMPOTENT)
DEFINE_STORAGE_WRITE_RPC_CODE(RPC_RRDB_RRDB_MULTI_PUT, NOT_ALLOW_BATCH, IS_IDEMPOTENT)
DEFINE_STORAGE_WRITE_RPC_CODE(RPC_RRDB_RRDB_REMOVE, ALLOW_BATCH, IS_IDEMPOTENT)
DEFINE_STORAGE_WRITE_RPC_CODE(RPC_RRDB_RRDB_MULTI_REMOVE, NOT_ALLOW_BATCH, IS_IDEMPOTENT)
DEFINE_STORAGE_WRITE_RPC_CODE(RPC_RRDB_RRDB_INCR, NOT_ALLOW_BATCH, NOT_IDEMPOTENT)
DEFINE_STORAGE_WRITE_RPC_CODE(RPC_RRDB_RRDB_CHECK_AND_SET, NOT_ALLOW_BATCH, NOT_IDEMPOTENT)
DEFINE_STORAGE_WRITE_RPC_CODE(RPC_RRDB_RRDB_CHECK_AND_MUTATE, NOT_ALLOW_BATCH, NOT_IDEMPOTENT)
DEFINE_STORAGE_WRITE_RPC_CODE(RPC_RRDB_RRDB_DUPLICATE, NOT_ALLOW_BATCH, IS_IDEMPOTENT)
DEFINE_STORAGE_READ_RPC_CODE(RPC_RRDB_RRDB_GET)
DEFINE_STORAGE_READ_RPC_CODE(RPC_RRDB_RRDB_TTL)
DEFINE_SCAN_STORAGE_READ_RPC_CODE(RPC_RRDB_RRDB_SORTKEY_COUNT)
DEFINE_SCAN_STORAGE_READ_RPC_CODE(RPC_RRDB_RRDB_GET_SCANNER)
DEFINE_SCAN_STORAGE_READ_RPC_CODE(RPC_RRDB_RRDB_SCAN)
DEFINE_SCAN_STORAGE_READ_RPC_CODE(RPC_RRDB_RRDB_CLEAR_SCANNER)
DEFINE_SCAN_STORAGE_READ_RPC_CODE(RPC_RRDB_RRDB_MULTI_GET)
}
}
