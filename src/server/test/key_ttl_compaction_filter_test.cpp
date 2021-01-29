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

#include <gtest/gtest.h>
#include "server/key_ttl_compaction_filter.h"

namespace pegasus {
namespace server {

TEST(key_ttl_compaction_filter_test, need_clean_key)
{
    int32_t oneday_sec = 24 * 60 * 60;

    struct
    {
        std::string hash_key;
        int32_t expire_sec_from_now;
        bool need_clean;
    } tests[] = {{"raw_tts_audio:", 100, false},
                 {"raw_tts_audio:xxx", 100, false},
                 {"raw_tts_audio:xxx", 3 * oneday_sec - 1, false},
                 {"raw_tts_audio:xxx", 3 * oneday_sec, true},
                 {"raw_tts_audio", 4 * oneday_sec, false},
                 {"stored_tts_url_info:", 100, false},
                 {"stored_tts_url_info:xxx", 100, false},
                 {"stored_tts_url_info:xxx", 3 * oneday_sec - 1, false},
                 {"stored_tts_url_info:xxx", 3 * oneday_sec, true},
                 {"stored_tts_url_info", 4 * oneday_sec, false},
                 {"donot_clean_key", 100, false},
                 {"donot_clean_key", 4 * oneday_sec, false}};

    for (auto const &test : tests) {
        uint32_t now_ts = utils::epoch_now();

        dsn::blob raw_key;
        pegasus_generate_key(raw_key, test.hash_key, std::string("sort"));
        bool need_clean = need_clean_key(
            utils::to_rocksdb_slice(raw_key), test.expire_sec_from_now + now_ts, now_ts);
        ASSERT_EQ(need_clean, test.need_clean);
    }
}

} // namespace server
} // namespace pegasus