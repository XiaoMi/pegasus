// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "pegasus_server_test_base.h"

#include <dsn/dist/replication/replica_base.h>

namespace pegasus {
namespace server {

class mock_capacity_unit_calculator : public capacity_unit_calculator
{
public:
    int64_t add_read_cu(int64_t read_data_size) override
    {
        read_cu += capacity_unit_calculator::add_read_cu(read_data_size);
        return read_cu;
    }

    int64_t add_write_cu(int64_t write_data_size) override
    {
        write_cu += capacity_unit_calculator::add_write_cu(write_data_size);
        return write_cu;
    }

    explicit mock_capacity_unit_calculator(dsn::replication::replica_base *r)
        : capacity_unit_calculator(r)
    {
    }

    void reset()
    {
        write_cu = 0;
        read_cu = 0;
    }

    int64_t write_cu{0};
    int64_t read_cu{0};
};

class capacity_unit_calculator_test : public pegasus_server_test_base
{
protected:
    std::unique_ptr<mock_capacity_unit_calculator> _cal;

public:
    capacity_unit_calculator_test() : pegasus_server_test_base()
    {
        _cal = dsn::make_unique<mock_capacity_unit_calculator>(_server.get());
    }

    void test_init()
    {
        ASSERT_EQ(_cal->read_cu, 0);
        ASSERT_EQ(_cal->write_cu, 0);
    }

    void generate_n_kvs(int n, std::vector<::dsn::apps::key_value> &kvs)
    {
        std::vector<::dsn::apps::key_value> tmp_kvs;
        for (int i = 0; i < n; i++) {
            dsn::apps::key_value kv;
            kv.key = dsn::blob::create_from_bytes("key_" + std::to_string(i));
            kv.value = dsn::blob::create_from_bytes("value_" + std::to_string(i));
            tmp_kvs.emplace_back(kv);
        }
        kvs = std::move(tmp_kvs);
    }

    void generate_n_keys(int n, std::vector<::dsn::blob> &keys)
    {
        std::vector<::dsn::blob> tmp_keys;
        for (int i = 0; i < n; i++) {
            tmp_keys.emplace_back(dsn::blob::create_from_bytes("key_" + std::to_string(i)));
        }
        keys = std::move(tmp_keys);
    }

    void generate_n_mutates(int n, std::vector<::dsn::apps::mutate> &mutates)
    {
        std::vector<::dsn::apps::mutate> tmp_mutates;
        for (int i = 0; i < n; i++) {
            dsn::apps::mutate m;
            m.sort_key = dsn::blob::create_from_bytes("key_" + std::to_string(i));
            m.value = dsn::blob::create_from_bytes("value_" + std::to_string(i));
            tmp_mutates.emplace_back(m);
        }
        mutates = std::move(tmp_mutates);
    }
};

TEST_F(capacity_unit_calculator_test, init) { test_init(); }

TEST_F(capacity_unit_calculator_test, get)
{
    _cal->add_get_cu(rocksdb::Status::kOk, dsn::blob::create_from_bytes("value"));
    ASSERT_EQ(_cal->read_cu, 1);
    _cal->reset();

    dsn::blob value;
    _cal->add_get_cu(rocksdb::Status::kNotFound, value);
    ASSERT_EQ(_cal->read_cu, 1);
    _cal->reset();

    _cal->add_get_cu(rocksdb::Status::kCorruption, value);
    ASSERT_EQ(_cal->read_cu, 0);
    _cal->reset();
}

TEST_F(capacity_unit_calculator_test, multi_get)
{
    std::vector<::dsn::apps::key_value> kvs;

    generate_n_kvs(100, kvs);
    _cal->add_multi_get_cu(rocksdb::Status::kIncomplete, kvs);
    ASSERT_EQ(_cal->read_cu, 1);
    _cal->reset();

    generate_n_kvs(500, kvs);
    _cal->add_multi_get_cu(rocksdb::Status::kOk, kvs);
    ASSERT_GT(_cal->read_cu, 1);
    _cal->reset();

    kvs.clear();
    _cal->add_multi_get_cu(rocksdb::Status::kNotFound, kvs);
    ASSERT_EQ(_cal->read_cu, 1);
    _cal->reset();

    _cal->add_multi_get_cu(rocksdb::Status::kInvalidArgument, kvs);
    ASSERT_EQ(_cal->read_cu, 1);
    _cal->reset();

    _cal->add_multi_get_cu(rocksdb::Status::kCorruption, kvs);
    ASSERT_EQ(_cal->read_cu, 0);
    _cal->reset();
}

TEST_F(capacity_unit_calculator_test, scan)
{
    std::vector<::dsn::apps::key_value> kvs;

    generate_n_kvs(100, kvs);
    _cal->add_scan_cu(rocksdb::Status::kIncomplete, kvs);
    ASSERT_EQ(_cal->read_cu, 1);
    _cal->reset();

    generate_n_kvs(500, kvs);
    _cal->add_scan_cu(rocksdb::Status::kOk, kvs);
    ASSERT_GT(_cal->read_cu, 1);
    _cal->reset();

    kvs.clear();
    _cal->add_scan_cu(rocksdb::Status::kInvalidArgument, kvs);
    ASSERT_EQ(_cal->read_cu, 1);
    _cal->reset();

    _cal->add_scan_cu(rocksdb::Status::kCorruption, kvs);
    ASSERT_EQ(_cal->read_cu, 0);
    _cal->reset();
}

TEST_F(capacity_unit_calculator_test, sortkey_count)
{
    _cal->add_sortkey_count_cu(rocksdb::Status::kOk);
    ASSERT_EQ(_cal->read_cu, 1);
    _cal->reset();

    _cal->add_sortkey_count_cu(rocksdb::Status::kCorruption);
    ASSERT_EQ(_cal->read_cu, 0);
    _cal->reset();
}

TEST_F(capacity_unit_calculator_test, ttl)
{
    _cal->add_ttl_cu(rocksdb::Status::kOk);
    ASSERT_EQ(_cal->read_cu, 1);
    _cal->reset();

    _cal->add_ttl_cu(rocksdb::Status::kNotFound);
    ASSERT_EQ(_cal->read_cu, 1);
    _cal->reset();

    _cal->add_ttl_cu(rocksdb::Status::kCorruption);
    ASSERT_EQ(_cal->read_cu, 0);
    _cal->reset();
}

TEST_F(capacity_unit_calculator_test, put)
{
    _cal->add_put_cu(rocksdb::Status::kOk,
                     dsn::blob::create_from_bytes("key"),
                     dsn::blob::create_from_bytes("value"));
    ASSERT_EQ(_cal->write_cu, 1);
    _cal->reset();

    _cal->add_put_cu(rocksdb::Status::kCorruption,
                     dsn::blob::create_from_bytes("key"),
                     dsn::blob::create_from_bytes("value"));
    ASSERT_EQ(_cal->write_cu, 0);
    _cal->reset();
}

TEST_F(capacity_unit_calculator_test, remove)
{
    _cal->add_remove_cu(rocksdb::Status::kOk, dsn::blob::create_from_bytes("key"));
    ASSERT_EQ(_cal->write_cu, 1);
    _cal->reset();

    _cal->add_remove_cu(rocksdb::Status::kCorruption, dsn::blob::create_from_bytes("key"));
    ASSERT_EQ(_cal->write_cu, 0);
    _cal->reset();
}

TEST_F(capacity_unit_calculator_test, multi_put)
{
    std::vector<::dsn::apps::key_value> kvs;

    generate_n_kvs(100, kvs);
    _cal->add_multi_put_cu(rocksdb::Status::kOk, kvs);
    ASSERT_EQ(_cal->write_cu, 1);
    _cal->reset();

    generate_n_kvs(500, kvs);
    _cal->add_multi_put_cu(rocksdb::Status::kOk, kvs);
    ASSERT_GT(_cal->write_cu, 1);
    _cal->reset();

    _cal->add_multi_put_cu(rocksdb::Status::kCorruption, kvs);
    ASSERT_EQ(_cal->write_cu, 0);
    _cal->reset();
}

TEST_F(capacity_unit_calculator_test, multi_remove)
{
    std::vector<::dsn::blob> keys;

    generate_n_keys(100, keys);
    _cal->add_multi_remove_cu(rocksdb::Status::kOk, keys);
    ASSERT_EQ(_cal->write_cu, 1);
    _cal->reset();

    generate_n_keys(1000, keys);
    _cal->add_multi_remove_cu(rocksdb::Status::kOk, keys);
    ASSERT_GT(_cal->write_cu, 1);
    _cal->reset();

    _cal->add_multi_remove_cu(rocksdb::Status::kCorruption, keys);
    ASSERT_EQ(_cal->write_cu, 0);
    _cal->reset();
}

TEST_F(capacity_unit_calculator_test, incr)
{
    _cal->add_incr_cu(rocksdb::Status::kOk);
    ASSERT_EQ(_cal->read_cu, 1);
    ASSERT_EQ(_cal->write_cu, 1);
    _cal->reset();

    _cal->add_incr_cu(rocksdb::Status::kInvalidArgument);
    ASSERT_EQ(_cal->read_cu, 1);
    ASSERT_EQ(_cal->write_cu, 0);
    _cal->reset();

    _cal->add_incr_cu(rocksdb::Status::kCorruption);
    ASSERT_EQ(_cal->read_cu, 0);
    ASSERT_EQ(_cal->write_cu, 0);
    _cal->reset();
}

TEST_F(capacity_unit_calculator_test, check_and_set)
{
    _cal->add_check_and_set_cu(rocksdb::Status::kOk,
                               dsn::blob::create_from_bytes("key"),
                               dsn::blob::create_from_bytes("value"));
    ASSERT_EQ(_cal->read_cu, 1);
    ASSERT_EQ(_cal->write_cu, 1);
    _cal->reset();

    _cal->add_check_and_set_cu(rocksdb::Status::kInvalidArgument,
                               dsn::blob::create_from_bytes("key"),
                               dsn::blob::create_from_bytes("value"));
    ASSERT_EQ(_cal->read_cu, 1);
    ASSERT_EQ(_cal->write_cu, 0);
    _cal->reset();

    _cal->add_check_and_set_cu(rocksdb::Status::kTryAgain,
                               dsn::blob::create_from_bytes("key"),
                               dsn::blob::create_from_bytes("value"));
    ASSERT_EQ(_cal->read_cu, 1);
    ASSERT_EQ(_cal->write_cu, 0);
    _cal->reset();

    _cal->add_check_and_set_cu(rocksdb::Status::kCorruption,
                               dsn::blob::create_from_bytes("key"),
                               dsn::blob::create_from_bytes("value"));
    ASSERT_EQ(_cal->read_cu, 0);
    ASSERT_EQ(_cal->write_cu, 0);
    _cal->reset();
}

TEST_F(capacity_unit_calculator_test, check_and_mutate)
{
    std::vector<::dsn::apps::mutate> mutate_list;

    generate_n_mutates(100, mutate_list);
    _cal->add_check_and_mutate_cu(rocksdb::Status::kOk, mutate_list);
    ASSERT_EQ(_cal->read_cu, 1);
    ASSERT_EQ(_cal->write_cu, 1);
    _cal->reset();

    generate_n_mutates(1000, mutate_list);
    _cal->add_check_and_mutate_cu(rocksdb::Status::kOk, mutate_list);
    ASSERT_EQ(_cal->read_cu, 1);
    ASSERT_GT(_cal->write_cu, 1);
    _cal->reset();

    _cal->add_check_and_mutate_cu(rocksdb::Status::kInvalidArgument, mutate_list);
    ASSERT_EQ(_cal->read_cu, 1);
    ASSERT_EQ(_cal->write_cu, 0);
    _cal->reset();

    _cal->add_check_and_mutate_cu(rocksdb::Status::kTryAgain, mutate_list);
    ASSERT_EQ(_cal->read_cu, 1);
    ASSERT_EQ(_cal->write_cu, 0);
    _cal->reset();

    _cal->add_check_and_mutate_cu(rocksdb::Status::kCorruption, mutate_list);
    ASSERT_EQ(_cal->read_cu, 0);
    ASSERT_EQ(_cal->write_cu, 0);
    _cal->reset();
}

} // namespace server
} // namespace pegasus
