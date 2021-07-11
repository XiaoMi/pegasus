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

#include "pegasus_client_factory_impl.h"

namespace pegasus {
namespace client {

std::unordered_map<std::string, pegasus_client_factory_impl::app_to_client_map>
    pegasus_client_factory_impl::_cluster_to_clients;
dsn::zlock *pegasus_client_factory_impl::_map_lock;

bool pegasus_client_factory_impl::initialize(const char *config_file)
{
    bool is_initialized = ::dsn::tools::is_engine_ready();
    if (config_file == nullptr) {
        dassert(is_initialized, "rdsn engine not started, please specify a valid config file");
    } else {
        if (is_initialized) {
            dwarn("rdsn engine already started, ignore the config file '%s'", config_file);
        } else {
            // use config file to run
            char exe[] = "client";
            char config[1024];
            snprintf(config, 1024, "%s", config_file);
            char *argv[] = {exe, config};
            dsn_run(2, argv, false);
        }
    }
    pegasus_client_impl::init_error();
    _map_lock = new ::dsn::zlock();
    return true;
}

pegasus_client *pegasus_client_factory_impl::get_client(const char *cluster_name,
                                                        const char *app_name)
{
    if (cluster_name == nullptr || cluster_name[0] == '\0') {
        derror("invalid parameter 'cluster_name'");
        return nullptr;
    }
    if (app_name == nullptr || app_name[0] == '\0') {
        derror("invalid parameter 'app_name'");
        return nullptr;
    }

    ::dsn::zauto_lock l(*_map_lock);
    auto it = _cluster_to_clients.find(cluster_name);
    if (it == _cluster_to_clients.end()) {
        it = _cluster_to_clients
                 .insert(cluster_to_app_map::value_type(cluster_name, app_to_client_map()))
                 .first;
    }

    app_to_client_map &app_to_clients = it->second;
    auto it2 = app_to_clients.find(app_name);
    if (it2 == app_to_clients.end()) {
        pegasus_client_impl *client = new pegasus_client_impl(cluster_name, app_name);
        it2 = app_to_clients.insert(app_to_client_map::value_type(app_name, client)).first;
    }

    return it2->second;
}
}
} // namespace
