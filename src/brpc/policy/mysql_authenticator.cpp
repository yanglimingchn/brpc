// Copyright (c) 2019 Baidu, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Author(s): Yang,Liming <yangliming01@baidu.com>

#include "brpc/policy/mysql_authenticator.h"
#include "brpc/policy/mysql_auth_hash.h"
#include "brpc/mysql_util.h"
#include "butil/base64.h"
#include "butil/iobuf.h"
#include "butil/logging.h"  // LOG()
#include "butil/sys_byteorder.h"
// #include "butil/memory/singleton_on_pthread_once.h"

namespace brpc {
namespace policy {

namespace {
MysqlAuthenticator* g_mysql_authenticator = NULL;
};

int MysqlAuthenticator::GenerateCredential(std::string* auth_str) const {
    MysqlGreeting greeting;
    auth_.cut1((char*)&greeting.protocol);
    auth_.cut_until(&greeting.version, MYSQL_STRING_NULL);
    {
        uint8_t buf[4];
        auth_.cutn(buf, 4);
        greeting.thread_id = mysql_uint4korr(buf);
    }
    auth_.cut_until(&greeting.salt, MYSQL_STRING_NULL);
    {
        uint8_t buf[2];
        auth_.cutn(&buf, 2);
        greeting.capability = mysql_uint2korr(buf);
    }
    auth_.cut1((char*)&greeting.language);
    {
        uint8_t buf[2];
        auth_.cutn(buf, 2);
        greeting.status = mysql_uint2korr(buf);
    }
    {
        uint8_t buf[2];
        auth_.cutn(buf, 2);
        greeting.extended_capability = mysql_uint2korr(buf);
    }
    auth_.cut1((char*)&greeting.auth_plugin_length);
    auth_.pop_front(10);
    auth_.cut_until(&greeting.salt2, MYSQL_STRING_NULL);

    MysqlAuthResponse response;
    response.capability = (db_ == "" ? 0xa285 : 0xa28d);
    response.extended_capability = 0x000f;
    response.max_package_length = 16777216UL;
    response.language = 33;
    response.user = user_;
    {
        butil::IOBuf salt;
        salt.append(greeting.salt);
        salt.append(greeting.salt2);
        std::string data =
            mysql_build_mysql41_authentication_response(salt.to_string(), "", passwd_, "");
        response.salt = data;
    }
    response.schema = db_;

    butil::IOBuf payload;
    uint16_t capability = butil::ByteSwapToLE16(response.capability);
    payload.append(&capability, 2);
    uint16_t extended_capability = butil::ByteSwapToLE16(response.extended_capability);
    payload.append(&response.extended_capability, 2);
    uint32_t max_package_length = butil::ByteSwapToLE32(response.max_package_length);
    payload.append(&max_package_length, 4);
    payload.append(&response.language, 1);
    for (int i = 0; i < 23; ++i)
        payload.push_back(MYSQL_STRING_NULL[0]);
    payload.append(response.user);
    payload.push_back('\0');
    payload.push_back((uint8_t)(response.salt.size()));
    payload.append(response.salt);
    if (db_ != "") {
        payload.append(db_);
        payload.push_back('\0');
    }
    uint32_t payload_size = butil::ByteSwapToLE32(payload.size());
    butil::IOBuf package;
    package.append(&payload_size, 3);
    package.push_back(0x01);
    package.append(payload);
    *auth_str = package.to_string();
    return 0;
}

MysqlAuthenticator* global_mysql_authenticator() {
    // return butil::get_leaky_singleton<MysqlAuthenticator>();
    return g_mysql_authenticator;
}

const Authenticator* global_mysql_authenticator(const std::string& user,
                                                const std::string& passwd,
                                                const std::string& db) {
    // return butil::get_leaky_singleton<MysqlAuthenticator>();
    g_mysql_authenticator = new MysqlAuthenticator(user, passwd, db);
    return g_mysql_authenticator;
}

}  // namespace policy
}  // namespace brpc
