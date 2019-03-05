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
#include "brpc/mysql_reply.h"
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
    uint16_t capability = (_db == "" ? 0xa285 : 0xa68d);
    uint16_t extended_capability = 0x0007;
    uint32_t max_package_length = 16777216UL;
    uint8_t language = 33;
    butil::IOBuf salt;
    salt.append(_auth->salt().data(), _auth->salt().size());
    salt.append(_auth->salt2().data(), _auth->salt2().size());
    salt = mysql_build_mysql41_authentication_response(salt.to_string(), "", _passwd, "");

    butil::IOBuf payload;
    capability = butil::ByteSwapToLE16(capability);
    payload.append(&capability, 2);
    extended_capability = butil::ByteSwapToLE16(extended_capability);
    payload.append(&extended_capability, 2);
    max_package_length = butil::ByteSwapToLE32(max_package_length);
    payload.append(&max_package_length, 4);
    payload.append(&language, 1);
    for (int i = 0; i < 23; ++i)
        payload.push_back(mysql_null_terminator[0]);
    payload.append(_user);
    payload.push_back('\0');
    payload.push_back((uint8_t)(salt.size()));
    payload.append(salt);
    if (_db != "") {
        payload.append(_db);
        payload.push_back('\0');
    }
    // payload.append("mysql_native_password");
    uint32_t payload_size = butil::ByteSwapToLE32(payload.size());
    butil::IOBuf package;
    package.append(&payload_size, 3);
    package.push_back(0x01);
    package.append(payload);
    *auth_str = package.to_string();
    return 0;
}

void MysqlAuthenticator::SaveAuth(const MysqlReply::Auth* auth) {
    _auth = new MysqlReply::Auth(auth->protocol(),
                                 auth->version(),
                                 auth->thread_id(),
                                 auth->salt(),
                                 auth->capability(),
                                 auth->language(),
                                 auth->status(),
                                 auth->extended_capability(),
                                 auth->auth_plugin_length(),
                                 auth->salt2());
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
