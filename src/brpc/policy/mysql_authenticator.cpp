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
        uint8_t tmp[4];
        auth_.cutn(tmp, 4);
        greeting.thread_id = mysql_uint4korr(tmp);
    }
    auth_.cut_until(&greeting.salt, MYSQL_STRING_NULL);
    {
        uint8_t tmp[2];
        auth_.cutn(tmp, 2);
        greeting.capability = mysql_uint2korr(tmp);
    }
    auth_.cut1((char*)&greeting.language);
    {
        uint8_t tmp[2];
        auth_.cutn(tmp, 2);
        greeting.status = mysql_uint2korr(tmp);
    }
    {
        uint8_t tmp[2];
        auth_.cutn(tmp, 2);
        greeting.extended_capability = mysql_uint2korr(tmp);
    }
    auth_.cut1((char*)&greeting.auth_plugin_length);
    auth_.pop_front(10);
    auth_.cut_until(&greeting.salt2, MYSQL_STRING_NULL);

    LOG(INFO) << "print auth: \n"
              << greeting.protocol << "\n"
              << greeting.version << "\n"
              << greeting.thread_id << "\n"
              << greeting.salt << "\n"
              << greeting.capability << "\n"
              << greeting.language << "\n"
              << greeting.status << "\n"
              << greeting.extended_capability << "\n"
              << greeting.auth_plugin_length << "\n"
              << greeting.salt2;
    butil::IOBuf salt;
    salt.append(greeting.salt);
    salt.append(greeting.salt2);
    std::string salt_response =
        mysql_build_mysql41_authentication_response(salt.to_string(), "", passwd_, "");

    // salt_response =
    //     mysql_build_mysql41_authentication_response("yxpGP=}$5{2#Ywi,<)W]", "", passwd_, "");
    MysqlAuthResponse response;
    response.capability = 0xa28d;
    response.extended_capability = 0x000f;
    response.max_package_length = 16777216UL;
    response.language = 33;
    response.user = user_;
    response.salt = salt_response;
    response.schema = "";

    butil::IOBuf resp;
    uint16_t capability = butil::ByteSwapToLE16(response.capability);
    resp.append(&capability, 2);
    uint16_t extended_capability = butil::ByteSwapToLE16(response.extended_capability);
    resp.append(&response.extended_capability, 2);
    uint32_t max_package_length = butil::ByteSwapToLE32(response.max_package_length);
    resp.append(&max_package_length, 4);
    resp.append(&response.language, 1);
    for (int i = 0; i < 23; ++i)
        resp.push_back(MYSQL_STRING_NULL[0]);
    resp.append(response.user);
    resp.push_back('\0');
    LOG(INFO) << "user size:" << response.user.size();
    // char c;
    // response.salt.cut1(&c);
    // int salt_size = response.salt.size();
    resp.push_back((uint8_t)(response.salt.size()));
    LOG(INFO) << response.salt;
    resp.append(response.salt);
    LOG(INFO) << "salt size:" << response.salt.size();
    // resp.append("mysql_native_password");
    resp.append("test");
    uint32_t package_length = butil::ByteSwapToLE32(resp.size());
    LOG(INFO) << "package_length:" << resp.size();
    butil::IOBuf resp_final;
    resp_final.append(&package_length, 3);
    resp_final.push_back(0x01);
    resp_final.append(resp);
    *auth_str = resp_final.to_string();
    return 0;
}

MysqlAuthenticator* global_mysql_authenticator() {
    // return butil::get_leaky_singleton<MysqlAuthenticator>();
    return g_mysql_authenticator;
}

const Authenticator* global_mysql_authenticator(const std::string& user,
                                                const std::string& passwd) {
    // return butil::get_leaky_singleton<MysqlAuthenticator>();
    g_mysql_authenticator = new MysqlAuthenticator(user, passwd);
    return g_mysql_authenticator;
}

}  // namespace policy
}  // namespace brpc
