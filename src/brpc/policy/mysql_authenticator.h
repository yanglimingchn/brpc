// Copyright (c) 2019 Baidu, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Author(s): Yang,Liming <yangliming01@baidu.com>

#ifndef BRPC_POLICY_MYSQL_AUTHENTICATOR_H
#define BRPC_POLICY_MYSQL_AUTHENTICATOR_H

#include "butil/iobuf.h"
#include "brpc/authenticator.h"

namespace brpc {
namespace policy {

// Request to mysql for authentication.
class MysqlAuthenticator : public Authenticator {
public:
    MysqlAuthenticator(const std::string& user, const std::string& passwd)
        : user_(user), passwd_(passwd), step_(0) {}

    int GenerateCredential(std::string* auth_str) const;

    int VerifyCredential(const std::string&, const butil::EndPoint&, brpc::AuthContext*) const {
        return 0;
    }

    int CurrStep() const {
        return step_;
    }

    void NextStep() {
        ++step_;
    }

    butil::IOBuf& raw_req() {
        return req_;
    }

    butil::IOBuf& raw_auth() {
        return auth_;
    }

private:
    const std::string user_;
    const std::string passwd_;
    int step_;
    butil::IOBuf req_;
    mutable butil::IOBuf auth_;
};

MysqlAuthenticator* global_mysql_authenticator();
const Authenticator* global_mysql_authenticator(const std::string& user, const std::string& passwd);

}  // namespace policy
}  // namespace brpc

#endif  // BRPC_POLICY_COUCHBASE_AUTHENTICATOR_H
