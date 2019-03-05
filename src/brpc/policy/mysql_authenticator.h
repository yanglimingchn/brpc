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
#include "brpc/mysql_reply.h"

namespace brpc {
namespace policy {
// auth step 1)greeting 2) response 3) ok
enum MysqlAuthStep {
    MYSQL_AUTH_STEP_ONE,
    MYSQL_AUTH_STEP_TWO,
};
// Request to mysql for authentication.
class MysqlAuthenticator : public Authenticator {
public:
    MysqlAuthenticator(const std::string& user, const std::string& passwd, const std::string& db)
        : _user(user), _passwd(passwd), _db(db), _step(MYSQL_AUTH_STEP_ONE) {}

    int GenerateCredential(std::string* auth_str) const;

    int VerifyCredential(const std::string&, const butil::EndPoint&, brpc::AuthContext*) const {
        return 0;
    }

    void SaveAuth(const MysqlReply::Auth* auth);
    MysqlAuthStep CurrStep() const;
    void NextStep();
    butil::IOBuf& raw_req() const;

private:
    const std::string _user;
    const std::string _passwd;
    const std::string _db;
    MysqlAuthStep _step;
    mutable butil::IOBuf _req;
    const MysqlReply::Auth* _auth;
};

inline MysqlAuthStep MysqlAuthenticator::CurrStep() const {
    return _step;
}

inline void MysqlAuthenticator::NextStep() {
    _step = MYSQL_AUTH_STEP_TWO;
}

inline butil::IOBuf& MysqlAuthenticator::raw_req() const {
    return _req;
}

const Authenticator* global_mysql_authenticator(const std::string& user,
                                                const std::string& passwd,
                                                const std::string& db = "");

}  // namespace policy
}  // namespace brpc

#endif  // BRPC_POLICY_COUCHBASE_AUTHENTICATOR_H
