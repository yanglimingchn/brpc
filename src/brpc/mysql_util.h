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

// Authors: Yang,Liming (yangliming01@baidu.com)

#ifndef BRPC_MYSQL_UTIL_H
#define BRPC_MYSQL_UTIL_H

#include "butil/iobuf.h"  // butil::IOBuf

namespace brpc {
extern const std::string MYSQL_STRING_NULL;
struct MysqlGreeting {
    uint8_t protocol;
    butil::IOBuf version;
    uint32_t thread_id;
    butil::IOBuf salt;
    uint16_t capability;
    uint8_t language;
    uint16_t status;
    uint16_t extended_capability;
    uint8_t auth_plugin_length;
    butil::IOBuf salt2;
    // butil::IOBuf auth_plugin;
};

struct MysqlAuthResponse {
    uint16_t capability;
    uint16_t extended_capability;
    uint32_t max_package_length;
    uint8_t language;
    butil::IOBuf user;
    butil::IOBuf salt;
    butil::IOBuf schema;
};

static inline uint16_t mysql_uint2korr(const uint8_t* A) {
    return (uint16_t)(((uint16_t)(A[0])) + ((uint16_t)(A[1]) << 8));
}

static inline uint32_t mysql_uint3korr(const uint8_t* A) {
    return (uint32_t)(((uint32_t)(A[0])) + (((uint32_t)(A[1])) << 8) + (((uint32_t)(A[2])) << 16));
}

static inline uint32_t mysql_uint4korr(const uint8_t* A) {
    return (uint32_t)(((uint32_t)(A[0])) + (((uint32_t)(A[1])) << 8) + (((uint32_t)(A[2])) << 16) +
                      (((uint32_t)(A[3])) << 24));
}

}  // namespace brpc

#endif  // BRPC_MYSQL_UTIL_H
