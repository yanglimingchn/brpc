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

#include <google/protobuf/descriptor.h>  // MethodDescriptor
#include <google/protobuf/message.h>     // Message
#include <gflags/gflags.h>
#include "butil/logging.h"  // LOG()
#include "butil/time.h"
#include "butil/iobuf.h"  // butil::IOBuf
#include "butil/sys_byteorder.h"
#include "brpc/controller.h"  // Controller
#include "brpc/details/controller_private_accessor.h"
#include "brpc/socket.h"  // Socket
#include "brpc/server.h"  // Server
#include "brpc/details/server_private_accessor.h"
#include "brpc/span.h"
#include "brpc/mysql.h"
#include "brpc/mysql_util.h"
#include "brpc/policy/mysql_protocol.h"
#include "brpc/policy/most_common_message.h"
#include "brpc/policy/mysql_authenticator.h"

namespace brpc {

DECLARE_bool(enable_rpcz);

namespace policy {

DEFINE_bool(mysql_verbose, false, "[DEBUG] Print EVERY mysql request/response");

// "Message" = "Response" as we only implement the client for mysql.
ParseResult ParseMysqlMessage(butil::IOBuf* source,
                              Socket* socket,
                              bool /*read_eof*/,
                              const void* /*arg*/) {
    if (source->empty()) {
        return MakeParseError(PARSE_ERROR_NOT_ENOUGH_DATA);
    }

    PipelinedInfo pi;
    if (!socket->PopPipelinedInfo(&pi)) {
        LOG(WARNING) << "No corresponding PipelinedInfo in socket";
        return MakeParseError(PARSE_ERROR_TRY_OTHERS);
    }

    do {
        MostCommonMessage* msg = static_cast<MostCommonMessage*>(socket->parsing_context());
        if (msg == NULL) {
            msg = MostCommonMessage::Get();
            socket->reset_parsing_context(msg);
        }
        char buf[4];
        const uint8_t* p = (const uint8_t*)source->fetch(buf, sizeof(buf));
        if (NULL == p) {
            return MakeParseError(PARSE_ERROR_NOT_ENOUGH_DATA);
        }

        uint32_t pack_length = mysql_uint3korr(p);
        LOG(INFO) << "pack_length:" << pack_length;
        LOG(INFO) << "source size = " << source->size();
        if (source->size() < 4 + pack_length /*header + payload*/) {
            return MakeParseError(PARSE_ERROR_NOT_ENOUGH_DATA);
        }

        LOG(INFO) << "enter ..";

        source->cutn(&msg->meta, 4);
        source->cutn(&msg->payload, pack_length);

        LOG(INFO) << "with_auth" << pi.with_auth;

        if (pi.with_auth) {
            MysqlAuthenticator* auth = global_mysql_authenticator();
            LOG(INFO) << "auth step=" << auth->CurrStep();
            if (auth->CurrStep() == 0) {  // receive challenge & send auth
                LOG(INFO) << "first time:" << source;
                auth->raw_auth() = msg->payload.movable();
                std::string auth_str;
                if (auth->GenerateCredential(&auth_str) != 0) {
                    return MakeParseError(PARSE_ERROR_ABSOLUTELY_WRONG);
                }
                butil::IOBuf auth_req;
                auth_req.append(auth_str);
                ssize_t size = auth_req.cut_into_file_descriptor(socket->fd(), auth_req.size());
                // return MakeParseError(PARSE_ERROR_ABSOLUTELY_WRONG);
                auth->NextStep();
            } else {  // check auth & send request
                LOG(INFO) << "second time";
                butil::IOBuf& buf = auth->raw_req();
                buf.cut_into_file_descriptor(socket->fd(), buf.size());
                pi.with_auth = false;
            }
            DestroyingPtr<MostCommonMessage> auth_msg =
                static_cast<MostCommonMessage*>(socket->release_parsing_context());
            socket->GivebackPipelinedInfo(pi);
            return MakeParseError(PARSE_ERROR_NOT_ENOUGH_DATA);
        }

        // CHECK_EQ((uint32_t)msg->response.reply_size(), pi.count);
        msg->pi = pi;
        size_t oldsize = source->size();
        std::string context = source->to_string();
        if (FLAGS_mysql_verbose) {
            LOG(INFO) << "\n[MYSQL PARSE] " << context;
        }
        socket->release_parsing_context();
        return MakeMessage(msg);
    } while (true);

    return MakeParseError(PARSE_ERROR_ABSOLUTELY_WRONG);
}

void ProcessMysqlResponse(InputMessageBase* msg_base) {
    const int64_t start_parse_us = butil::cpuwide_time_us();
    DestroyingPtr<MostCommonMessage> msg(static_cast<MostCommonMessage*>(msg_base));

    const bthread_id_t cid = msg->pi.id_wait;
    Controller* cntl = NULL;
    const int rc = bthread_id_lock(cid, (void**)&cntl);
    if (rc != 0) {
        LOG_IF(ERROR, rc != EINVAL && rc != EPERM)
            << "Fail to lock correlation_id=" << cid << ": " << berror(rc);
        return;
    }

    ControllerPrivateAccessor accessor(cntl);
    Span* span = accessor.span();
    if (span) {
        // span->set_base_real_us(msg->base_real_us());
        // span->set_received_us(msg->received_us());
        // span->set_response_size(msg->response.ByteSize());
        // span->set_start_parse_us(start_parse_us);
    }
    const int saved_error = cntl->ErrorCode();
    // if (cntl->response() != NULL) {
    //     if (cntl->response()->GetDescriptor() != MysqlResponse::descriptor()) {
    //         cntl->SetFailed(ERESPONSE, "Must be MysqlResponse");
    //     } else {
    //         // We work around ParseFrom of pb which is just a placeholder.
    //         if (msg->response.reply_size() != (int)accessor.pipelined_count()) {
    //             cntl->SetFailed(ERESPONSE,
    //                             "pipelined_count=%d of response does "
    //                             "not equal request's=%d",
    //                             msg->response.reply_size(),
    //                             accessor.pipelined_count());
    //         }
    //         ((MysqlResponse*)cntl->response())->Swap(&msg->response);
    //         if (FLAGS_mysql_verbose) {
    //             LOG(INFO) << "\n[MYSQL RESPONSE] " << *((MysqlResponse*)cntl->response());
    //         }
    //     }
    // }  // silently ignore the response.

    if (FLAGS_mysql_verbose) {
        LOG(INFO) << "\n[MYSQL RESPONSE] ";
    }

    // Unlocks correlation_id inside. Revert controller's
    // error code if it version check of `cid' fails
    msg.reset();  // optional, just release resourse ASAP
    accessor.OnResponse(cid, saved_error);
}

void SerializeMysqlRequest(butil::IOBuf* buf,
                           Controller* cntl,
                           const google::protobuf::Message* request) {
    if (request == NULL) {
        return cntl->SetFailed(EREQUEST, "request is NULL");
    }
    if (request->GetDescriptor() != MysqlRequest::descriptor()) {
        return cntl->SetFailed(EREQUEST, "The request is not a MysqlRequest");
    }
    const MysqlRequest* rr = (const MysqlRequest*)request;
    // We work around SerializeTo of pb which is just a placeholder.
    if (!rr->SerializeTo(buf)) {
        return cntl->SetFailed(EREQUEST, "Fail to serialize MysqlRequest");
    }
    ControllerPrivateAccessor(cntl).set_pipelined_count(1);
    if (FLAGS_mysql_verbose) {
        LOG(INFO) << "\n[MYSQL REQUEST] " << *rr;
    }
}

void PackMysqlRequest(butil::IOBuf* buf,
                      SocketMessage**,
                      uint64_t /*correlation_id*/,
                      const google::protobuf::MethodDescriptor*,
                      Controller* cntl,
                      const butil::IOBuf& request,
                      const Authenticator* auth) {
    if (auth) {
        // std::string auth_str;
        // if (auth->GenerateCredential(&auth_str) != 0) {
        //     return cntl->SetFailed(EREQUEST, "Fail to generate credential");
        // }
        // buf->append(auth_str);
        const MysqlAuthenticator* myauth(dynamic_cast<const MysqlAuthenticator*>(auth));
        ControllerPrivateAccessor(cntl).add_with_auth();
        if (FLAGS_mysql_verbose) {
            LOG(INFO) << "\n[MYSQL PACK] we need auth";
        }
        butil::IOBuf body;
        butil::IOBuf req;
        body.push_back(0x03);
        body.append("SELECT * from runoob_tbl");
        uint32_t package_length = butil::ByteSwapToLE32(body.size());
        req.append(&package_length, 3);
        req.push_back(0x00);
        req.append(body);
        ((MysqlAuthenticator*)myauth)->raw_req() = req;
    } else {
        buf->append(request);
    }
}

const std::string& GetMysqlMethodName(const google::protobuf::MethodDescriptor*,
                                      const Controller*) {
    const static std::string MYSQL_SERVER_STR = "mysql-server";
    return MYSQL_SERVER_STR;
}

}  // namespace policy
}  // namespace brpc
