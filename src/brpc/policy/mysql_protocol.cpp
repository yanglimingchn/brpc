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
#include "brpc/mysql_reply.h"
#include "brpc/policy/mysql_protocol.h"
#include "brpc/policy/most_common_message.h"
#include "brpc/policy/mysql_authenticator.h"

namespace brpc {

DECLARE_bool(enable_rpcz);

namespace policy {

DEFINE_bool(mysql_verbose, false, "[DEBUG] Print EVERY mysql request/response");


MysqlAuthenticator* global_mysql_authenticator();

struct InputResponse : public InputMessageBase {
    bthread_id_t id_wait;
    MysqlResponse response;

    // @InputMessageBase
    void DestroyImpl() {
        delete this;
    }
};

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
        LOG(INFO) << "got msg";
        InputResponse* msg = static_cast<InputResponse*>(socket->parsing_context());
        if (msg == NULL) {
            LOG(INFO) << "msg is null";
            msg = new InputResponse;
            socket->reset_parsing_context(msg);
        }

        if (msg->response.ConsumePartialIOBuf(*source, pi.with_auth) == false) {
            LOG(INFO) << "consume return false";
            socket->GivebackPipelinedInfo(pi);
            return MakeParseError(PARSE_ERROR_NOT_ENOUGH_DATA);
        }

        if (pi.with_auth) {
            if (FLAGS_mysql_verbose) {
                LOG(INFO) << "\n[MYSQL RESPONSE] " << msg->response;
            }
            MysqlAuthenticator* auth = global_mysql_authenticator();
            if (auth->CurrStep() == MYSQL_AUTH_STEP_ONE) {  // receive auth & send auth
                auth->SaveAuth(msg->response.reply(0).auth());
                std::string auth_data;
                if (auth->GenerateCredential(&auth_data) != 0) {
                    LOG(INFO) << "[MYSQL PARSE] authentication step one";
                    return MakeParseError(PARSE_ERROR_ABSOLUTELY_WRONG);
                }
                butil::IOBuf auth_resp;
                auth_resp.append(auth_data);
                auth_resp.cut_into_file_descriptor(socket->fd());
                auth->NextStep();
            } else if (auth->CurrStep() == MYSQL_AUTH_STEP_TWO) {  // check auth & send request
                butil::IOBuf& raw_req = auth->raw_req();
                raw_req.cut_into_file_descriptor(socket->fd());
                pi.with_auth = false;
            } else {
                LOG(ERROR) << "[MYSQL PARSE] wrong authentication step";
                return MakeParseError(PARSE_ERROR_ABSOLUTELY_WRONG);
            }
            DestroyingPtr<InputResponse> auth_msg =
                static_cast<InputResponse*>(socket->release_parsing_context());
            socket->GivebackPipelinedInfo(pi);
            return MakeParseError(PARSE_ERROR_NOT_ENOUGH_DATA);
        }

        msg->id_wait = pi.id_wait;
        socket->release_parsing_context();
        return MakeMessage(msg);
    } while (true);

    return MakeParseError(PARSE_ERROR_ABSOLUTELY_WRONG);
}

void ProcessMysqlResponse(InputMessageBase* msg_base) {
    const int64_t start_parse_us = butil::cpuwide_time_us();
    DestroyingPtr<InputResponse> msg(static_cast<InputResponse*>(msg_base));

    const bthread_id_t cid = msg->id_wait;
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
        span->set_base_real_us(msg->base_real_us());
        span->set_received_us(msg->received_us());
        span->set_response_size(msg->response.ByteSize());
        span->set_start_parse_us(start_parse_us);
    }
    const int saved_error = cntl->ErrorCode();
    if (cntl->response() != NULL) {
        if (cntl->response()->GetDescriptor() != MysqlResponse::descriptor()) {
            cntl->SetFailed(ERESPONSE, "Must be MysqlResponse");
        } else {
            // We work around ParseFrom of pb which is just a placeholder.
            ((MysqlResponse*)cntl->response())->Swap(&msg->response);
            if (FLAGS_mysql_verbose) {
                LOG(INFO) << "\n[MYSQL RESPONSE] " << *((MysqlResponse*)cntl->response());
            }
        }
    }  // silently ignore the response.

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
        const MysqlAuthenticator* my_auth(dynamic_cast<const MysqlAuthenticator*>(auth));
        if (my_auth == NULL) {
            LOG(ERROR) << "[MYSQL PACK] there is not MysqlAuthenticator";
            return;
        }
        ControllerPrivateAccessor(cntl).add_with_auth();
        my_auth->raw_req() = request;
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
