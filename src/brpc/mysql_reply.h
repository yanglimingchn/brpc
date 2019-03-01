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
#include "butil/arena.h"
#include "butil/sys_byteorder.h"

namespace brpc {

const std::string MYSQL_STRING_NULL(1, 0x00);

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

struct MysqlHeader {
    uint32_t payload_size;
    uint32_t seq;
};

class MysqlReply {
public:
    enum ServerStatus {
        SERVER_STATUS_IN_TRANS = 1,
        SERVER_STATUS_AUTOCOMMIT = 2,   /* Server in auto_commit mode */
        SERVER_MORE_RESULTS_EXISTS = 8, /* Multi query - next query exists */
        SERVER_QUERY_NO_GOOD_INDEX_USED = 16,
        SERVER_QUERY_NO_INDEX_USED = 32,
        /**
          The server was able to fulfill the clients request and opened a
          read-only non-scrollable cursor for a query. This flag comes
          in reply to COM_STMT_EXECUTE and COM_STMT_FETCH commands.
        */
        SERVER_STATUS_CURSOR_EXISTS = 64,
        /**
          This flag is sent when a read-only cursor is exhausted, in reply to
          COM_STMT_FETCH command.
        */
        SERVER_STATUS_LAST_ROW_SENT = 128,
        SERVER_STATUS_DB_DROPPED = 256, /* A database was dropped */
        SERVER_STATUS_NO_BACKSLASH_ESCAPES = 512,
        /**
          Sent to the client if after a prepared statement reprepare
          we discovered that the new statement returns a different
          number of result set columns.
        */
        SERVER_STATUS_METADATA_CHANGED = 1024,
        SERVER_QUERY_WAS_SLOW = 2048,

        /**
          To mark ResultSet containing output parameter values.
        */
        SERVER_PS_OUT_PARAMS = 4096,

        /**
          Set at the same time as SERVER_STATUS_IN_TRANS if the started
          multi-statement transaction is a read-only transaction. Cleared
          when the transaction commits or aborts. Since this flag is sent
          to clients in OK and EOF packets, the flag indicates the
          transaction status at the end of command execution.
        */
        SERVER_STATUS_IN_TRANS_READONLY = 8192,
        SERVER_SESSION_STATE_CHANGED = 1UL << 14,
    };
    enum RspType {
        RSP_OK = 0x00,
        RSP_ERROR = 0xFF,
        RSP_RESULTSET = 0x01,
        RSP_EOF = 0xFE,
    };
    enum FieldType {
        FIELD_TYPE_DECIMAL = 0x00,
        FIELD_TYPE_TINY = 0x01,
        FIELD_TYPE_SHORT = 0x02,
        FIELD_TYPE_LONG = 0x03,
        FIELD_TYPE_FLOAT = 0x04,
        FIELD_TYPE_DOUBLE = 0x05,
        FIELD_TYPE_NULL = 0x06,
        FIELD_TYPE_TIMESTAMP = 0x07,
        FIELD_TYPE_LONGLONG = 0x08,
        FIELD_TYPE_INT24 = 0x09,
        FIELD_TYPE_DATE = 0x0A,
        FIELD_TYPE_TIME = 0x0B,
        FIELD_TYPE_DATETIME = 0x0C,
        FIELD_TYPE_YEAR = 0x0D,
        FIELD_TYPE_NEWDATE = 0x0E,
        FIELD_TYPE_VARCHAR = 0x0F,
        FIELD_TYPE_BIT = 0x10,
        FIELD_TYPE_JSON = 0xF5,
        FIELD_TYPE_NEWDECIMAL = 0xF6,
        FIELD_TYPE_ENUM = 0xF7,
        FIELD_TYPE_SET = 0xF8,
        FIELD_TYPE_TINY_BLOB = 0xF9,
        FIELD_TYPE_MEDIUM_BLOB = 0xFA,
        FIELD_TYPE_LONG_BLOB = 0xFB,
        FIELD_TYPE_BLOB = 0xFC,
        FIELD_TYPE_VAR_STRING = 0xFD,
        FIELD_TYPE_STRING = 0xFE,
        FIELD_TYPE_GEOMETRY = 0xFF,
    };
    enum FieldFlag {
        NOT_NULL_FLAG = 0x0001,
        PRI_KEY_FLAG = 0x0002,
        UNIQUE_KEY_FLAG = 0x0004,
        MULTIPLE_KEY_FLAG = 0x0008,
        BLOB_FLAG = 0x0010,
        UNSIGNED_FLAG = 0x0020,
        ZEROFILL_FLAG = 0x0040,
        BINARY_FLAG = 0x0080,
        ENUM_FLAG = 0x0100,
        AUTO_INCREMENT_FLAG = 0x0200,
        TIMESTAMP_FLAG = 0x0400,
        SET_FLAG = 0x0800,
    };
    struct Ok {
        uint64_t affect_row;
        uint64_t index;
        uint16_t status;
        uint16_t warning;
        butil::StringPiece msg;
    };
    struct Error {
        uint16_t errcode;
        butil::StringPiece status;
        butil::StringPiece msg;
    };
    struct Eof {
        uint16_t warning;
        uint16_t status;
    };
    struct ResultSetHeader {
        uint64_t column_number;
        uint64_t extra_msg;
    };
    struct Column {
        butil::StringPiece catalog;
        butil::StringPiece database;
        butil::StringPiece table;
        butil::StringPiece origin_table;
        butil::StringPiece name;
        butil::StringPiece origin_name;
        uint16_t charset;
        uint32_t length;
        FieldType type;
        FieldFlag flag;
        uint8_t decimal;
    };
    class Field {
    public:
        int8_t stiny() const;
        uint8_t tiny() const;
        int16_t ssmall() const;
        uint16_t small() const;
        int32_t sinteger() const;
        uint32_t integer() const;
        int64_t sbigint() const;
        uint64_t bigint() const;
        float float32() const;
        double float64() const;
        const char* c_str() const;
        butil::StringPiece data() const;
        bool is_null();

    public:
        union {
            int8_t stiny;
            uint8_t tiny;
            int16_t ssmall;
            uint16_t small;
            int32_t sinteger;
            uint32_t integer;
            int64_t sbigint;
            uint64_t bigint;
            float float32;
            double float64;
            const char* str;
        } _data;
        uint64_t _len;
        bool _is_null;
    };
    struct Row {
        const Field* fields;
    };
    struct ResultSet {
        ResultSetHeader header;
        const Column* columns;
        Eof eof1;
        const Row* const* rows;
        uint64_t row_number;
        Eof eof2;
    };

public:
    MysqlReply(){};
    bool ConsumePartialIOBuf(butil::IOBuf& buf, butil::Arena* arena);
    void Swap(MysqlReply& other);
    void Print(std::ostream& os) const;
    // get column number
    uint64_t column_number() const;
    // get one column
    const Column& column(const uint64_t index) const;
    // get row number
    uint64_t row_number() const;
    // get one row
    const Row& row(const uint64_t index) const;
    // get one field
    const Field& field(const Row& row, const uint64_t index) const;

private:
    DISALLOW_COPY_AND_ASSIGN(MysqlReply);

    RspType _type;
    union {
        const ResultSet* result_set;
        const Ok* ok;
        const Error* error;
        const Eof* eof;
        const void* padding;  // For swapping
    } _data;
};

inline void MysqlReply::Swap(MysqlReply& other) {
    std::swap(_type, other._type);
    std::swap(_data.padding, other._data.padding);
}

inline std::ostream& operator<<(std::ostream& os, const MysqlReply& r) {
    r.Print(os);
    return os;
}

// get column number
inline uint64_t MysqlReply::column_number() const {
    return _data.result_set->header.column_number;
}
// get one column
inline const MysqlReply::Column& MysqlReply::column(const uint64_t index) const {
    return _data.result_set->columns[index];
}
// get row number
inline uint64_t MysqlReply::row_number() const {
    return _data.result_set->row_number;
}
// get one row
inline const MysqlReply::Row& MysqlReply::row(const uint64_t index) const {
    return *_data.result_set->rows[index];
}
// get one field
inline const MysqlReply::Field& MysqlReply::field(const MysqlReply::Row& row,
                                                  const uint64_t index) const {
    return row.fields[index];
}

inline int8_t MysqlReply::Field::stiny() const {
    return _data.stiny;
}
inline uint8_t MysqlReply::Field::tiny() const {
    return _data.tiny;
}
inline int16_t MysqlReply::Field::ssmall() const {
    return _data.ssmall;
}
inline uint16_t MysqlReply::Field::small() const {
    return _data.small;
}
inline int32_t MysqlReply::Field::sinteger() const {
    return _data.sinteger;
}
inline uint32_t MysqlReply::Field::integer() const {
    return _data.integer;
}
inline int64_t MysqlReply::Field::sbigint() const {
    return _data.sbigint;
}
inline uint64_t MysqlReply::Field::bigint() const {
    return _data.bigint;
}
inline float MysqlReply::Field::float32() const {
    return _data.float32;
}
inline double MysqlReply::Field::float64() const {
    return _data.float64;
}
inline const char* MysqlReply::Field::c_str() const {
    return _data.str;
}
inline butil::StringPiece MysqlReply::Field::data() const {
    return butil::StringPiece(_data.str, _len);
}
inline bool MysqlReply::Field::is_null() {
    return _is_null;
}

// little endian order to host order
inline uint16_t mysql_uint2korr(const uint8_t* A) {
    return (uint16_t)(((uint16_t)(A[0])) + ((uint16_t)(A[1]) << 8));
}

inline uint32_t mysql_uint3korr(const uint8_t* A) {
    return (uint32_t)(((uint32_t)(A[0])) + (((uint32_t)(A[1])) << 8) + (((uint32_t)(A[2])) << 16));
}

inline uint32_t mysql_uint4korr(const uint8_t* A) {
    return (uint32_t)(((uint32_t)(A[0])) + (((uint32_t)(A[1])) << 8) + (((uint32_t)(A[2])) << 16) +
                      (((uint32_t)(A[3])) << 24));
}

inline uint64_t mysql_uint8korr(const uint8_t* A) {
    return (uint64_t)(((uint64_t)(A[0])) + (((uint64_t)(A[1])) << 8) + (((uint64_t)(A[2])) << 16) +
                      (((uint64_t)(A[3])) << 24) + (((uint64_t)(A[4])) << 32) +
                      (((uint64_t)(A[5])) << 40) + (((uint64_t)(A[6])) << 48) +
                      (((uint64_t)(A[7])) << 56));
}

}  // namespace brpc

#endif  // BRPC_MYSQL_UTIL_H
