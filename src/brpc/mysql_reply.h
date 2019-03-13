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
#include "butil/logging.h"  // LOG()

namespace brpc {

struct MysqlHeader {
    uint32_t payload_size;
    uint32_t seq;
};

enum MysqlRspType : unsigned char {
    RSP_OK = 0x00,
    RSP_ERROR = 0xFF,
    RSP_RESULTSET = 0x01,
    RSP_EOF = 0xFE,
    RSP_AUTH = 0xFB,     // add for mysql auth
    RSP_UNKNOWN = 0xFC,  // add for other case
};

enum MysqlFieldType : unsigned char {
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

enum MysqlFieldFlag : unsigned short {
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

enum MysqlServerStatus : unsigned short {
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

// Msql Collation
enum MysqlCollation : unsigned short {
    big5_chinese_ci = 1,
    latin2_czech_cs = 2,
    dec8_swedish_ci = 3,
    cp850_general_ci = 4,
    latin1_german1_ci = 5,
    hp8_english_ci = 6,
    koi8r_general_ci = 7,
    latin1_swedish_ci = 8,
    latin2_general_ci = 9,
    swe7_swedish_ci = 10,
    ascii_general_ci = 11,
    ujis_japanese_ci = 12,
    sjis_japanese_ci = 13,
    cp1251_bulgarian_ci = 14,
    latin1_danish_ci = 15,
    hebrew_general_ci = 16,
    tis620_thai_ci = 18,
    euckr_korean_ci = 19,
    latin7_estonian_cs = 20,
    latin2_hungarian_ci = 21,
    koi8u_general_ci = 22,
    cp1251_ukrainian_ci = 23,
    gb2312_chinese_ci = 24,
    greek_general_ci = 25,
    cp1250_general_ci = 26,
    latin2_croatian_ci = 27,
    gbk_chinese_ci = 28,
    cp1257_lithuanian_ci = 29,
    latin5_turkish_ci = 30,
    latin1_german2_ci = 31,
    armscii8_general_ci = 32,
    utf8_general_ci = 33,
    cp1250_czech_cs = 34,
    ucs2_general_ci = 35,
    cp866_general_ci = 36,
    keybcs2_general_ci = 37,
    macce_general_ci = 38,
    macroman_general_ci = 39,
    cp852_general_ci = 40,
    latin7_general_ci = 41,
    latin7_general_cs = 42,
    macce_bin = 43,
    cp1250_croatian_ci = 44,
    utf8mb4_general_ci = 45,
    utf8mb4_bin = 46,
    latin1_bin = 47,
    latin1_general_ci = 48,
    latin1_general_cs = 49,
    cp1251_bin = 50,
    cp1251_general_ci = 51,
    cp1251_general_cs = 52,
    macroman_bin = 53,
    utf16_general_ci = 54,
    utf16_bin = 55,
    utf16le_general_ci = 56,
    cp1256_general_ci = 57,
    cp1257_bin = 58,
    cp1257_general_ci = 59,
    utf32_general_ci = 60,
    utf32_bin = 61,
    utf16le_bin = 62,
    binary = 63,
    armscii8_bin = 64,
    ascii_bin = 65,
    cp1250_bin = 66,
    cp1256_bin = 67,
    cp866_bin = 68,
    dec8_bin = 69,
    greek_bin = 70,
    hebrew_bin = 71,
    hp8_bin = 72,
    keybcs2_bin = 73,
    koi8r_bin = 74,
    koi8u_bin = 75,
    latin2_bin = 77,
    latin5_bin = 78,
    latin7_bin = 79,
    cp850_bin = 80,
    cp852_bin = 81,
    swe7_bin = 82,
    utf8_bin = 83,
    big5_bin = 84,
    euckr_bin = 85,
    gb2312_bin = 86,
    gbk_bin = 87,
    sjis_bin = 88,
    tis620_bin = 89,
    ucs2_bin = 90,
    ujis_bin = 91,
    geostd8_general_ci = 92,
    geostd8_bin = 93,
    latin1_spanish_ci = 94,
    cp932_japanese_ci = 95,
    cp932_bin = 96,
    eucjpms_japanese_ci = 97,
    eucjpms_bin = 98,
    cp1250_polish_ci = 99,
    utf16_unicode_ci = 101,
    utf16_icelandic_ci = 102,
    utf16_latvian_ci = 103,
    utf16_romanian_ci = 104,
    utf16_slovenian_ci = 105,
    utf16_polish_ci = 106,
    utf16_estonian_ci = 107,
    utf16_spanish_ci = 108,
    utf16_swedish_ci = 109,
    utf16_turkish_ci = 110,
    utf16_czech_ci = 111,
    utf16_danish_ci = 112,
    utf16_lithuanian_ci = 113,
    utf16_slovak_ci = 114,
    utf16_spanish2_ci = 115,
    utf16_roman_ci = 116,
    utf16_persian_ci = 117,
    utf16_esperanto_ci = 118,
    utf16_hungarian_ci = 119,
    utf16_sinhala_ci = 120,
    utf16_german2_ci = 121,
    utf16_croatian_ci = 122,
    utf16_unicode_520_ci = 123,
    utf16_vietnamese_ci = 124,
    ucs2_unicode_ci = 128,
    ucs2_icelandic_ci = 129,
    ucs2_latvian_ci = 130,
    ucs2_romanian_ci = 131,
    ucs2_slovenian_ci = 132,
    ucs2_polish_ci = 133,
    ucs2_estonian_ci = 134,
    ucs2_spanish_ci = 135,
    ucs2_swedish_ci = 136,
    ucs2_turkish_ci = 137,
    ucs2_czech_ci = 138,
    ucs2_danish_ci = 139,
    ucs2_lithuanian_ci = 140,
    ucs2_slovak_ci = 141,
    ucs2_spanish2_ci = 142,
    ucs2_roman_ci = 143,
    ucs2_persian_ci = 144,
    ucs2_esperanto_ci = 145,
    ucs2_hungarian_ci = 146,
    ucs2_sinhala_ci = 147,
    ucs2_german2_ci = 148,
    ucs2_croatian_ci = 149,
    ucs2_unicode_520_ci = 150,
    ucs2_vietnamese_ci = 151,
    ucs2_general_mysql500_ci = 159,
    utf32_unicode_ci = 160,
    utf32_icelandic_ci = 161,
    utf32_latvian_ci = 162,
    utf32_romanian_ci = 163,
    utf32_slovenian_ci = 164,
    utf32_polish_ci = 165,
    utf32_estonian_ci = 166,
    utf32_spanish_ci = 167,
    utf32_swedish_ci = 168,
    utf32_turkish_ci = 169,
    utf32_czech_ci = 170,
    utf32_danish_ci = 171,
    utf32_lithuanian_ci = 172,
    utf32_slovak_ci = 173,
    utf32_spanish2_ci = 174,
    utf32_roman_ci = 175,
    utf32_persian_ci = 176,
    utf32_esperanto_ci = 177,
    utf32_hungarian_ci = 178,
    utf32_sinhala_ci = 179,
    utf32_german2_ci = 180,
    utf32_croatian_ci = 181,
    utf32_unicode_520_ci = 182,
    utf32_vietnamese_ci = 183,
    utf8_unicode_ci = 192,
    utf8_icelandic_ci = 193,
    utf8_latvian_ci = 194,
    utf8_romanian_ci = 195,
    utf8_slovenian_ci = 196,
    utf8_polish_ci = 197,
    utf8_estonian_ci = 198,
    utf8_spanish_ci = 199,
    utf8_swedish_ci = 200,
    utf8_turkish_ci = 201,
    utf8_czech_ci = 202,
    utf8_danish_ci = 203,
    utf8_lithuanian_ci = 204,
    utf8_slovak_ci = 205,
    utf8_spanish2_ci = 206,
    utf8_roman_ci = 207,
    utf8_persian_ci = 208,
    utf8_esperanto_ci = 209,
    utf8_hungarian_ci = 210,
    utf8_sinhala_ci = 211,
    utf8_german2_ci = 212,
    utf8_croatian_ci = 213,
    utf8_unicode_520_ci = 214,
    utf8_vietnamese_ci = 215,
    utf8_general_mysql500_ci = 223,
    utf8mb4_unicode_ci = 224,
    utf8mb4_icelandic_ci = 225,
    utf8mb4_latvian_ci = 226,
    utf8mb4_romanian_ci = 227,
    utf8mb4_slovenian_ci = 228,
    utf8mb4_polish_ci = 229,
    utf8mb4_estonian_ci = 230,
    utf8mb4_spanish_ci = 231,
    utf8mb4_swedish_ci = 232,
    utf8mb4_turkish_ci = 233,
    utf8mb4_czech_ci = 234,
    utf8mb4_danish_ci = 235,
    utf8mb4_lithuanian_ci = 236,
    utf8mb4_slovak_ci = 237,
    utf8mb4_spanish2_ci = 238,
    utf8mb4_roman_ci = 239,
    utf8mb4_persian_ci = 240,
    utf8mb4_esperanto_ci = 241,
    utf8mb4_hungarian_ci = 242,
    utf8mb4_sinhala_ci = 243,
    utf8mb4_german2_ci = 244,
    utf8mb4_croatian_ci = 245,
    utf8mb4_unicode_520_ci = 246,
    utf8mb4_vietnamese_ci = 247,
};

const char* MysqlFieldTypeToString(MysqlFieldType);
const char* MysqlRspTypeToString(MysqlRspType);

class MysqlReply {
public:
    // Mysql Auth package
    class Auth {
    public:
        uint8_t protocol() const;
        butil::StringPiece version() const;
        uint32_t thread_id() const;
        butil::StringPiece salt() const;
        uint16_t capability() const;
        uint8_t language() const;
        uint16_t status() const;
        uint16_t extended_capability() const;
        uint8_t auth_plugin_length() const;
        butil::StringPiece salt2() const;
        butil::StringPiece auth_plugin() const;

    private:
        bool parseAuth(butil::IOBuf& buf, butil::Arena* arena);
        bool is_parsed() const;
        void set_parsed();

        DISALLOW_COPY_AND_ASSIGN(Auth);
        friend class MysqlReply;

        uint8_t _protocol;
        butil::StringPiece _version;
        uint32_t _thread_id;
        butil::StringPiece _salt;
        uint16_t _capability;
        uint8_t _language;
        uint16_t _status;
        uint16_t _extended_capability;
        uint8_t _auth_plugin_length;
        butil::StringPiece _salt2;
        butil::StringPiece _auth_plugin;
        // if it is parsed
        bool _is_parsed;
    };
    // Mysql Ok package
    class Ok {
    public:
        uint64_t affect_row() const;
        uint64_t index() const;
        uint16_t status() const;
        uint16_t warning() const;
        butil::StringPiece msg() const;

    private:
        bool parseOk(butil::IOBuf& buf, butil::Arena* arena);
        bool is_parsed() const;
        void set_parsed();

        DISALLOW_COPY_AND_ASSIGN(Ok);
        friend class MysqlReply;

        uint64_t _affect_row;
        uint64_t _index;
        uint16_t _status;
        uint16_t _warning;
        butil::StringPiece _msg;
        // if it is parsed
        bool _is_parsed;
    };
    // Mysql Error package
    class Error {
    public:
        uint16_t errcode() const;
        butil::StringPiece status() const;
        butil::StringPiece msg() const;

    private:
        bool parseError(butil::IOBuf& buf, butil::Arena* arena);
        bool is_parsed() const;
        void set_parsed();

        DISALLOW_COPY_AND_ASSIGN(Error);
        friend class MysqlReply;

        uint16_t _errcode;
        butil::StringPiece _status;
        butil::StringPiece _msg;
        // if it is parsed
        bool _is_parsed;
    };
    // Mysql Eof package
    class Eof {
    public:
        Eof() {}
        uint16_t warning() const;
        uint16_t status() const;

    private:
        bool parseEof(butil::IOBuf& buf);
        bool is_parsed() const;
        void set_parsed();
        bool isEof(const butil::IOBuf& buf);

        DISALLOW_COPY_AND_ASSIGN(Eof);
        friend class MysqlReply;

        uint16_t _warning;
        uint16_t _status;
        // if it is parsed
        bool _is_parsed;
    };
    // Mysql Column
    class Column {
    public:
        butil::StringPiece catalog() const;
        butil::StringPiece database() const;
        butil::StringPiece table() const;
        butil::StringPiece origin_table() const;
        butil::StringPiece name() const;
        butil::StringPiece origin_name() const;
        MysqlCollation collation() const;
        uint32_t length() const;
        MysqlFieldType type() const;
        MysqlFieldFlag flag() const;
        uint8_t decimal() const;

    private:
        bool parseColumn(butil::IOBuf& buf, butil::Arena* arena);
        bool is_parsed() const;
        void set_parsed();

        DISALLOW_COPY_AND_ASSIGN(Column);
        friend class MysqlReply;

        butil::StringPiece _catalog;
        butil::StringPiece _database;
        butil::StringPiece _table;
        butil::StringPiece _origin_table;
        butil::StringPiece _name;
        butil::StringPiece _origin_name;
        MysqlCollation _collation;
        uint32_t _length;
        MysqlFieldType _type;
        MysqlFieldFlag _flag;
        uint8_t _decimal;

        // if it is parsed
        bool _is_parsed;
    };
    // Mysql Row
    class Field;
    class Row {
    public:
        uint64_t field_number() const;
        const Field* field(const uint64_t index) const;

    private:
        bool parseTextRow(butil::IOBuf& buf);
        bool is_parsed() const;
        void set_parsed();

        DISALLOW_COPY_AND_ASSIGN(Row);
        friend class MysqlReply;

        Field* _fields;
        uint64_t _field_number;
        Row* _next;
        // if it is parsed
        bool _is_parsed;
    };
    // Mysql Field
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
        butil::StringPiece string() const;

        bool is_stiny() const;
        bool is_tiny() const;
        bool is_ssmall() const;
        bool is_small() const;
        bool is_sinteger() const;
        bool is_integer() const;
        bool is_sbigint() const;
        bool is_bigint() const;
        bool is_float32() const;
        bool is_float64() const;
        bool is_string() const;
        bool is_null() const;

    private:
        bool parseField(butil::IOBuf& buf, const MysqlReply::Column* column, butil::Arena* arena);
        bool is_parsed() const;
        void set_parsed();

        DISALLOW_COPY_AND_ASSIGN(Field);
        friend class MysqlReply;
        // friend class Row;

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
            butil::StringPiece str;
        } _data;
        MysqlFieldType _type;
        bool _is_null;
        bool _is_unsigned;

        // if it is parsed
        bool _is_parsed;
    };

public:
    MysqlReply();
    bool ConsumePartialIOBuf(butil::IOBuf& buf,
                             butil::Arena* arena,
                             const bool is_auth,
                             bool* is_multi);
    void Swap(MysqlReply& other);
    void Print(std::ostream& os) const;
    // response type
    MysqlRspType type() const;
    // get auth
    const Auth* auth() const;
    const Ok* ok() const;
    const Error* error() const;
    const Eof* eof() const;
    bool is_auth() const;
    bool is_ok() const;
    bool is_error() const;
    bool is_eof() const;
    bool is_resultset() const;
    // get column number
    uint64_t column_number() const;
    // get one column
    const Column* column(const uint64_t index) const;
    // get row number
    uint64_t row_number() const;
    // get one row
    const Row* next() const;

private:
    // Mysql result set header
    struct ResultSetHeader {
        bool parseResultHeader(butil::IOBuf& buf);
        bool is_parsed() const {
            return _is_parsed;
        }
        void set_parsed() {
            _is_parsed = true;
        }
        uint64_t _column_number;
        uint64_t _extra_msg;

    private:
        DISALLOW_COPY_AND_ASSIGN(ResultSetHeader);
        bool _is_parsed;
    };
    // Mysql result set
    struct ResultSet {
        ResultSetHeader _header;
        Column* _columns;
        Eof _eof1;
        // row list
        Row* _first;
        Row* _last;
        Row* _cur;
        // row list end
        uint64_t _row_number;
        Eof _eof2;

    private:
        DISALLOW_COPY_AND_ASSIGN(ResultSet);
    };
    // member values
    MysqlRspType _type;
    union {
        const Auth* auth;
        union {
            const ResultSet* const_var;
            // build full result set, we may call ConsumePartialIOBuf many times
            ResultSet* var;
        } result_set;
        const Ok* ok;
        const Error* error;
        const Eof* eof;
        const void* padding;  // For swapping
    } _data;

    DISALLOW_COPY_AND_ASSIGN(MysqlReply);
};

// mysql reply
inline MysqlReply::MysqlReply() {
    _type = RSP_UNKNOWN;
    _data.padding = NULL;
}
inline void MysqlReply::Swap(MysqlReply& other) {
    std::swap(_type, other._type);
    std::swap(_data.padding, other._data.padding);
}
inline std::ostream& operator<<(std::ostream& os, const MysqlReply& r) {
    r.Print(os);
    return os;
}
inline MysqlRspType MysqlReply::type() const {
    return _type;
}
inline const MysqlReply::Auth* MysqlReply::auth() const {
    if (is_auth()) {
        return _data.auth;
    }
    CHECK(false) << "The reply is " << MysqlRspTypeToString(_type) << ", not an auth";
    return NULL;
}
inline const MysqlReply::Ok* MysqlReply::ok() const {
    if (is_ok()) {
        return _data.ok;
    }
    CHECK(false) << "The reply is " << MysqlRspTypeToString(_type) << ", not an ok";
    return NULL;
}
inline const MysqlReply::Error* MysqlReply::error() const {
    if (is_error()) {
        return _data.error;
    }
    CHECK(false) << "The reply is " << MysqlRspTypeToString(_type) << ", not an error";
    return NULL;
}
inline const MysqlReply::Eof* MysqlReply::eof() const {
    if (is_eof()) {
        return _data.eof;
    }
    CHECK(false) << "The reply is " << MysqlRspTypeToString(_type) << ", not an eof";
    return NULL;
}
inline bool MysqlReply::is_auth() const {
    return _type == RSP_AUTH;
}
inline bool MysqlReply::is_ok() const {
    return _type == RSP_OK;
}
inline bool MysqlReply::is_error() const {
    return _type == RSP_ERROR;
}
inline bool MysqlReply::is_eof() const {
    return _type == RSP_EOF;
}
inline bool MysqlReply::is_resultset() const {
    return _type == RSP_RESULTSET;
}
inline uint64_t MysqlReply::column_number() const {
    if (is_resultset()) {
        return _data.result_set.const_var->_header._column_number;
    }
    CHECK(false) << "The reply is " << MysqlRspTypeToString(_type) << ", not an resultset";
    return 0;
}
inline const MysqlReply::Column* MysqlReply::column(const uint64_t index) const {
    if (is_resultset()) {
        if (index > _data.result_set.const_var->_header._column_number) {
            LOG(ERROR) << "wrong index, must between [0, "
                       << _data.result_set.const_var->_header._column_number << ")";
            return NULL;
        }
        return _data.result_set.const_var->_columns + index;
    }
    CHECK(false) << "The reply is " << MysqlRspTypeToString(_type) << ", not an resultset";
    return NULL;
}
inline uint64_t MysqlReply::row_number() const {
    if (is_resultset()) {
        return _data.result_set.const_var->_row_number;
    }
    CHECK(false) << "The reply is " << MysqlRspTypeToString(_type) << ", not an resultset";
    return 0;
}
inline const MysqlReply::Row* MysqlReply::next() const {
    if (is_resultset()) {
        if (_data.result_set.var->_cur == NULL) {
            _data.result_set.var->_cur = _data.result_set.var->_first;
        } else {
            _data.result_set.var->_cur = _data.result_set.var->_cur->_next;
        }
        return _data.result_set.var->_cur;
    }
    CHECK(false) << "The reply is " << MysqlRspTypeToString(_type) << ", not an resultset";
    return NULL;
}
// mysql auth
inline uint8_t MysqlReply::Auth::protocol() const {
    return _protocol;
}
inline butil::StringPiece MysqlReply::Auth::version() const {
    return _version;
}
inline uint32_t MysqlReply::Auth::thread_id() const {
    return _thread_id;
}
inline butil::StringPiece MysqlReply::Auth::salt() const {
    return _salt;
}
inline uint16_t MysqlReply::Auth::capability() const {
    return _capability;
}
inline uint8_t MysqlReply::Auth::language() const {
    return _language;
}
inline uint16_t MysqlReply::Auth::status() const {
    return _status;
}
inline uint16_t MysqlReply::Auth::extended_capability() const {
    return _extended_capability;
}
inline uint8_t MysqlReply::Auth::auth_plugin_length() const {
    return _auth_plugin_length;
}
inline butil::StringPiece MysqlReply::Auth::salt2() const {
    return _salt2;
}
inline butil::StringPiece MysqlReply::Auth::auth_plugin() const {
    return _auth_plugin;
}
inline bool MysqlReply::Auth::is_parsed() const {
    return _is_parsed;
}
inline void MysqlReply::Auth::set_parsed() {
    _is_parsed = true;
}
// mysql reply ok
inline uint64_t MysqlReply::Ok::affect_row() const {
    return _affect_row;
}
inline uint64_t MysqlReply::Ok::index() const {
    return _index;
}
inline uint16_t MysqlReply::Ok::status() const {
    return _status;
}
inline uint16_t MysqlReply::Ok::warning() const {
    return _warning;
}
inline butil::StringPiece MysqlReply::Ok::msg() const {
    return _msg;
}
inline bool MysqlReply::Ok::is_parsed() const {
    return _is_parsed;
}
inline void MysqlReply::Ok::set_parsed() {
    _is_parsed = true;
}
// mysql reply error
inline uint16_t MysqlReply::Error::errcode() const {
    return _errcode;
}
inline butil::StringPiece MysqlReply::Error::status() const {
    return _status;
}
inline butil::StringPiece MysqlReply::Error::msg() const {
    return _msg;
}
inline bool MysqlReply::Error::is_parsed() const {
    return _is_parsed;
}
inline void MysqlReply::Error::set_parsed() {
    _is_parsed = true;
}
// mysql reply eof
inline uint16_t MysqlReply::Eof::warning() const {
    return _warning;
}
inline uint16_t MysqlReply::Eof::status() const {
    return _status;
}
inline bool MysqlReply::Eof::is_parsed() const {
    return _is_parsed;
}
inline void MysqlReply::Eof::set_parsed() {
    _is_parsed = true;
}
// mysql reply column
inline butil::StringPiece MysqlReply::Column::catalog() const {
    return _catalog;
}
inline butil::StringPiece MysqlReply::Column::database() const {
    return _database;
}
inline butil::StringPiece MysqlReply::Column::table() const {
    return _table;
}
inline butil::StringPiece MysqlReply::Column::origin_table() const {
    return _origin_table;
}
inline butil::StringPiece MysqlReply::Column::name() const {
    return _name;
}
inline butil::StringPiece MysqlReply::Column::origin_name() const {
    return _origin_name;
}
inline MysqlCollation MysqlReply::Column::collation() const {
    return _collation;
}
inline uint32_t MysqlReply::Column::length() const {
    return _length;
}
inline MysqlFieldType MysqlReply::Column::type() const {
    return _type;
}
inline MysqlFieldFlag MysqlReply::Column::flag() const {
    return _flag;
}
inline uint8_t MysqlReply::Column::decimal() const {
    return _decimal;
}
inline bool MysqlReply::Column::is_parsed() const {
    return _is_parsed;
}
inline void MysqlReply::Column::set_parsed() {
    _is_parsed = true;
}
// mysql reply row
inline uint64_t MysqlReply::Row::field_number() const {
    return _field_number;
}
inline const MysqlReply::Field* MysqlReply::Row::field(const uint64_t index) const {
    if (index > _field_number) {
        LOG(ERROR) << "wrong index, must between [0, " << _field_number << ")";
        return NULL;
    }
    return _fields + index;
}
inline bool MysqlReply::Row::is_parsed() const {
    return _is_parsed;
}
inline void MysqlReply::Row::set_parsed() {
    _is_parsed = true;
}
// mysql reply field
inline int8_t MysqlReply::Field::stiny() const {
    if (is_stiny()) {
        return _data.stiny;
    }
    CHECK(false) << "The reply is " << MysqlFieldTypeToString(_type) << ", not an stiny";
    return 0;
}
inline uint8_t MysqlReply::Field::tiny() const {
    if (is_tiny()) {
        return _data.tiny;
    }
    CHECK(false) << "The reply is " << MysqlFieldTypeToString(_type) << ", not an tiny";
    return 0;
}
inline int16_t MysqlReply::Field::ssmall() const {
    if (is_ssmall()) {
        return _data.ssmall;
    }
    CHECK(false) << "The reply is " << MysqlFieldTypeToString(_type) << ", not an ssmall";
    return 0;
}
inline uint16_t MysqlReply::Field::small() const {
    if (is_small()) {
        return _data.small;
    }
    CHECK(false) << "The reply is " << MysqlFieldTypeToString(_type) << ", not an small";
    return 0;
}
inline int32_t MysqlReply::Field::sinteger() const {
    if (is_sinteger()) {
        return _data.sinteger;
    }
    CHECK(false) << "The reply is " << MysqlFieldTypeToString(_type) << ", not an sinteger";
    return 0;
}
inline uint32_t MysqlReply::Field::integer() const {
    if (is_integer()) {
        return _data.integer;
    }
    CHECK(false) << "The reply is " << MysqlFieldTypeToString(_type) << ", not an integer";
    return 0;
}
inline int64_t MysqlReply::Field::sbigint() const {
    if (is_sbigint()) {
        return _data.sbigint;
    }
    CHECK(false) << "The reply is " << MysqlFieldTypeToString(_type) << ", not an sbigint";
    return 0;
}
inline uint64_t MysqlReply::Field::bigint() const {
    if (is_bigint()) {
        return _data.bigint;
    }
    CHECK(false) << "The reply is " << MysqlFieldTypeToString(_type) << ", not an bigint";
    return 0;
}
inline float MysqlReply::Field::float32() const {
    if (is_float32()) {
        return _data.float32;
    }
    CHECK(false) << "The reply is " << MysqlFieldTypeToString(_type) << ", not an float32";
    return 0;
}
inline double MysqlReply::Field::float64() const {
    if (is_float64()) {
        return _data.float64;
    }
    CHECK(false) << "The reply is " << MysqlFieldTypeToString(_type) << ", not an float64";
    return 0;
}
inline butil::StringPiece MysqlReply::Field::string() const {
    if (is_string()) {
        return _data.str;
    }
    CHECK(false) << "The reply is " << MysqlFieldTypeToString(_type) << ", not an string";
    return butil::StringPiece();
}
inline bool MysqlReply::Field::is_stiny() const {
    return _type == FIELD_TYPE_TINY && !_is_unsigned;
}
inline bool MysqlReply::Field::is_tiny() const {
    return _type == FIELD_TYPE_TINY && _is_unsigned;
}
inline bool MysqlReply::Field::is_ssmall() const {
    return (_type == FIELD_TYPE_SHORT || _type == FIELD_TYPE_YEAR) && !_is_unsigned;
}
inline bool MysqlReply::Field::is_small() const {
    return (_type == FIELD_TYPE_SHORT || _type == FIELD_TYPE_YEAR) && _is_unsigned;
}
inline bool MysqlReply::Field::is_sinteger() const {
    return (_type == FIELD_TYPE_INT24 || _type == FIELD_TYPE_LONG) && !_is_unsigned;
}
inline bool MysqlReply::Field::is_integer() const {
    return (_type == FIELD_TYPE_INT24 || _type == FIELD_TYPE_LONG) && _is_unsigned;
}
inline bool MysqlReply::Field::is_sbigint() const {
    return _type == FIELD_TYPE_LONGLONG && !_is_unsigned;
}
inline bool MysqlReply::Field::is_bigint() const {
    return _type == FIELD_TYPE_LONGLONG && _is_unsigned;
}
inline bool MysqlReply::Field::is_float32() const {
    return _type == FIELD_TYPE_FLOAT;
}
inline bool MysqlReply::Field::is_float64() const {
    return _type == FIELD_TYPE_DOUBLE;
}
inline bool MysqlReply::Field::is_string() const {
    return _type == FIELD_TYPE_DECIMAL || _type == FIELD_TYPE_NEWDECIMAL ||
        _type == FIELD_TYPE_VARCHAR || _type == FIELD_TYPE_BIT || _type == FIELD_TYPE_ENUM ||
        _type == FIELD_TYPE_SET || _type == FIELD_TYPE_TINY_BLOB ||
        _type == FIELD_TYPE_MEDIUM_BLOB || _type == FIELD_TYPE_LONG_BLOB ||
        _type == FIELD_TYPE_BLOB || _type == FIELD_TYPE_VAR_STRING || _type == FIELD_TYPE_STRING ||
        _type == FIELD_TYPE_GEOMETRY || _type == FIELD_TYPE_JSON || _type == FIELD_TYPE_TIME ||
        _type == FIELD_TYPE_DATE || _type == FIELD_TYPE_NEWDATE || _type == FIELD_TYPE_TIMESTAMP ||
        _type == FIELD_TYPE_DATETIME;
}
inline bool MysqlReply::Field::is_null() const {
    return _is_null;
}
inline bool MysqlReply::Field::is_parsed() const {
    return _is_parsed;
}
inline void MysqlReply::Field::set_parsed() {
    _is_parsed = true;
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
