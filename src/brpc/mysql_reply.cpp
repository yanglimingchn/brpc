#include "brpc/mysql_reply.h"

#include <ios>
#include "butil/logging.h"  // LOG()
namespace brpc {

#define MY_ERROR_RET(expr)                                              \
    do {                                                                \
        if ((expr) == true) {                                           \
            LOG(FATAL) << "Fail to arena allocate in file:" << __LINE__ \
                       << "function:" << __FUNCTION__;                  \
            return false;                                               \
        }                                                               \
    } while (0)
bool ParseHeader(butil::IOBuf& buf, MysqlHeader* value);
bool ParseEncodeLength(butil::IOBuf& buf, uint64_t* value);
bool ParseResultHeader(butil::IOBuf& buf, MysqlReply::ResultSetHeader* value);
bool ParseField(butil::IOBuf& buf, MysqlReply::Field* value, butil::Arena* arena);
bool ParseOk(butil::IOBuf& buf, MysqlReply::Ok* value, butil::Arena* arena);
bool IsEof(const butil::IOBuf& buf, MysqlReply::Eof* value);
bool ParseEof(butil::IOBuf& buf, MysqlReply::Eof* value);
bool ParseError(butil::IOBuf& buf, MysqlReply::Error* value, butil::Arena* arena);
bool ParseRowData(butil::IOBuf& buf,
                  butil::StringPiece* value,
                  const uint64_t field_number,
                  butil::Arena* arena);

bool MysqlReply::ConsumePartialIOBuf(butil::IOBuf& buf, butil::Arena* arena) {
    uint8_t header[5];
    const uint8_t* p = (const uint8_t*)buf.fetch(header, sizeof(header));
    uint8_t type = p[4];
    if (type == 0x00) {
        _type = RSP_OK;
        _data.ok = (Ok*)arena->allocate(sizeof(Ok));
        MY_ERROR_RET(_data.ok == NULL);
        ParseOk(buf, _data.ok, arena);
    } else if (type == 0xFF) {
        _type = RSP_ERROR;
        _data.error = (Error*)arena->allocate(sizeof(Error));
        MY_ERROR_RET(_data.error == NULL);
        ParseError(buf, _data.error, arena);
    } else if (type == 0xFE) {
        _type = RSP_EOF;
        _data.eof = (Eof*)arena->allocate(sizeof(Eof));
        MY_ERROR_RET(_data.eof == NULL);
        ParseEof(buf, _data.eof);
    } else if (type >= 0x01 && type <= 0xFA) {
        _type = RSP_RESULTSET;
        _data.result_set = (ResultSet*)arena->allocate(sizeof(ResultSet));
        MY_ERROR_RET(_data.result_set == NULL);
        ResultSet& r = *_data.result_set;
        ParseResultHeader(buf, &r.header);
        // parse fields
        Field* fields;
        fields = (Field*)arena->allocate(sizeof(Field) * r.header.field_number);
        MY_ERROR_RET(fields == NULL);
        for (uint64_t i = 0; i < r.header.field_number; ++i) {
            ParseField(buf, fields + i, arena);
        }
        r.fields = fields;
        ParseEof(buf, &r.eof1);

        // parse row
        Eof eof;
        std::vector<RowData*> rows;
        while (!IsEof(buf, &eof)) {
            RowData* row = (RowData*)arena->allocate(sizeof(RowData));
            MY_ERROR_RET(row == NULL);
            row->fields = (butil::StringPiece*)arena->allocate(sizeof(butil::StringPiece) *
                                                               r.header.field_number);
            ParseRowData(buf, row->fields, r.header.field_number, arena);
            rows.push_back(row);
        }
        r.rows = (RowData**)arena->allocate(sizeof(RowData*) * rows.size());
        MY_ERROR_RET(r.rows == NULL);
        for (size_t i = 0; i < rows.size(); ++i) {
            *(r.rows + i) = rows[i];
        }
        r.row_number = rows.size();
        ParseEof(buf, &r.eof2);
    } else {
    }
    return true;
}

void MysqlReply::Print(std::ostream& os) const {
    if (_type == RSP_OK) {
        const Ok& ok = *_data.ok;
        os << "\naffect_row:" << ok.affect_row << "\nindex:" << ok.index << "\nstatus:" << ok.status
           << "\nwarning:" << ok.warning << "\nmsg:" << ok.msg.as_string();
    } else if (_type == RSP_ERROR) {
        const Error& err = *_data.error;
        os << "\nerrcode:" << err.errcode << "\nstatus:" << err.status.as_string()
           << "\nmsg:" << err.msg.as_string();
    } else if (_type == RSP_RESULTSET) {
        const ResultSet& r = *_data.result_set;
        os << "\nheader.field_number:" << r.header.field_number;
        for (uint64_t i = 0; i < r.header.field_number; ++i) {
            os << "\nfield[" << i << "].catalog:" << r.fields[i].catalog.as_string() << "\nfield["
               << i << "].database:" << r.fields[i].database.as_string() << "\nfield[" << i
               << "].table:" << r.fields[i].table.as_string() << "\nfield[" << i
               << "].origin_table:" << r.fields[i].origin_table.as_string() << "\nfield[" << i
               << "].name:" << r.fields[i].name.as_string() << "\nfield[" << i
               << "].origin_name:" << r.fields[i].origin_name.as_string() << "\nfield[" << i
               << "].charset:" << r.fields[i].charset << "\nfield[" << i
               << "].length:" << r.fields[i].length << "\nfield[" << i
               << "].type:" << r.fields[i].type << "\nfield[" << i
               << "].flag:" << (unsigned)r.fields[i].flag << "\nfield[" << i
               << "].decimal:" << (unsigned)r.fields[i].decimal;
        }
        os << "\neof1.warning:" << r.eof1.warning;
        os << "\neof1.status:" << r.eof1.status;
        for (uint64_t i = 0; i < r.row_number; ++i) {
            for (uint64_t j = 0; j < r.header.field_number; ++j) {
                os << "\nrow[" << i << "][" << j << "]:" << r.rows[i]->fields[j];
            }
        }
        os << "\neof2.warning:" << r.eof2.warning;
        os << "\neof2.status:" << r.eof2.status;
    } else if (_type == RSP_EOF) {
        const Eof& e = *_data.eof;
        os << "\nwarning:" << e.warning << "\nstatus:" << e.status;
    }
}

bool ParseHeader(butil::IOBuf& buf, MysqlHeader* value) {
    {
        uint8_t tmp[3];
        buf.cutn(tmp, sizeof(tmp));
        value->payload_size = mysql_uint3korr(tmp);
    }
    {
        uint8_t tmp;
        buf.cut1((char*)&tmp);
        value->seq = tmp;
    }
}

bool ParseEncodeLength(butil::IOBuf& buf, uint64_t* value) {
    uint8_t f;
    buf.cut1((char*)&f);
    if (f >= 0 && f <= 250) {
        *value = f;
    } else if (f == 251) {
        *value = 0;
    } else if (f == 252) {
        uint8_t tmp[2];
        buf.cutn(tmp, sizeof(tmp));
        *value = mysql_uint2korr(tmp);
    } else if (f == 253) {
        uint8_t tmp[3];
        buf.cutn(tmp, sizeof(tmp));
        *value = mysql_uint3korr(tmp);
    } else if (f == 254) {
        uint8_t tmp[8];
        buf.cutn(tmp, sizeof(tmp));
        *value = mysql_uint8korr(tmp);
    }
}

bool ParseResultHeader(butil::IOBuf& buf, MysqlReply::ResultSetHeader* value) {
    MysqlHeader header;
    uint64_t old_size, new_size;
    ParseHeader(buf, &header);
    ParseEncodeLength(buf, &value->field_number);
}

bool ParseField(butil::IOBuf& buf, MysqlReply::Field* value, butil::Arena* arena) {
    MysqlHeader header;
    ParseHeader(buf, &header);

    uint64_t len;
    ParseEncodeLength(buf, &len);
    char* catalog = (char*)arena->allocate(sizeof(char) * len);
    MY_ERROR_RET(catalog == NULL);
    buf.cutn(catalog, len);
    value->catalog.set(catalog, len);

    ParseEncodeLength(buf, &len);
    char* database = (char*)arena->allocate(sizeof(char) * len);
    MY_ERROR_RET(database == NULL);
    buf.cutn(database, len);
    value->database.set(database, len);

    ParseEncodeLength(buf, &len);
    char* table = (char*)arena->allocate(sizeof(char) * len);
    MY_ERROR_RET(table == NULL);
    buf.cutn(table, len);
    value->table.set(table, len);

    ParseEncodeLength(buf, &len);
    char* origin_table = (char*)arena->allocate(sizeof(char) * len);
    MY_ERROR_RET(origin_table == NULL);
    buf.cutn(origin_table, len);
    value->origin_table.set(origin_table, len);

    ParseEncodeLength(buf, &len);
    char* name = (char*)arena->allocate(sizeof(char) * len);
    MY_ERROR_RET(name == NULL);
    buf.cutn(name, len);
    value->name.set(name, len);

    ParseEncodeLength(buf, &len);
    char* origin_name = (char*)arena->allocate(sizeof(char) * len);
    MY_ERROR_RET(origin_name == NULL);
    buf.cutn(origin_name, len);
    value->origin_name.set(origin_name, len);
    buf.pop_front(1);
    {
        uint8_t tmp[2];
        buf.cutn(tmp, sizeof(tmp));
        value->charset = mysql_uint2korr(tmp);
    }
    {
        uint8_t tmp[4];
        buf.cutn(tmp, sizeof(tmp));
        value->length = mysql_uint4korr(tmp);
    }
    buf.cut1((char*)&value->type);
    {
        uint8_t tmp[2];
        buf.cutn(tmp, sizeof(tmp));
        value->flag = (MysqlReply::FieldFlag)mysql_uint2korr(tmp);
    }
    buf.cut1((char*)&value->decimal);
    buf.pop_front(2);
}

bool ParseOk(butil::IOBuf& buf, MysqlReply::Ok* value, butil::Arena* arena) {
    MysqlHeader header;
    ParseHeader(buf, &header);
    buf.pop_front(1);
    uint64_t len, old_size, new_size;

    old_size = buf.size();
    ParseEncodeLength(buf, &len);
    value->affect_row = 0;
    buf.cutn(&value->affect_row, len);
    value->affect_row = mysql_uint8korr((uint8_t*)&value->affect_row);

    ParseEncodeLength(buf, &len);
    value->index = 0;
    buf.cutn(&value->index, len);
    value->index = mysql_uint8korr((uint8_t*)&value->index);

    buf.cutn(&value->status, 2);
    buf.cutn(&value->warning, 2);

    new_size = buf.size();
    if (old_size - new_size < header.payload_size) {
        len = header.payload_size - (old_size - new_size);
        char* msg = (char*)arena->allocate(sizeof(char) * len);
        MY_ERROR_RET(msg == NULL);
        buf.cutn(msg, len);
        value->msg.set(msg, len);
    }
}

bool IsEof(const butil::IOBuf& buf, MysqlReply::Eof* value) {
    uint8_t tmp[5];
    const uint8_t* p = (const uint8_t*)buf.fetch(tmp, sizeof(tmp));
    uint8_t type = p[4];
    if (type == MysqlReply::RSP_EOF) {
        buf.copy_to(&value->warning, 2, 5);
        buf.copy_to(&value->status, 2, 7);
        return true;
    }
    return false;
}

bool ParseEof(butil::IOBuf& buf, MysqlReply::Eof* value) {
    MysqlHeader header;
    ParseHeader(buf, &header);
    buf.pop_front(1);
    buf.cutn(&value->warning, 2);
    buf.cutn(&value->status, 2);
}

bool ParseError(butil::IOBuf& buf, MysqlReply::Error* value, butil::Arena* arena) {
    MysqlHeader header;
    ParseHeader(buf, &header);
    buf.pop_front(1);
    {
        uint8_t tmp[2];
        buf.cutn(tmp, sizeof(tmp));
        value->errcode = mysql_uint2korr(tmp);
    }
    buf.pop_front(1);

    char* status = (char*)arena->allocate(sizeof(char) * 5);
    MY_ERROR_RET(status == NULL);
    buf.cutn(status, 5);
    value->status.set(status, 5);

    uint64_t len = header.payload_size - 8;
    char* msg = (char*)arena->allocate(sizeof(char) * len);
    MY_ERROR_RET(msg == NULL);
    buf.cutn(msg, len);
    value->msg.set(msg, len);
}

bool ParseRowData(butil::IOBuf& buf,
                  butil::StringPiece* value,
                  const uint64_t field_number,
                  butil::Arena* arena) {
    MysqlHeader header;
    ParseHeader(buf, &header);

    uint64_t len;
    for (uint64_t i = 0; i < field_number; ++i) {
        ParseEncodeLength(buf, &len);
        char* field = (char*)arena->allocate(sizeof(char) * len);
        MY_ERROR_RET(field == NULL);
        buf.cutn(field, len);
        value[i].set(field, len);
    }
}

}  // namespace brpc
