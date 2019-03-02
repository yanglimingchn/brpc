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

const std::string null_str = "NULL";
bool ParseHeader(butil::IOBuf& buf, MysqlHeader* value);
bool ParseEncodeLength(butil::IOBuf& buf, uint64_t* value);
bool ParseResultHeader(butil::IOBuf& buf, MysqlReply::ResultSetHeader* value);
bool ParseColumn(butil::IOBuf& buf, MysqlReply::Column* value, butil::Arena* arena);
bool ParseOk(butil::IOBuf& buf, MysqlReply::Ok* value, butil::Arena* arena);
bool IsEof(const butil::IOBuf& buf, MysqlReply::Eof* value);
bool ParseEof(butil::IOBuf& buf, MysqlReply::Eof* value);
bool ParseError(butil::IOBuf& buf, MysqlReply::Error* value, butil::Arena* arena);
bool ParseTextRow(butil::IOBuf& buf,
                  MysqlReply::Field* value,
                  const MysqlReply::Column* columns,
                  const uint64_t column_number,
                  butil::Arena* arena);

bool MysqlReply::ConsumePartialIOBuf(butil::IOBuf& buf, butil::Arena* arena) {
    uint8_t header[5];
    const uint8_t* p = (const uint8_t*)buf.fetch(header, sizeof(header));
    uint8_t type = p[4];
    if (type == 0x00) {
        _type = RSP_OK;
        Ok* ok = (Ok*)arena->allocate(sizeof(Ok));
        MY_ERROR_RET(ok == NULL);
        ParseOk(buf, ok, arena);
        _data.ok = ok;
    } else if (type == 0xFF) {
        _type = RSP_ERROR;
        Error* error = (Error*)arena->allocate(sizeof(Error));
        MY_ERROR_RET(error == NULL);
        ParseError(buf, error, arena);
        _data.error = error;
    } else if (type == 0xFE) {
        _type = RSP_EOF;
        Eof* eof = (Eof*)arena->allocate(sizeof(Eof));
        MY_ERROR_RET(eof == NULL);
        ParseEof(buf, eof);
        _data.eof = eof;
    } else if (type >= 0x01 && type <= 0xFA) {
        _type = RSP_RESULTSET;
        ResultSet* result_set = (ResultSet*)arena->allocate(sizeof(ResultSet));
        MY_ERROR_RET(result_set == NULL);
        ResultSet& r = *result_set;
        ParseResultHeader(buf, &r.header);
        // parse colunms
        Column* columns = (Column*)arena->allocate(sizeof(Column) * r.header.column_number);
        MY_ERROR_RET(columns == NULL);
        for (uint64_t i = 0; i < r.header.column_number; ++i) {
            ParseColumn(buf, columns + i, arena);
        }
        r.columns = columns;
        ParseEof(buf, &r.eof1);

        // parse row
        Eof eof;
        std::vector<Row*> rows;
        while (!IsEof(buf, &eof)) {
            Row* row = (Row*)arena->allocate(sizeof(Row));
            MY_ERROR_RET(row == NULL);
            Field* fields = (Field*)arena->allocate(sizeof(Field) * r.header.column_number);
            ParseTextRow(buf, fields, columns, r.header.column_number, arena);
            row->fields = fields;
            rows.push_back(row);
        }
        Row** rpp = (Row**)arena->allocate(sizeof(Row*) * rows.size());
        MY_ERROR_RET(rpp == NULL);
        for (size_t i = 0; i < rows.size(); ++i) {
            *(rpp + i) = rows[i];
        }
        r.rows = rpp;
        r.row_number = rows.size();
        ParseEof(buf, &r.eof2);
        _data.result_set = result_set;
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
        os << "\nheader.column_number:" << r.header.column_number;
        for (uint64_t i = 0; i < r.header.column_number; ++i) {
            os << "\ncolumn[" << i << "].catalog:" << r.columns[i].catalog.as_string()
               << "\ncolumn[" << i << "].database:" << r.columns[i].database.as_string()
               << "\ncolumn[" << i << "].table:" << r.columns[i].table.as_string() << "\ncolumn["
               << i << "].origin_table:" << r.columns[i].origin_table.as_string() << "\ncolumn["
               << i << "].name:" << r.columns[i].name.as_string() << "\ncolumn[" << i
               << "].origin_name:" << r.columns[i].origin_name.as_string() << "\ncolumn[" << i
               << "].charset:" << r.columns[i].charset << "\ncolumn[" << i
               << "].length:" << r.columns[i].length << "\ncolumn[" << i
               << "].type:" << (unsigned)r.columns[i].type << "\ncolumn[" << i
               << "].flag:" << (unsigned)r.columns[i].flag << "\ncolumn[" << i
               << "].decimal:" << (unsigned)r.columns[i].decimal;
        }
        os << "\neof1.warning:" << r.eof1.warning;
        os << "\neof1.status:" << r.eof1.status;
        for (uint64_t i = 0; i < r.row_number; ++i) {
          for (uint64_t j = 0; j < r.header.column_number; ++j) {
            os << "\nrow[" << i << "][" << j << "]:";
            switch(r.columns[j].type) {
              case FIELD_TYPE_TINY:
                if (r.columns[j].flag & MysqlReply::UNSIGNED_FLAG) {os << r.rows[i]->fields[j].tiny();} else {
                  os << r.rows[i]->fields[j].stiny();
                }
                break;
              case FIELD_TYPE_SHORT:
              case FIELD_TYPE_YEAR:
                if (r.columns[j].flag & MysqlReply::UNSIGNED_FLAG) {os << r.rows[i]->fields[j].small();} else {
                  os << r.rows[i]->fields[j].ssmall();
                }
                break;
              case FIELD_TYPE_INT24:
              case FIELD_TYPE_LONG:
                if (r.columns[j].flag & MysqlReply::UNSIGNED_FLAG) {os << r.rows[i]->fields[j].integer();} else {
                  os << r.rows[i]->fields[j].sinteger();
                }
                break;
              case FIELD_TYPE_LONGLONG:
                if (r.columns[j].flag & MysqlReply::UNSIGNED_FLAG) {os << r.rows[i]->fields[j].bigint();} else {
                  os << r.rows[i]->fields[j].sbigint();
                }
                break;
              case FIELD_TYPE_FLOAT:
                  os << r.rows[i]->fields[j].float32();
                break;
              case FIELD_TYPE_DOUBLE:
                os << r.rows[i]->fields[j].float64();
                break;
              case FIELD_TYPE_DECIMAL:
              case FIELD_TYPE_NEWDECIMAL:
              case FIELD_TYPE_VARCHAR:
              case FIELD_TYPE_BIT:
              case FIELD_TYPE_ENUM:
              case FIELD_TYPE_SET:
              case FIELD_TYPE_TINY_BLOB:
              case FIELD_TYPE_MEDIUM_BLOB:
              case FIELD_TYPE_LONG_BLOB:
              case FIELD_TYPE_BLOB:
              case FIELD_TYPE_VAR_STRING:
              case FIELD_TYPE_STRING:
              case FIELD_TYPE_GEOMETRY:
              case FIELD_TYPE_JSON:
              case FIELD_TYPE_TIME:
              case FIELD_TYPE_DATE:
              case FIELD_TYPE_NEWDATE:
              case FIELD_TYPE_TIMESTAMP:
              case FIELD_TYPE_DATETIME:
                // LOG(INFO) << "sizeof field:" << sizeof(r.rows[i]->fields[j]);
                os << r.rows[i]->fields[j].data();
                break;
              default:
                os << "Unknown field type";
            }
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
    old_size = buf.size();
    ParseEncodeLength(buf, &value->column_number);
    new_size = buf.size();
    if (old_size - new_size < header.payload_size) {
        ParseEncodeLength(buf, &value->extra_msg);
    } else {
        value->extra_msg = 0;
    }
}

bool ParseColumn(butil::IOBuf& buf, MysqlReply::Column* value, butil::Arena* arena) {
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
    LOG(INFO) << "value->type:" << value->type;
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
    buf.pop_front(1);  // 0xFF
    {
        uint8_t tmp[2];
        buf.cutn(tmp, sizeof(tmp));
        value->errcode = mysql_uint2korr(tmp);
    }
    buf.pop_front(1);  // '#'
    // 5 byte server status
    char* status = (char*)arena->allocate(sizeof(char) * 5);
    MY_ERROR_RET(status == NULL);
    buf.cutn(status, 5);
    value->status.set(status, 5);
    // error message, Null-Terminated string
    uint64_t len = header.payload_size - 8;
    char* msg = (char*)arena->allocate(sizeof(char) * len);
    MY_ERROR_RET(msg == NULL);
    buf.cutn(msg, len - 1);
    value->msg.set(msg, len - 1);
    buf.pop_front(1);  // Null
}

bool ParseTextRow(butil::IOBuf& buf,
                  MysqlReply::Field* value,
                  const MysqlReply::Column* columns,
                  const uint64_t column_number,
                  butil::Arena* arena) {
    MysqlHeader header;
    ParseHeader(buf, &header);

    uint64_t len;
    for (uint64_t i = 0; i < column_number; ++i) {
        ParseEncodeLength(buf, &len);
        // if field is null
        value[i]._is_null = len > 0 ? false : true;
        if (value[i]._is_null) {
            return true;
        }
        // if field is not null
        butil::IOBuf str;
        buf.cutn(&str, len);
        LOG(INFO) << "column.len=" << len;
        const MysqlReply::Column& column = columns[i];
        LOG(INFO) << "column.type=" << (unsigned) column.type;
        switch (column.type) {
            case FIELD_TYPE_TINY:
                if (column.flag & MysqlReply::UNSIGNED_FLAG) {
                    std::istringstream(str.to_string()) >> value[i]._data.tiny;
                } else {
                    std::istringstream(str.to_string()) >> value[i]._data.stiny;
                }
                break;
            case FIELD_TYPE_SHORT:
            case FIELD_TYPE_YEAR:
                if (column.flag & MysqlReply::UNSIGNED_FLAG) {
                    std::istringstream(str.to_string()) >> value[i]._data.small;
                } else {
                    std::istringstream(str.to_string()) >> value[i]._data.ssmall;
                }
                break;
            case FIELD_TYPE_INT24:
            case FIELD_TYPE_LONG:
                if (column.flag & MysqlReply::UNSIGNED_FLAG) {
                    std::istringstream(str.to_string()) >> value[i]._data.integer;
                } else {
                    std::istringstream(str.to_string()) >> value[i]._data.sinteger;
                }
                break;
            case FIELD_TYPE_LONGLONG:
                if (column.flag & MysqlReply::UNSIGNED_FLAG) {
                    std::istringstream(str.to_string()) >> value[i]._data.bigint;
                } else {
                    std::istringstream(str.to_string()) >> value[i]._data.sbigint;
                }
                break;
            case FIELD_TYPE_FLOAT:
                std::istringstream(str.to_string()) >> value[i]._data.float32;
                break;
            case FIELD_TYPE_DOUBLE:
                std::istringstream(str.to_string()) >> value[i]._data.float64;
                break;
            case FIELD_TYPE_DECIMAL:
            case FIELD_TYPE_NEWDECIMAL:
            case FIELD_TYPE_VARCHAR:
            case FIELD_TYPE_BIT:
            case FIELD_TYPE_ENUM:
            case FIELD_TYPE_SET:
            case FIELD_TYPE_TINY_BLOB:
            case FIELD_TYPE_MEDIUM_BLOB:
            case FIELD_TYPE_LONG_BLOB:
            case FIELD_TYPE_BLOB:
            case FIELD_TYPE_VAR_STRING:
            case FIELD_TYPE_STRING:
            case FIELD_TYPE_GEOMETRY:
            case FIELD_TYPE_JSON:
            case FIELD_TYPE_TIME:
            case FIELD_TYPE_DATE:
            case FIELD_TYPE_NEWDATE:
            case FIELD_TYPE_TIMESTAMP:
          case FIELD_TYPE_DATETIME: {
                char* d = (char*)arena->allocate(sizeof(char) * len);
                MY_ERROR_RET(d == NULL);
                str.copy_to(d);
                value[i]._data.str = d;
                value[i]._len = len;
                LOG(INFO) << "len=" << len << " row=" << value[i]._data.str;
            }
              break;
          default:
            LOG(ERROR) << "Unknown field type";
        }
    }
    return true;
}

}  // namespace brpc
