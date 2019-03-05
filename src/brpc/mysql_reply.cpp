#include "brpc/mysql_reply.h"
#include <ios>

namespace brpc {

#define MY_ERROR_RET(expr, message)                                                       \
    do {                                                                                  \
        if ((expr) == true) {                                                             \
            LOG(FATAL) << message " in file:" << __LINE__ << "function:" << __FUNCTION__; \
            return false;                                                                 \
        }                                                                                 \
    } while (0)

#define MY_ALLOC_CHECK(expr) MY_ERROR_RET(((expr) == NULL), "Fail to arena allocate")
#define MY_PARSE_CHECK(expr) MY_ERROR_RET(!(expr), "Fail to parse mysql protocol")

inline void* alloc_and_init(butil::Arena* arena, size_t n) {
    void* p = arena->allocate(n);
    if (p == NULL)
        return NULL;
    memset(p, 0, n);
    return p;
}

const char* MysqlFieldTypeToString(MysqlFieldType type) {
    switch (type) {
        case FIELD_TYPE_DECIMAL:
        case FIELD_TYPE_TINY:
            return "tiny";
        case FIELD_TYPE_SHORT:
            return "short";
        case FIELD_TYPE_LONG:
            return "long";
        case FIELD_TYPE_FLOAT:
            return "float";
        case FIELD_TYPE_DOUBLE:
            return "double";
        case FIELD_TYPE_NULL:
            return "null";
        case FIELD_TYPE_TIMESTAMP:
            return "timestamp";
        case FIELD_TYPE_LONGLONG:
            return "longlong";
        case FIELD_TYPE_INT24:
            return "int24";
        case FIELD_TYPE_DATE:
            return "date";
        case FIELD_TYPE_TIME:
            return "time";
        case FIELD_TYPE_DATETIME:
            return "datetime";
        case FIELD_TYPE_YEAR:
            return "year";
        case FIELD_TYPE_NEWDATE:
            return "new date";
        case FIELD_TYPE_VARCHAR:
            return "varchar";
        case FIELD_TYPE_BIT:
            return "bit";
        case FIELD_TYPE_JSON:
            return "json";
        case FIELD_TYPE_NEWDECIMAL:
            return "new decimal";
        case FIELD_TYPE_ENUM:
            return "enum";
        case FIELD_TYPE_SET:
            return "set";
        case FIELD_TYPE_TINY_BLOB:
            return "tiny blob";
        case FIELD_TYPE_MEDIUM_BLOB:
            return "blob";
        case FIELD_TYPE_LONG_BLOB:
            return "long blob";
        case FIELD_TYPE_BLOB:
            return "blob";
        case FIELD_TYPE_VAR_STRING:
            return "var string";
        case FIELD_TYPE_STRING:
            return "string";
        case FIELD_TYPE_GEOMETRY:
            return "geometry";
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
    return true;
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
    return true;
}

bool MysqlReply::ConsumePartialIOBuf(butil::IOBuf& buf, butil::Arena* arena) {
    uint8_t header[5];
    const uint8_t* p = (const uint8_t*)buf.fetch(header, sizeof(header));
    uint8_t type = p[4];
    if (type == 0x00) {
        _type = RSP_OK;
        Ok* ok = (Ok*)alloc_and_init(arena, sizeof(Ok));
        MY_ALLOC_CHECK(ok);
        MY_PARSE_CHECK(ok->parseOk(buf, arena));
        _data.ok = ok;
    } else if (type == 0xFF) {
        _type = RSP_ERROR;
        Error* error = (Error*)alloc_and_init(arena, sizeof(Error));
        MY_ALLOC_CHECK(error);
        MY_PARSE_CHECK(error->parseError(buf, arena));
        _data.error = error;
    } else if (type == 0xFE) {
        _type = RSP_EOF;
        Eof* eof = (Eof*)alloc_and_init(arena, sizeof(Eof));
        MY_ALLOC_CHECK(eof);
        MY_PARSE_CHECK(eof->parseEof(buf));
        _data.eof = eof;
    } else if (type >= 0x01 && type <= 0xFA) {
        _type = RSP_RESULTSET;
        ResultSet* result_set = (ResultSet*)alloc_and_init(arena, sizeof(ResultSet));
        MY_ALLOC_CHECK(result_set);
        ResultSet& r = *result_set;
        MY_PARSE_CHECK(r._header.parseResultHeader(buf));
        // parse colunms
        Column* columns = (Column*)alloc_and_init(arena, sizeof(Column) * r._header._column_number);
        MY_ALLOC_CHECK(columns);
        for (uint64_t i = 0; i < r._header._column_number; ++i) {
            MY_PARSE_CHECK(columns[i].parseColumn(buf, arena));
        }
        r._columns = columns;
        MY_PARSE_CHECK(r._eof1.parseEof(buf));
        // parse row
        Eof eof;
        std::vector<Row*> rows;
        while (!eof.isEof(buf)) {
            Row* row = (Row*)alloc_and_init(arena, sizeof(Row));
            MY_ALLOC_CHECK(row);
            Field* fields = (Field*)alloc_and_init(arena, sizeof(Field) * r._header._column_number);
            MY_ALLOC_CHECK(fields);
            MY_PARSE_CHECK(
                row->parseTextRow(buf, fields, columns, r._header._column_number, arena));
            row->_fields = fields;
            row->_field_number = r._header._column_number;
            rows.push_back(row);
        }
        Row** rpp = (Row**)alloc_and_init(arena, sizeof(Row*) * rows.size());
        MY_ALLOC_CHECK(rpp);
        for (size_t i = 0; i < rows.size(); ++i) {
            *(rpp + i) = rows[i];
        }
        r._rows = rpp;
        r._row_number = rows.size();
        MY_PARSE_CHECK(r._eof2.parseEof(buf));
        _data.result_set = result_set;
    } else {
        return false;
    }
    return true;
}

void MysqlReply::Print(std::ostream& os) const {
    if (_type == RSP_OK) {
        const Ok& ok = *_data.ok;
        os << "\naffect_row:" << ok._affect_row << "\nindex:" << ok._index
           << "\nstatus:" << ok._status << "\nwarning:" << ok._warning
           << "\nmessage:" << ok._msg.as_string();
    } else if (_type == RSP_ERROR) {
        const Error& err = *_data.error;
        os << "\nerrcode:" << err._errcode << "\nstatus:" << err._status.as_string()
           << "\nmessage:" << err._msg.as_string();
    } else if (_type == RSP_RESULTSET) {
        const ResultSet& r = *_data.result_set;
        os << "\nheader.column_number:" << r._header._column_number;
        for (uint64_t i = 0; i < r._header._column_number; ++i) {
            os << "\ncolumn[" << i << "].catalog:" << r._columns[i]._catalog.as_string()
               << "\ncolumn[" << i << "].database:" << r._columns[i]._database.as_string()
               << "\ncolumn[" << i << "].table:" << r._columns[i]._table.as_string() << "\ncolumn["
               << i << "].origin_table:" << r._columns[i]._origin_table.as_string() << "\ncolumn["
               << i << "].name:" << r._columns[i]._name.as_string() << "\ncolumn[" << i
               << "].origin_name:" << r._columns[i]._origin_name.as_string() << "\ncolumn[" << i
               << "].charset:" << r._columns[i]._charset << "\ncolumn[" << i
               << "].length:" << r._columns[i]._length << "\ncolumn[" << i
               << "].type:" << (unsigned)r._columns[i]._type << "\ncolumn[" << i
               << "].flag:" << (unsigned)r._columns[i]._flag << "\ncolumn[" << i
               << "].decimal:" << (unsigned)r._columns[i]._decimal;
        }
        os << "\neof1.warning:" << r._eof1._warning;
        os << "\neof1.status:" << r._eof1._status;
        for (uint64_t i = 0; i < r._row_number; ++i) {
            for (uint64_t j = 0; j < r._header._column_number; ++j) {
                os << "\nrow[" << i << "][" << j << "]:";
                switch (r._columns[j]._type) {
                    case FIELD_TYPE_TINY:
                        if (r._columns[j]._flag & UNSIGNED_FLAG) {
                            os << r._rows[i]->field(j)->tiny();
                        } else {
                            os << r._rows[i]->field(j)->stiny();
                        }
                        break;
                    case FIELD_TYPE_SHORT:
                    case FIELD_TYPE_YEAR:
                        if (r._columns[j]._flag & UNSIGNED_FLAG) {
                            os << r._rows[i]->field(j)->small();
                        } else {
                            os << r._rows[i]->field(j)->ssmall();
                        }
                        break;
                    case FIELD_TYPE_INT24:
                    case FIELD_TYPE_LONG:
                        if (r._columns[j]._flag & UNSIGNED_FLAG) {
                            os << r._rows[i]->field(j)->integer();
                        } else {
                            os << r._rows[i]->field(j)->sinteger();
                        }
                        break;
                    case FIELD_TYPE_LONGLONG:
                        if (r._columns[j]._flag & UNSIGNED_FLAG) {
                            os << r._rows[i]->field(j)->bigint();
                        } else {
                            os << r._rows[i]->field(j)->sbigint();
                        }
                        break;
                    case FIELD_TYPE_FLOAT:
                        os << r._rows[i]->field(j)->float32();
                        break;
                    case FIELD_TYPE_DOUBLE:
                        os << r._rows[i]->field(j)->float64();
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
                        os << r._rows[i]->field(j)->string();
                        break;
                    default:
                        os << "Unknown field type";
                }
            }
        }
        os << "\neof2.warning:" << r._eof2._warning;
        os << "\neof2.status:" << r._eof2._status;
    } else if (_type == RSP_EOF) {
        const Eof& e = *_data.eof;
        os << "\nwarning:" << e._warning << "\nstatus:" << e._status;
    }
}

bool MysqlReply::ResultSetHeader::parseResultHeader(butil::IOBuf& buf) {
    MysqlHeader header;
    uint64_t old_size, new_size;
    ParseHeader(buf, &header);
    old_size = buf.size();
    ParseEncodeLength(buf, &_column_number);
    new_size = buf.size();
    if (old_size - new_size < header.payload_size) {
        ParseEncodeLength(buf, &_extra_msg);
    } else {
        _extra_msg = 0;
    }
    return true;
}

bool MysqlReply::Column::parseColumn(butil::IOBuf& buf, butil::Arena* arena) {
    MysqlHeader header;
    ParseHeader(buf, &header);

    uint64_t len;
    ParseEncodeLength(buf, &len);
    char* catalog = (char*)alloc_and_init(arena, sizeof(char) * len);
    MY_ALLOC_CHECK(catalog);
    buf.cutn(catalog, len);
    _catalog.set(catalog, len);

    ParseEncodeLength(buf, &len);
    char* database = (char*)alloc_and_init(arena, sizeof(char) * len);
    MY_ALLOC_CHECK(database);
    buf.cutn(database, len);
    _database.set(database, len);

    ParseEncodeLength(buf, &len);
    char* table = (char*)alloc_and_init(arena, sizeof(char) * len);
    MY_ALLOC_CHECK(table);
    buf.cutn(table, len);
    _table.set(table, len);

    ParseEncodeLength(buf, &len);
    char* origin_table = (char*)alloc_and_init(arena, sizeof(char) * len);
    MY_ALLOC_CHECK(origin_table);
    buf.cutn(origin_table, len);
    _origin_table.set(origin_table, len);

    ParseEncodeLength(buf, &len);
    char* name = (char*)alloc_and_init(arena, sizeof(char) * len);
    MY_ALLOC_CHECK(name);
    buf.cutn(name, len);
    _name.set(name, len);

    ParseEncodeLength(buf, &len);
    char* origin_name = (char*)alloc_and_init(arena, sizeof(char) * len);
    MY_ALLOC_CHECK(origin_name);
    buf.cutn(origin_name, len);
    _origin_name.set(origin_name, len);
    buf.pop_front(1);
    {
        uint8_t tmp[2];
        buf.cutn(tmp, sizeof(tmp));
        _charset = mysql_uint2korr(tmp);
    }
    {
        uint8_t tmp[4];
        buf.cutn(tmp, sizeof(tmp));
        _length = mysql_uint4korr(tmp);
    }
    buf.cut1((char*)&_type);
    {
        uint8_t tmp[2];
        buf.cutn(tmp, sizeof(tmp));
        _flag = (MysqlFieldFlag)mysql_uint2korr(tmp);
    }
    buf.cut1((char*)&_decimal);
    buf.pop_front(2);
    return true;
}

bool MysqlReply::Ok::parseOk(butil::IOBuf& buf, butil::Arena* arena) {
    MysqlHeader header;
    ParseHeader(buf, &header);
    buf.pop_front(1);
    uint64_t len, old_size, new_size;

    old_size = buf.size();
    ParseEncodeLength(buf, &len);
    _affect_row = len;

    ParseEncodeLength(buf, &len);
    _index = len;

    buf.cutn(&_status, 2);
    buf.cutn(&_warning, 2);

    new_size = buf.size();
    if (old_size - new_size < header.payload_size) {
        len = header.payload_size - (old_size - new_size);
        char* msg = (char*)alloc_and_init(arena, sizeof(char) * len - 1);
        MY_ALLOC_CHECK(msg);
        buf.cutn(msg, len - 1);
        _msg.set(msg, len - 1);
        buf.pop_front(1);  // Null
    }
    return true;
}

bool MysqlReply::Eof::isEof(const butil::IOBuf& buf) {
    uint8_t tmp[5];
    const uint8_t* p = (const uint8_t*)buf.fetch(tmp, sizeof(tmp));
    uint8_t type = p[4];
    if (type == RSP_EOF) {
        buf.copy_to(&_warning, 2, 5);
        buf.copy_to(&_status, 2, 7);
        return true;
    }
    return false;
}

bool MysqlReply::Eof::parseEof(butil::IOBuf& buf) {
    MysqlHeader header;
    ParseHeader(buf, &header);
    buf.pop_front(1);
    buf.cutn(&_warning, 2);
    buf.cutn(&_status, 2);
    return true;
}

bool MysqlReply::Error::parseError(butil::IOBuf& buf, butil::Arena* arena) {
    MysqlHeader header;
    ParseHeader(buf, &header);
    buf.pop_front(1);  // 0xFF
    {
        uint8_t tmp[2];
        buf.cutn(tmp, sizeof(tmp));
        _errcode = mysql_uint2korr(tmp);
    }
    buf.pop_front(1);  // '#'
    // 5 byte server status
    char* status = (char*)alloc_and_init(arena, sizeof(char) * 5);
    MY_ALLOC_CHECK(status);
    buf.cutn(status, 5);
    _status.set(status, 5);
    // error message, Null-Terminated string
    uint64_t len = header.payload_size - 8;
    char* msg = (char*)alloc_and_init(arena, sizeof(char) * len - 1);
    MY_ALLOC_CHECK(msg);
    buf.cutn(msg, len - 1);
    _msg.set(msg, len - 1);
    buf.pop_front(1);  // Null
    return true;
}

bool MysqlReply::Row::parseTextRow(butil::IOBuf& buf,
                                   MysqlReply::Field* value,
                                   const MysqlReply::Column* column,
                                   const uint64_t column_number,
                                   butil::Arena* arena) {
    MysqlHeader header;
    ParseHeader(buf, &header);

    for (uint64_t i = 0; i < column_number; ++i) {
        if (value[i].parseField(buf, column + i, arena) == false) {
            LOG(ERROR) << "parse field error";
            return false;
        }
    }
    return true;
}

bool MysqlReply::Field::parseField(butil::IOBuf& buf,
                                   const MysqlReply::Column* column,
                                   butil::Arena* arena) {
    uint64_t len;
    ParseEncodeLength(buf, &len);
    // field type
    _type = column->_type;
    // field flag
    _is_unsigned = column->_flag & UNSIGNED_FLAG;
    // field is null
    _is_null = len > 0 ? false : true;
    if (_is_null) {
        return true;
    }
    // field is not null
    butil::IOBuf str;
    buf.cutn(&str, len);
    switch (_type) {
        case FIELD_TYPE_TINY:
            if (column->_flag & UNSIGNED_FLAG) {
                std::istringstream(str.to_string()) >> _data.tiny;
            } else {
                std::istringstream(str.to_string()) >> _data.stiny;
            }
            break;
        case FIELD_TYPE_SHORT:
        case FIELD_TYPE_YEAR:
            if (column->_flag & UNSIGNED_FLAG) {
                std::istringstream(str.to_string()) >> _data.small;
            } else {
                std::istringstream(str.to_string()) >> _data.ssmall;
            }
            break;
        case FIELD_TYPE_INT24:
        case FIELD_TYPE_LONG:
            if (column->_flag & UNSIGNED_FLAG) {
                std::istringstream(str.to_string()) >> _data.integer;
            } else {
                std::istringstream(str.to_string()) >> _data.sinteger;
            }
            break;
        case FIELD_TYPE_LONGLONG:
            if (column->_flag & UNSIGNED_FLAG) {
                std::istringstream(str.to_string()) >> _data.bigint;
            } else {
                std::istringstream(str.to_string()) >> _data.sbigint;
            }
            break;
        case FIELD_TYPE_FLOAT:
            std::istringstream(str.to_string()) >> _data.float32;
            break;
        case FIELD_TYPE_DOUBLE:
            std::istringstream(str.to_string()) >> _data.float64;
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
            char* d = (char*)alloc_and_init(arena, sizeof(char) * len);
            MY_ALLOC_CHECK(d);
            str.copy_to(d);
            _data.str.set(d, len);
        } break;
        default:
            LOG(ERROR) << "Unknown field type";
            return false;
    }
    return true;
}

}  // namespace brpc
