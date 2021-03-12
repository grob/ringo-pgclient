const binary = require("binary");
const {Byte} = java.lang;
const {ByteArrayOutputStream} = java.io;

const BIGINT = exports.BIGINT = "bigint";
const BIGSERIAL = exports.BIGSERIAL = "bigserial";
const BIT = exports.BIT = "bit";
const BIT_VARYING = exports.BIT_VARYING = "bit varying";
const BOOL = exports.BOOL = "bool";
const BOOLEAN = exports.BOOLEAN = "boolean";
const BOX = exports.BOX = "box";
const BPCHAR = exports.BPCHAR = "bpchar"; // FIXME: getColumnTypeName returns "bpchar" for "char"
const BYTEA = exports.BYTEA = "bytea";
const CHAR = exports.CHAR = "char";
const CHARACTER = exports.CHARACTER = "character";
const CHARACTER_VARYING = exports.CHARACTER_VARYING = "character varying";
const CIDR = exports.CIDR = "cidr";
const CIRCLE = exports.CIRCLE = "circle";
const DATE = exports.DATE = "date";
const DECIMAL = exports.DECIMAL = "decimal";
const DOUBLE_PRECISION = exports.DOUBLE_PRECISION = "double precision";
const FLOAT4 = exports.FLOAT4 = "float4";
const FLOAT8 = exports.FLOAT8 = "float8";
const INET = exports.INET = "inet";
const INT = exports.INTEGER = "int";
const INT2 = exports.INT2 = "int2";
const INT4 = exports.INT4 = "int4";
const INT8 = exports.INT8 = "int8";
const INTEGER = exports.INTEGER = "integer";
const INTERVAL = exports.INTERVAL = "interval";
const JSON_TYPE = exports.JSON_TYPE = "json"; // JSON is a reserved word
const JSONB = exports.JSONB = "jsonb";
const LINE = exports.LINE = "line";
const LSEG = exports.LSEG = "lseg";
// not supported: const MONEY = exports.MONEY = "money";
const NUMERIC = exports.NUMERIC = "numeric";
const PATH = exports.PATH = "path";
const POINT = exports.POINT = "point";
const POLYGON = exports.POLYGON = "polygon";
const REAL = exports.REAL = "real";
const SMALLINT = exports.SMALLINT = "smallint";
const SMALLSERIAL = exports.SMALLSERIAL = "smallserial";
const SERIAL = exports.SERIAL = "serial";
const SERIAL2 = exports.SERIAL2 = "serial2";
const SERIAL4 = exports.SERIAL4 = "serial4";
const SERIAL8 = exports.SERIAL8 = "serial8";
const TEXT = exports.TEXT = "text";
const TIME = exports.TIME = "time";
const TIME_WITH_TIMEZONE = exports.TIME_WITH_TIMEZONE = "time with time zone";
const TIMETZ = exports.TIMETZ = "timetz";
const TIMESTAMP = exports.TIMESTAMP = "timestamp";
const TIMESTAMP_WITH_TIMEZONE = exports.TIMESTAMP_WITH_TIMEZONE = "timestamp with time zone";
const TIMESTAMPTZ = exports.TIMESTAMPTZ = "timestamptz";
const TSQUERY = exports.TSQUERY = "tsquery";
const TSVECTOR = exports.TSVECTOR = "tsvector";
const UUID = exports.UUID = "uuid";
const VARBIT = exports.VARBIT = "varbit";
const VARCHAR = exports.VARCHAR = "varchar";
const XML_TYPE = exports.XML_TYPE = "xml"; // XML is a reserved word
// this one isn't in the list of psql data types, but returned by pg_class catalog
const NAME = exports.NAME = "name";

const ALIASES = exports.DATA_TYPE_ALIASES = {};
ALIASES[BIGINT] = [INT8];
ALIASES[BIGSERIAL] = [SERIAL8];
// ALIASES[BIT_VARYING] = [VARBIT];
ALIASES[BOOLEAN] = [BOOL];
ALIASES[CHARACTER] = [CHAR, BPCHAR];
ALIASES[CHARACTER_VARYING] = [VARCHAR, NAME];
ALIASES[DOUBLE_PRECISION] = [FLOAT8];
ALIASES[INTEGER] = [INT, INT4];
ALIASES[NUMERIC] = [DECIMAL];
ALIASES[REAL] = [FLOAT4];
ALIASES[SMALLINT] = [INT2];
ALIASES[SMALLSERIAL] = [SERIAL2];
ALIASES[SERIAL] = [SERIAL4];
// ALIASES[TIME_WITH_TIMEZONE] = [TIMETZ];
ALIASES[TIMESTAMP_WITH_TIMEZONE] = [TIMESTAMPTZ];

const wrapGetter = (getter) => {
    return (resultSet, index) => {
        return resultSet.wasNull() ? null : getter(resultSet, index);
    };
};

const wrapSetter = (setter) => {
    return (statement, index, value) => {
        if (value === undefined || value === null) {
            statement.setNull(index, java.sql.Types.NULL);
        } else {
            setter(statement, index, value);
        }
    };
};

const DataType = function(getter, setter) {
    Object.defineProperties(this, {
        "get": {"value": getter},
        "set": {"value": wrapSetter(setter)},
    });
    return this;
};

DataType.prototype.toString = function() {
    return "[DataType]";
};

exports.getByColumnTypeName = (name) => {
    if (!DATA_TYPES.hasOwnProperty(name)) {
        throw new Error("Unknown data type '" + name + "'");
    }
    return DATA_TYPES[name];
};

const getString = (resultSet, index) => resultSet.getString(index);
const setString = (statement, index, value) => statement.setString(index, value);
const getLong = (resultSet, index) => resultSet.getLong(index);
const setLong = (statement, index, value) => statement.setLong(index, value);
const getFloat = (resultSet, index) => resultSet.getFloat(index);
const setFloat = (statement, index, value) => statement.setFloat(index, value);
const getDouble = (resultSet, index) => resultSet.getDouble(index);
const setDouble = (statement, index, value) => statement.setDouble(index, value);
const getInt = (resultSet, index) => resultSet.getInt(index);
const setInt = (statement, index, value) => statement.setInt(index, value);
const getBoolean = (resultSet, index) => resultSet.getBoolean(index);
const setBoolean = (statement, index, value) => statement.setBoolean(index, value);
const getByteArray = (resultSet, index) => {
    const inStream = resultSet.getBinaryStream(index);
    if (inStream === null) {
        return null;
    }
    const out = new ByteArrayOutputStream();
    const buffer = new binary.ByteArray(2048);
    let read = -1;
    while ((read = inStream.read(buffer)) > -1) {
        out.write(buffer, 0, read);
    }
    return binary.ByteArray.wrap(out.toByteArray());
};
const setByteArray = (statement, index, value) => {
    if (!value instanceof binary.Binary &&
            !value.getClass().getComponentType().equals(Byte.TYPE)) {
        throw new Error("Expected byte[] for binary column");
    }
    statement.setBytes(index, value);
};
const getDate = (resultSet, index) => {
    const date = resultSet.getDate(index);
    return (date !== null) ? new Date(resultSet.getDate(index).getTime()) : null;
};
const setDate = (statement, index, value) => statement.setDate(index, new java.sql.Date(value.getTime()));
const getTimestamp = (resultSet, index) => resultSet.getTimestamp(index);
const setTimestamp = (statement, index, value) => statement.setTimestamp(index, new java.sql.Timestamp(value.getTime()));
const getJson = (resultSet, index) => {
    const source = resultSet.getObject(index);
    return (source !== null) ? JSON.parse(source) : null;
}
const setJson = (statement, index, value) => {
    if (typeof(value) !== "string") {
        value = JSON.stringify(value);
    }
    statement.setObject(index, value, java.sql.Types.OTHER)
};

const DATA_TYPES = exports.DATA_TYPES = {};

DATA_TYPES[BIGINT] = new DataType(getLong, setLong);
DATA_TYPES[BIGSERIAL] = new DataType(getLong, setLong);
DATA_TYPES[CHARACTER_VARYING] = new DataType(getString, setString);
// TODO: BIT
// TODO: BIT_VARYING
DATA_TYPES[BOOLEAN] = new DataType(getBoolean, setBoolean);
// TODO: BOX
DATA_TYPES[BYTEA] = new DataType(getByteArray, setByteArray);
DATA_TYPES[CHARACTER] = new DataType(getString, setString);
// TODO: CIDR
// TODO: CIRCLE
DATA_TYPES[DATE] = new DataType(getDate, setDate);
DATA_TYPES[DOUBLE_PRECISION] = new DataType(getFloat, setFloat);
// TODO: INET
DATA_TYPES[INTEGER] = new DataType(getInt, setInt);
// TODO: INTERVAL
DATA_TYPES[JSON_TYPE] = new DataType(getJson, setJson);
DATA_TYPES[JSONB] = new DataType(getJson, setJson);
// TODO: LINE
// TODO: LSEG
DATA_TYPES[NUMERIC] = new DataType(getDouble, setDouble);
// TODO: PATH
// TODO: POINT
// TODO: POLYGON
DATA_TYPES[REAL] = new DataType(getDouble, setDouble);
DATA_TYPES[SMALLINT] = new DataType(getInt, setInt);
DATA_TYPES[SMALLSERIAL] = new DataType(getInt, setInt);
DATA_TYPES[SERIAL] = new DataType(getInt, setInt);
DATA_TYPES[TEXT] = new DataType(getString, setString);
// TODO: TIME
DATA_TYPES[TIMESTAMP] = new DataType(getTimestamp, setTimestamp);
DATA_TYPES[TIMESTAMP_WITH_TIMEZONE] = new DataType(getTimestamp, setTimestamp);
// TODO: TSQUERY
// TODO: TSVECTOR
// TODO: UUID
// TODO: XML

Object.keys(ALIASES).forEach((type) => ALIASES[type].forEach((alias) => {
    DATA_TYPES[alias] = DATA_TYPES[type];
}));
