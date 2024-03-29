require("./utils").loadJars();

const binary = require("binary");
const io = require("io");
const {Byte} = java.lang;
const {Calendar} = java.util;
const {PGbox, PGpoint, PGcircle, PGline, PGlseg, PGpath, PGpolygon} = org.postgresql.geometric;
const {PGobject, PGTime, PGInterval} = org.postgresql.util;

const BIGINT = exports.BIGINT = "bigint";
const BIGSERIAL = exports.BIGSERIAL = "bigserial";
const BIT = exports.BIT = "bit";
const BIT_ARRAY = exports.BIT_ARRAY = "_bit";
const BIT_VARYING = exports.BIT_VARYING = "bit varying";
const BOOL = exports.BOOL = "bool";
const BOOL_ARRAY = exports.BOOL_ARRAY = "_bool";
const BOOLEAN = exports.BOOLEAN = "boolean";
const BOX = exports.BOX = "box";
const BPCHAR = exports.BPCHAR = "bpchar"; // FIXME: getColumnTypeName returns "bpchar" for "char"
const BYTEA = exports.BYTEA = "bytea";
// TODO: const BYTEA_ARRAY = exports.BYTEA_ARRAY = "_bytea";
const CHAR = exports.CHAR = "char";
const CHAR_ARRAY = exports.CHAR_ARRAY = "_bpchar";
const CHARACTER = exports.CHARACTER = "character";
const CHARACTER_VARYING = exports.CHARACTER_VARYING = "character varying";
const CIDR = exports.CIDR = "cidr";
const CIRCLE = exports.CIRCLE = "circle";
const DATE = exports.DATE = "date";
const DECIMAL = exports.DECIMAL = "decimal";
const DOUBLE_PRECISION = exports.DOUBLE_PRECISION = "double precision";
const FLOAT4 = exports.FLOAT4 = "float4";
const FLOAT4_ARRAY = exports.FLOAT4_ARRAY = "_float4";
const FLOAT8 = exports.FLOAT8 = "float8";
const FLOAT8_ARRAY = exports.FLOAT8_ARRAY = "_float8";
const INET = exports.INET = "inet";
const INT = exports.INTEGER = "int";
const INT2 = exports.INT2 = "int2";
const INT2_ARRAY = exports.INT2_ARRAY = "_int2";
const INT4 = exports.INT4 = "int4";
const INT4_ARRAY = exports.INT4_ARRAY = "_int4";
const INT8 = exports.INT8 = "int8";
const INT8_ARRAY = exports.INT8_ARRAY = "_int8";
const INTEGER = exports.INTEGER = "integer";
const INTERVAL = exports.INTERVAL = "interval";
const JSON_TYPE = exports.JSON_TYPE = "json"; // JSON is a reserved word
const JSONB = exports.JSONB = "jsonb";
const LINE = exports.LINE = "line";
const LSEG = exports.LSEG = "lseg";
// not supported: const MONEY = exports.MONEY = "money";
const NUMERIC = exports.NUMERIC = "numeric";
const NUMERIC_ARRAY = exports.NUMERIC_ARRAY = "_numeric";
const MACADDR = exports.MACADDR = "macaddr";
const MACADDR8 = exports.MACADDR8 = "macaddr8";
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
const TEXT_ARRAY = exports.TEXT_ARRAY = "_text";
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
const VARBIT_ARRAY = exports.VARBIT_ARRAY = "_varbit";
const VARCHAR = exports.VARCHAR = "varchar";
const VARCHAR_ARRAY = exports.VARCHAR_ARRAY = "_varchar";
const XML_TYPE = exports.XML_TYPE = "xml"; // XML is a reserved word
// this one isn't in the list of psql data types, but returned by pg_class catalog
const NAME = exports.NAME = "name";

const ALIASES = exports.DATA_TYPE_ALIASES = {};
ALIASES[BIGINT] = [INT8];
ALIASES[BIGSERIAL] = [SERIAL8];
ALIASES[BIT_VARYING] = [VARBIT];
ALIASES[BOOLEAN] = [BOOL];
ALIASES[CHARACTER] = [CHAR, BPCHAR];
ALIASES[CHARACTER_VARYING] = [VARCHAR];
ALIASES[DOUBLE_PRECISION] = [FLOAT8];
ALIASES[INTEGER] = [INT, INT4];
ALIASES[NUMERIC] = [DECIMAL];
ALIASES[REAL] = [FLOAT4];
ALIASES[SMALLINT] = [INT2];
ALIASES[SMALLSERIAL] = [SERIAL2];
ALIASES[SERIAL] = [SERIAL4];
ALIASES[TIME_WITH_TIMEZONE] = [TIMETZ];
ALIASES[TIMESTAMP_WITH_TIMEZONE] = [TIMESTAMPTZ];

const wrapSetter = (setter) => {
    return (statement, index, value) => {
        if (value === undefined || value === null) {
            statement.setNull(index, java.sql.Types.NULL);
        } else {
            setter(statement, index, value);
        }
    };
};

const DataType = function(type, getter, setter) {
    Object.defineProperties(this, {
        "type": {"value": type},
        "get": {"value": getter},
        "set": {"value": wrapSetter(setter)},
    });
    return this;
};

DataType.prototype.toString = function() {
    return "[DataType " + this.type + "]";
};

exports.get = (name) => {
    if (!DATA_TYPES.hasOwnProperty(name)) {
        throw new Error("Unknown data type '" + name + "'");
    }
    return DATA_TYPES[name];
};

const setObject = (type) => {
    return (statement, index, value) => {
        const obj = new PGobject();
        obj.setType(type);
        obj.setValue(value);
        statement.setObject(index, obj, java.sql.Types.OTHER);
    };
};

const toArrayLiteral = (arr) => {
    return "{" + JSON.stringify(arr).slice(1, -1) + "}";
};

const getArray = (resultSet, index) => {
    const array = resultSet.getArray(index);
    return !resultSet.wasNull() ? array.getArray().slice() : null;
};

const setArray = (statement, index, value) => {
    statement.setObject(index, toArrayLiteral(value), java.sql.Types.OTHER);
};

const getString = (resultSet, index) => {
    const value = resultSet.getString(index);
    return !resultSet.wasNull() ? value : null;
};
const setString = (statement, index, value) => statement.setString(index, value);

const getLong = (resultSet, index) => {
    const value = resultSet.getLong(index);
    return !resultSet.wasNull() ? value : null;
};
const setLong = (statement, index, value) => statement.setLong(index, value);

const getFloat = (resultSet, index) => {
    const value = resultSet.getFloat(index);
    return !resultSet.wasNull() ? value : null;
};
const setFloat = (statement, index, value) => statement.setFloat(index, value);

const getDouble = (resultSet, index) => {
    const value = resultSet.getDouble(index);
    return !resultSet.wasNull() ? value : null;
};
const setDouble = (statement, index, value) => statement.setDouble(index, value);

const getNumeric = (resultSet, index) => {
    const value = resultSet.getObject(index);
    return !resultSet.wasNull() ? value.doubleValue() : null;
};
const setNumeric = (statement, index, value) => {
    statement.setObject(index, value, java.sql.Types.NUMERIC);
};

const getInt = (resultSet, index) => {
    const value = resultSet.getInt(index);
    return !resultSet.wasNull() ? value : null;
};
const setInt = (statement, index, value) => statement.setInt(index, value);

const getBoolean = (resultSet, index) => {
    const value = resultSet.getBoolean(index);
    return !resultSet.wasNull() ? value : null;
};
const setBoolean = (statement, index, value) => statement.setBoolean(index, value);

const getByteArray = (resultSet, index) => {
    const inStream = resultSet.getBinaryStream(index);
    if (resultSet.wasNull()) {
        return null;
    }
    const stream = new io.Stream(inStream);
    const memStream = new io.MemoryStream();
    stream.copy(memStream);
    stream.close();
    return memStream.content;
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
    return !resultSet.wasNull() ? new Date(date.getTime()) : null;
};
const setDate = (statement, index, value) => statement.setDate(index, new java.sql.Date(value.getTime()));

const getTime = (resultSet, index) => {
    const time = resultSet.getTime(index);
    return !resultSet.wasNull() ? time.getTime() : null;
};
const setTime = (statement, index, value) => {
    statement.setTime(index, new PGTime(value, Calendar.getInstance()));
};

const getTimestamp = (resultSet, index) => {
    const timestamp = resultSet.getTimestamp(index);
    return !resultSet.wasNull() ? new Date(timestamp.getTime()) : null;
};
const setTimestamp = (statement, index, value) => statement.setTimestamp(index, new java.sql.Timestamp(value.getTime()));

const getJson = (resultSet, index) => {
    const source = resultSet.getObject(index);
    return !resultSet.wasNull() ? JSON.parse(source) : null;
};
const setJson = (statement, index, value) => {
    if (typeof(value) !== "string") {
        value = JSON.stringify(value);
    }
    statement.setObject(index, value, java.sql.Types.OTHER);
};

const getBox = (resultSet, index) => {
    const box = resultSet.getObject(index);
    return !resultSet.wasNull() ? box.point.map(point => ({"x": point.x, "y": point.y})) : null;
};
const setBox = (statement, index, value) => {
    const points = value.map(point => new PGpoint(point.x, point.y));
    statement.setObject(index, new PGbox(points[0], points[1]));
};

const getPoint = (resultSet, index) => {
    const point = resultSet.getObject(index);
    return !resultSet.wasNull() ? {"x": point.x, "y": point.y} : null;
};
const setPoint = (statement, index, value) => {
    const point = new PGpoint(value.x, value.y);
    statement.setObject(index, point);
};

const getCircle = (resultSet, index) => {
    const circle = resultSet.getObject(index);
    return !resultSet.wasNull() ? {"center": {"x": circle.center.x, "y": circle.center.y}, "radius": circle.radius} : null;
};
const setCircle = (statement, index, value) => {
    const center = new PGpoint(value.center.x, value.center.y);
    const circle = new PGcircle(center, value.radius);
    statement.setObject(index, circle);
};

const getLine = (resultSet, index) => {
    const line = resultSet.getObject(index);
    return !resultSet.wasNull() ? {"a": line.a, "b": line.b, "c": line.c} : null;
};
const setLine = (statement, index, value) => {
    const line = new PGline(value.x1, value.y1, value.x2, value.y2);
    statement.setObject(index, line);
};

const getLseg = (resultSet, index) => {
    const lseg = resultSet.getObject(index);
    if (resultSet.wasNull()) {
        return null;
    }
    return [
        {"x": lseg.point[0].x, "y": lseg.point[0].y},
        {"x": lseg.point[1].x, "y": lseg.point[1].y}
    ];
};
const setLseg = (statement, index, value) => {
    const points = value.map(point => new PGpoint(point.x, point.y));
    statement.setObject(index, new PGlseg(points[0], points[1]));
};

const getPath = (resultSet, index) => {
    const path = resultSet.getObject(index);
    if (resultSet.wasNull()) {
        return null;
    }
    return {
        "points": path.points.map(point => ({"x": point.x, "y": point.y})),
        "isOpen": path.isOpen()
    };
};
const setPath = (statement, index, value) => {
    const points = value.points.map(point => new PGpoint(point.x, point.y));
    statement.setObject(index, new PGpath(points, value.isOpen === true));
};

const getPolygon = (resultSet, index) => {
    const polygon = resultSet.getObject(index);
    if (resultSet.wasNull()) {
        return null;
    }
    return polygon.points.map(point => ({"x": point.x, "y": point.y}));
};
const setPolygon = (statement, index, value) => {
    const points = value.map(point => new PGpoint(point.x, point.y));
    statement.setObject(index, new PGpolygon(points));
};

const getIntervalType = (resultSet, index) => {
    const interval = resultSet.getObject(index);
    if (resultSet.wasNull()) {
        return null;
    }
    return {
        "years": interval.years,
        "months": interval.months,
        "days": interval.days,
        "hours": interval.hours,
        "minutes": interval.minutes,
        "seconds": interval.seconds
    };
};
const setIntervalType = (statement, index, value) => {
    const interval = new PGInterval(value.years || 0, value.months || 0, value.days || 0,
            value.hours || 0, value.minutes || 0, value.seconds || 0);
    statement.setObject(index, interval);
};

const DATA_TYPES = exports.DATA_TYPES = {};

DATA_TYPES[BIGINT] = new DataType(BIGINT, getLong, setLong);
DATA_TYPES[BIGSERIAL] = new DataType(BIGSERIAL, getLong, setLong);
DATA_TYPES[CHARACTER_VARYING] = new DataType(CHARACTER_VARYING, getString, setString);
DATA_TYPES[BIT] = new DataType(BIT, getString, setObject(BIT));
DATA_TYPES[BIT_VARYING] = new DataType(BIT_VARYING, getString, setObject(BIT));
DATA_TYPES[BOOLEAN] = new DataType(BOOLEAN, getBoolean, setBoolean);
DATA_TYPES[BOX] = new DataType(BOX, getBox, setBox);
DATA_TYPES[BYTEA] = new DataType(BYTEA, getByteArray, setByteArray);
DATA_TYPES[CHARACTER] = new DataType(CHARACTER, getString, setString);
DATA_TYPES[CIDR] = new DataType(CIDR, getString, setObject(CIDR));
DATA_TYPES[CIRCLE] = new DataType(CIRCLE, getCircle, setCircle);
DATA_TYPES[DATE] = new DataType(DATE, getDate, setDate);
DATA_TYPES[DOUBLE_PRECISION] = new DataType(DOUBLE_PRECISION, getFloat, setFloat);
DATA_TYPES[INET] = new DataType(INET, getString, setObject(INET));
DATA_TYPES[INTEGER] = new DataType(INTEGER, getInt, setInt);
DATA_TYPES[INTERVAL] = new DataType(INTERVAL, getIntervalType, setIntervalType);
DATA_TYPES[JSON_TYPE] = new DataType(JSON_TYPE, getJson, setJson);
DATA_TYPES[JSONB] = new DataType(JSONB, getJson, setJson);
DATA_TYPES[LINE] = new DataType(LINE, getLine, setLine);
DATA_TYPES[LSEG] = new DataType(LSEG, getLseg, setLseg);
DATA_TYPES[NAME] = new DataType(NAME, getString, setString);
DATA_TYPES[NUMERIC] = new DataType(NUMERIC, getNumeric, setNumeric);
DATA_TYPES[MACADDR] = new DataType(MACADDR, getString, setObject(MACADDR));
DATA_TYPES[MACADDR8] = new DataType(MACADDR8, getString, setObject(MACADDR8));
DATA_TYPES[PATH] = new DataType(PATH, getPath, setPath);
DATA_TYPES[POINT] = new DataType(POINT, getPoint, setPoint);
DATA_TYPES[POLYGON] = new DataType(POLYGON, getPolygon, setPolygon);
DATA_TYPES[REAL] = new DataType(REAL, getDouble, setDouble);
DATA_TYPES[SMALLINT] = new DataType(SMALLINT, getInt, setInt);
DATA_TYPES[SMALLSERIAL] = new DataType(SMALLSERIAL, getInt, setInt);
DATA_TYPES[SERIAL] = new DataType(SERIAL, getInt, setInt);
DATA_TYPES[TEXT] = new DataType(TEXT, getString, setString);
DATA_TYPES[TIME] = new DataType(TIME, getTime, setTime);
DATA_TYPES[TIME_WITH_TIMEZONE] = new DataType(TIME_WITH_TIMEZONE, getTime, setTime);
DATA_TYPES[TIMESTAMP] = new DataType(TIMESTAMP, getTimestamp, setTimestamp);
DATA_TYPES[TIMESTAMP_WITH_TIMEZONE] = new DataType(TIMESTAMP_WITH_TIMEZONE, getTimestamp, setTimestamp);
DATA_TYPES[TSQUERY] = new DataType(TSQUERY, getString, setObject(TSQUERY));
DATA_TYPES[TSVECTOR] = new DataType(TSVECTOR, getString, setObject(TSVECTOR));
DATA_TYPES[UUID] = new DataType(UUID, getString, setObject(UUID));
DATA_TYPES[XML_TYPE] = new DataType(XML_TYPE, getString, setObject(XML_TYPE));

[
    BIT_ARRAY,
    BOOL_ARRAY,
    // TODO BYTEA_ARRAY,
    CHAR_ARRAY,
    INT2_ARRAY,
    INT4_ARRAY,
    INT8_ARRAY,
    FLOAT4_ARRAY,
    FLOAT8_ARRAY,
    NUMERIC_ARRAY,
    TEXT_ARRAY,
    VARBIT_ARRAY,
    VARCHAR_ARRAY
].forEach(type => DATA_TYPES[type] = new DataType(type, getArray, setArray));

Object.keys(ALIASES).forEach((type) => ALIASES[type].forEach((alias) => {
    DATA_TYPES[alias] = DATA_TYPES[type];
}));
