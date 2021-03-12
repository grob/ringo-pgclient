const VARCHAR = "varchar";
const INT4 = "int4";
const INT8 = "int8";
const BOOL = "bool";
const TIMESTAMP = "timestamp";
// this one isn't in the list of psql data types, but returned by pg_class catalog
const NAME = "name";

const wrapGetter = (getter) => {
    return (resultSet, index) => {
        const value = getter(resultSet, index);
        if (resultSet.wasNull()) {
            return null;
        }
        return value;
    };
};

const wrapSetter = (setter) => {
    return (statement, index, value) => {
        if (value === undefined || value === null) {
            statement.setNull(index);
        } else {
            setter(statement, index, value);
        }
    };
};

const nullSetter = (statement, index) => {
    statement.setNull(index, java.sql.Types.NULL);
};

const DataType = function(getter, setter) {
    Object.defineProperties(this, {
        "get": {"value": wrapGetter(getter)},
        "set": {"value": wrapSetter(setter)},
    });
    return this;
};

DataType.prototype.toString = function() {
    return "[DataType " + this.id + "]";
};

exports.getByColumnTypeName = (name) => {
    if (!DATA_TYPES.hasOwnProperty(name)) {
        throw new Error("Unknown data type '" + name + "'");
    }
    return DATA_TYPES[name];
};

const DATA_TYPES = exports.DATA_TYPES = {};

DATA_TYPES[VARCHAR] = DATA_TYPES[NAME] = new DataType(
        (resultSet, index) => resultSet.getString(index),
        (statement, index, value) => statement.setString(index, value)
);
DATA_TYPES[INT4] = new DataType(
        (resultSet, index) => resultSet.getInt(index),
        (statement, index, value) => statement.setInt(index, value)
);
DATA_TYPES[INT8] = new DataType(
        (resultSet, index) => resultSet.getLong(index),
        (statement, index, value) => statement.setLong(index, value)
);
DATA_TYPES[BOOL] = new DataType(
        (resultSet, index) => resultSet.getBoolean(index),
        (statement, index, value) => statement.setBoolean(index, value)
);
DATA_TYPES[TIMESTAMP] = new DataType(
        (resultSet, index) => {
            const timestamp = resultSet.getTimestamp(index);
            return (timestamp !== null) ? new Date(timestamp.getTime()) : null;
        },
        (statement, index, value) => statement.setTimestamp(index,
                new java.sql.Timestamp(value.getTime()))
);

exports.getParameterSetter = (value) => {
    if (value === undefined || value === null) {
        return nullSetter;
    } else if (Number.isFinite(value)) {
        return DATA_TYPES[(value % 1 === 0) ? INT8 : DOUBLE].set;
    } else if (typeof(value) === "string") {
        return DATA_TYPES[VARCHAR].set;
    } else if (typeof(value) === "boolean") {
        return DATA_TYPES[BOOL].set;
    } else if (value instanceof Date) {
        return DATA_TYPES[TIMESTAMP].set;
    }
    throw new Error("Unable to determine data type for '" + value + "'");
};
