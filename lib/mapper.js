const dataTypes = require("./datatypes");
const modelUtils = require("./model/utils");

const getColumns = (metaData) => {
    const columns = [];
    for (let idx = 1; idx <= metaData.columnCount; idx += 1) {
        columns.push({
            "label": metaData.getColumnLabel(idx),
            "dataType": dataTypes.get(metaData.getColumnTypeName(idx)),
            "precision": metaData.getPrecision(idx),
            "scale": metaData.getScale(idx)
        });
    }
    return columns;
};

const mapRow = (resultSet, columns) => {
    return columns.reduce((row, column, idx) => {
        row[column.label] = column.dataType.get(resultSet, idx + 1,
                column.precision, column.scale);
        return row;
    }, {});
};

const mapResultSet = (resultSet, rowMapper) => {
    try {
        const columns = getColumns(resultSet.getMetaData());
        const rows = [];
        while (resultSet.next()) {
            rows.push(rowMapper(resultSet, columns));
        }
        return rows;
    } finally {
        resultSet.close();
    }
};

/**
 * Contains a mapping function that converts a JDBC result set into an array
 * of objects
 * @param {java.sql.ResultSet} resultSet The result set to convert
 * @return {Array} An array of converted result set row objects
 */
exports.mapToJson = (resultSet) => {
    return mapResultSet(resultSet, mapRow);
};

/**
 * Returns a function for converting a JDBC result set into an array of model instances
 * @param {Function} Model The model constructor function to use. If the client
 * passed as argument has a model cache enabled, this mapper will utilize/populate it.
 * @param {Client} client The Client to use
 * @return {Function} The mapper function
 */
exports.mapToModel = (Model, client) => {
    return (resultSet) => {
        const idMapping = Model.mapping.id;
        const idDataType = dataTypes.get(idMapping.type);
        const transaction = client.getTransaction();
        return mapResultSet(resultSet, (resultSet, columns) => {
            const position = resultSet.findColumn(idMapping.column);
            const id = idDataType.get(resultSet, position);
            const key = modelUtils.getKey(Model.type, id);
            const useCache = !!client.cache && (!transaction || !transaction.containsKey(key));
            const data = (useCache && client.cache.containsKey(key)) ?
                    client.cache.get(key) :
                    mapRow(resultSet, columns);
            if (useCache) {
                client.cache.put(key, data);
            }
            return Model.createInstance(data);
        });
    };
};