const dataTypes = require("./datatypes");
const modelUtils = require("./model/utils");

const getColumns = (metaData) => {
    const columns = [];
    for (let idx = 1; idx <= metaData.columnCount; idx += 1) {
        columns.push({
            "label": metaData.getColumnLabel(idx),
            "dataType": dataTypes.getByColumnTypeName(metaData.getColumnTypeName(idx)),
            "precision": metaData.getPrecision(idx),
            "scale": metaData.getScale(idx)
        });
    }
    return columns;
};

const mapRow = exports.mapRow = (resultSet, columns) => {
    return columns.reduce((row, column, idx) => {
        row[column.label] = column.dataType.get(resultSet, idx + 1,
                column.precision, column.scale);
        return row;
    }, {});
};

const mapResultSet = exports.mapResultSet = (resultSet, rowMapper) => {
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

exports.mapToJson = (resultSet) => {
    return mapResultSet(resultSet, mapRow);
};

exports.mapToModel = (Model, client) => {
    return (resultSet) => {
        return mapResultSet(resultSet, (resultSet, columns) => {
            const idMapping = Model.mapping.id;
            const position = resultSet.findColumn(idMapping.column);
            const id = dataTypes.getByColumnTypeName(idMapping.type).get(resultSet, position);
            const key = modelUtils.getKey(Model.type, id);
            const transaction = client.getTransaction();
            const useCache = client.cache && (!transaction || !transaction.containsKey(key));
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