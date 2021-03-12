const dataTypes = require("./datatypes");

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

exports.mapToModel = (Model) => {
    return (resultSet) => {
        return mapResultSet(resultSet, (resultSet, columns) => {
            return Model.createInstance(mapRow(resultSet, columns))
        });
    };
};