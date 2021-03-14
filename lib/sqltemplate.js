const {TYPE_FORWARD_ONLY, CONCUR_READ_ONLY} = java.sql.ResultSet;
const dataTypes = require("./datatypes");
const log = require("ringo/logging").getLogger(module.id);
const mapper = require("./mapper");

const PATTERN_PARAM = /#{([^}]+)}/g;

const setParams = (statement, statementMetaData, mapping, params) => {
    mapping.forEach((name, index) => {
        if (!params.hasOwnProperty(name)) {
            throw new Error("Missing required parameter '" + name + "'");
        }
        const position = index + 1;
        const value = params[name];
        const dataType = dataTypes.DATA_TYPES[statementMetaData.getParameterTypeName(position)];
        dataType.set(statement, position, value);
    });
};

const setBatchParams = (statement, statementMetaData, mapping, params, batchSize) => {
    const paramTypes = mapping.map((name, index) => {
        return dataTypes.DATA_TYPES[statementMetaData.getParameterTypeName(index + 1)];
    });
    return params.reduce((updateCounts, param, index) => {
        mapping.forEach((name, index) => {
            paramTypes[index].set(statement, index + 1, param[index]);
            statement.addBatch();
        });
        if (Number.isFinite(batchSize) && (index % batchSize) === 0) {
            return updateCounts.concat(Array.prototype.slice.call(statement.executeBatch()));
        }
        return updateCounts;
    }, []);
};

const SqlTemplate = function(templateDesc) {

    Object.defineProperties(this, {
        "templateDesc": {"value": templateDesc},
        "rowMapper": {
            "value": mapper.mapToJson,
            "writable": true
        }
    });

    return this;
};

SqlTemplate.prototype.setMapper = function(mapper) {
    if (typeof(mapper) !== "function") {
        debugger
        throw new Error("Row mapper must be a function");
    }
    this.rowMapper = mapper;
    return this;
};

SqlTemplate.prototype.execute = function(client, params) {
    let connection, statement;
    try {
        if (log.isDebugEnabled()) {
            log.debug(this.templateDesc.sql,
                    params ? JSON.stringify(params) : "(no params)");
        }
        connection = client.getConnection();
        statement = connection.prepareStatement(this.templateDesc.sql,
                TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
        const metaData = statement.getParameterMetaData();
        setParams(statement, metaData, this.templateDesc.mapping, params);
        const hasResultSet = statement.execute();
        if (hasResultSet) {
            const resultSet = statement.getResultSet();
            if (this.rowMapper !== null) {
                return this.rowMapper(resultSet);
            }
            return resultSet;
        }
        return statement.getUpdateCount();
    } catch (e) {
        log.error("Statement '{}' failed with", this.templateDesc.sql, e);
        throw e;
    } finally {
        statement && statement.close(); // closes resultSet too
        if (connection && !client.hasTransaction()) {
            connection.close();
        }
    }
};

SqlTemplate.prototype.executeBatch = function(client, params, batchSize) {
    let connection, statement;
    try {
        if (log.isDebugEnabled()) {
            log.debug(this.templateDesc.sql,
                    params ? JSON.stringify(params) : "(no params)");
        }
        connection = client.getConnection();
        statement = connection.prepareStatement(this.templateDesc.sql,
                TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
        const metaData = statement.getParameterMetaData();
        const updateCounts = setBatchParams(statement, metaData, this.templateDesc.mapping, params, batchSize);
        const result = updateCounts.concat(Array.prototype.slice.call(statement.executeBatch()));
        return result.every(value => value !== java.sql.Statement.EXECUTE_FAILED);
    } catch (e) {
        log.error("Batch statement '{}' failed with", this.templateDesc.sql, e);
        throw e;
    } finally {
        statement && statement.close(); // closes resultSet too
        if (connection && !client.hasTransaction()) {
            connection.close();
        }
    }
};

const parse = exports.parse = (template) => {
    const mapping = [];
    const sql = template.replace(PATTERN_PARAM, (match, propname) => {
        mapping.push(propname);
        return "?";
    });
    return {
        "sql": sql,
        "mapping": mapping
    };
};

exports.build = (template) => {
    return new SqlTemplate(parse(template));
};

exports.newInstance = (templateDesc) => {
    return new SqlTemplate(templateDesc);
};