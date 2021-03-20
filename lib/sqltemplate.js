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

/**
 * SqlTemplate constructor
 * @param {Object} templateDesc An object containing the sql query with parameter
 * placeholders ('?') and an array with parameter names used in this query
 * @return {SqlTemplate} A new SqlTemplate instance
 * @constructor
 */
const SqlTemplate = function(templateDesc) {

    Object.defineProperties(this, {
        /**
         * The template descriptor
         * @type Object
         */
        "templateDesc": {"value": templateDesc},
        /**
         * Contains the row mapper function (by default the JSON row mapper)
         * @type Function
         */
        "rowMapper": {
            "value": mapper.mapToJson,
            "writable": true
        }
    });

    return this;
};

/**
 * Use the row mapping function passed as argument
 * @param {Function} mapper The row mapper function, accepting the JDBC result
 * set as sole argument
 * @return {SqlTemplate} The SqlTemplate instance
 */
SqlTemplate.prototype.setMapper = function(mapper) {
    if (typeof(mapper) !== "function") {
        throw new Error("Row mapper must be a function");
    }
    this.rowMapper = mapper;
    return this;
};

/**
 * Executes this SqlTemplate instance. This method either returns an array
 * containing the mapped result rows (see #setMapper) or the number of
 * affected rows
 * @param {Client} client The client to use
 * @param {Object} params The parameters referenced in the query
 * @return {Array|Number}
 */
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
            return this.rowMapper(statement.getResultSet());
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

/**
 * Executes a batch of inserts or updates
 * @param {Client} client The client to use
 * @param {Object} params An array of objects containing the parameters
 * referenced in the query
 * @param {Number} batchSize Optional batch size. If not specified the whole
 * batch is sent to the database, otherwise it's split into smaller chunks
 * @return {Number} The number of affected rows
 */
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

/**
 * Parses the SQL query template into an sql query containing parameter
 * placeholders ('?') and an array containing the parameter names used therein.
 * @param {String} template The sql template to parse, using <code>#{name}</code> parameter
 * placeholders
 * @return {Object} The parsed SQL query template with properties <code>sql</code>
 * (the sql query string) and <code>mapping</code> (the referenced parameters) 
 */
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

/**
 * Parses and creates a new SqlTemplate instance
 * @param {String} template The sql template to parse, using <code>#{name}</code> parameter
 * placeholders
 * @return {SqlTemplate} A new SqlTemplate instance
 */
exports.build = (template) => {
    return new SqlTemplate(parse(template));
};

/**
 * Returns a new SqlTemplate (in contrast to [build](#build) this method expects the
 * template descriptor returned by [parse](#parse)).
 * @param {Object} templateDesc The template descriptor
 * @return {SqlTemplate} A new SqlTemplate instance
 */
exports.newInstance = (templateDesc) => {
    return new SqlTemplate(templateDesc);
};