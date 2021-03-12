const {TYPE_FORWARD_ONLY, CONCUR_READ_ONLY} = java.sql.ResultSet;
const dataTypes = require("./datatypes");
const log = require("ringo/logging").getLogger(module.id);
const mapper = require("./mapper");

const PATTERN_PARAM = /#{([^}]+)}/g;

const setStatementParams = (statement, mapping, params) => {
    mapping.forEach((name, idx) => {
        if (!params.hasOwnProperty(name)) {
            throw new Error("Missing required parameter '" + name + "'");
        }
        const value = params[name];
        const setter = dataTypes.getParameterSetter(value);
        setter(statement, idx + 1, value);
    });
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
        log.debug(this.templateDesc.source);
        connection = client.getConnection();
        statement = connection.prepareStatement(this.templateDesc.source,
                TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
        setStatementParams(statement, this.templateDesc.mapping, params);
        const hasResultSet = statement.execute();
        if (hasResultSet) {
            const resultSet = statement.getResultSet();
            if (this.rowMapper !== null) {
                return this.rowMapper(resultSet);
            }
            return resultSet;
        }
        return statement.getUpdateCount();
    } finally {
        statement && statement.close(); // closes resultSet too
        if (connection && !client.hasTransaction()) {
            connection.close();
        }
    }
};

const parse = exports.parse = (template) => {
    const mapping = [];
    const source = template.replace(PATTERN_PARAM, (match, propname) => {
        mapping.push(propname);
        return "?";
    });
    return {
        "source": source,
        "mapping": mapping
    };
};

exports.build = (template) => {
    return new SqlTemplate(parse(template));
};

exports.newInstance = (templateDesc) => {
    return new SqlTemplate(templateDesc);
};