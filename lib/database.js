const log = require("ringo/logging").getLogger(module.id);
const SqlTemplate = require("./sqltemplate");

const getColumnSpec = (propMapping) => {
   return [
       propMapping.column,
       propMapping.type,
       propMapping.constraint
   ].join(" ").trim(); 
};

/**
 * Returns a fully qualified name (<schema>.<name>) if a schema is passed,
 * otherwise the name passed as argument
 * @return {String} The (fully qualified) name
 */
const getFqn = exports.getFqn = (name, schema) => {
    if (typeof(schema) === "string" && schema.length > 0) {
        return schema + "." + name;
    }
    return name;
}

/**
 * Initializes the database table of the model and (if specified in the mapping)
 * the sequence as well
 * @param {Client} client The client to use
 * @param {Object} mapping The model mapping
 */
exports.initModel = (client, mapping) => {
    const columns = Object.keys(mapping.properties)
            .reduce((columns, name) => {
                columns.push(getColumnSpec(mapping.properties[name]));
                return columns;
            }, [getColumnSpec(mapping.id)]).join(", ");
    createTable(client, mapping.table, columns, mapping.schema);
    if (mapping.id.sequence) {
        createSequence(client, mapping.id.sequence, 
                mapping.table + "." + mapping.id.column, mapping.schema);
    }
};

/**
 * Creates the database table for a model
 * @param {Client} client The client to use
 * @param {String} name The name of the table
 * @param {Array} columns The column definitions as specified in the model mapping
 * @param {String} schema Optional schema name
 */
const createTable = exports.createTable = (client, name, columns, schema) => {
    const statement = "create table if not exists " + getFqn(name, schema) + " (" + columns + ")";
    return SqlTemplate.build(statement).execute(client);
};

/**
 * Creates a model sequence
 * @param {Client} client The client to use
 * @param {String} name The name of the sequence
 * @param {String} owner The column owning the sequence (<table>.<column>)
 * @param {String} schema Optional schema name
 */
const createSequence = exports.createSequence = (client, name, owner, schema) => {
    const statement = "create sequence if not exists " + getFqn(name, schema) + " owned by " + owner;
    return SqlTemplate.build(statement).execute(client);
};

/**
 * Returns an array containing the tables defined in the database
 * @return {Array} An array containing objects for every table with the
 * table name and schema as properties
 */
const getTables = exports.getTables = (client, schema) => {
    let connection = client.getConnection();
    let resultSet = null;
    try {
        resultSet = connection.getMetaData()
                .getTables(null,schema || null, "%", ["TABLE"]);
        const result = [];
        while (resultSet.next()) {
            result.push({
                "schema": resultSet.getString("TABLE_SCHEM"),
                "name": resultSet.getString("TABLE_NAME")
            });
        }
        return result;
    } finally {
        resultSet && resultSet.close();
        connection.close();
    }
};

/**
 * Returns a list of sequence names
 * @param {Client} client The client to use
 * @returns {Array} An array containing sequence names
 */
const getSequences = exports.getSequences = (client) => {
    return SqlTemplate.build("select relname as name from pg_class where relkind = #{kind}")
            .execute(client, {"kind": "S"});
};

/**
 * Drops the table with the given name in the optional schema
 * @param {Client} client The client to use
 * @param {String} name The name of the table to drop
 * @param {String} schema Optional schema
 */
const dropTable = exports.dropTable = (client, name, schema) => {
    return SqlTemplate.build("drop table if exists " + getFqn(name, schema) + " cascade")
            .execute(client);
};

/**
 * Drops the sequence with the given name
 * @param {Client} client The client to use
 * @param {String} name The name of the sequence to drop
 */
const dropSequence = exports.dropSequence = (client, name) => {
    return SqlTemplate.build("drop sequence if exists " + name + " cascade")
            .execute(client);
};

/**
 * Drops all tables and sequences
 * @param {Client} client The client to use
 * @param {String} schema Optional schema name
 */
const dropAll = exports.dropAll = (client, schema) => {
    getTables(client, schema).forEach(table => dropTable(client, table.name, table.schema));
    getSequences(client).forEach(sequence => dropSequence(client, sequence.name));
};