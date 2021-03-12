const log = require("ringo/logging").getLogger(module.id);
const SqlTemplate = require("./sqltemplate");

const getColumnSpec = (propMapping) => {
   return [
       propMapping.column,
       propMapping.type,
       propMapping.constraint
   ].join(" ").trim(); 
};

const getFqn = (name, schema) => {
    if (typeof(schema) === "string" && schema.length > 0) {
        return schema + "." + name;
    }
    return name;
}

exports.initModel = (client, mapping) => {
    const columns = Object.keys(mapping.properties)
            .reduce((columns, name) => {
                columns.push(getColumnSpec(mapping.properties[name]));
                return columns;
            }, [getColumnSpec(mapping.id)]).join(", ");
    createTable(client, getFqn(mapping.table, mapping.schema), columns);
    if (mapping.id.sequence) {
        createSequence(client, getFqn(mapping.id.sequence, mapping.schema), 
                mapping.table + "." + mapping.id.column, mapping.schema);
    }
};

const createTable = exports.createTable = (client, name, columns, schema) => {
    const statement = "create table if not exists " + name + " (" + columns + ")";
    return SqlTemplate.build(statement).execute(client);
};

const createSequence = exports.createSequence = (client, name, owner, schema) => {
    const statement = "create sequence if not exists " + name + " owned by " + owner;
    return SqlTemplate.build(statement).execute(client);
};

const getTables = exports.getTables = (client, schemaName) => {
    let connection = client.getConnection();
    let resultSet = null;
    try {
        resultSet = connection.getMetaData()
                .getTables(null,schemaName || null, "%", ["TABLE"]);
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

const dropTable = exports.dropTable = (client, name, schema) => {
    return SqlTemplate.build("drop table if exists " + getFqn(name, schema) + " cascade")
            .execute(client);
};

const dropSequence = exports.dropSequence = (client, name) => {
    return SqlTemplate.build("drop sequence if exists " + name + " cascade")
            .execute(client);
};

const dropAll = exports.dropAll = (client, schema) => {
    getTables(client, schema).forEach(table => dropTable(client, table.name, table.schema));
    getSequences(client).forEach(sequence => dropSequence(client, sequence.name));
};