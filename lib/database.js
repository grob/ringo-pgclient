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
 * @param {String} name The name
 * @param {String} schema Optional schema name
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
    if (Array.isArray(mapping.indexes)) {
        mapping.indexes.forEach(spec => createIndex(client, mapping.table, spec, mapping.schema));
    }
};

/**
 * Drops the table and any associated indexes/sequences
 * @param {Client} client The client to use
 * @param {Object} mapping The model mapping
 */
exports.dropModel = (client, mapping) => {
    dropTable(client, mapping.table, mapping.schema);
    getTableSequences(client, mapping.table).forEach(sequence => {
        dropSequence(client, sequence.name, sequence.schema);
    });
    getTableIndexes(client, mapping.table).forEach(index => {
        dropIndex(client, index.name, index.schema);
    });
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
 * @param {String} owner The column owning the sequence
 * @param {String} schema Optional schema name
 */
const createSequence = exports.createSequence = (client, name, owner, schema) => {
    const statement = "create sequence if not exists " + getFqn(name, schema) + " owned by " + owner;
    return SqlTemplate.build(statement).execute(client);
};

/**
 * Creates an index for the specified table
 * @param {Client} client The client to use
 * @param {String} table The name of the table to create the index for
 * @param {Object} spec The index specification with the following properties:
 * <ul>
 * <li><i>name</i>: The name of the index (required)</li>
 * <li><i>columns</i>: Array of column names (required)</li>
 * <li><i>unique</i>: Optional boolean</li>
 * <li><i>type</i>: Optional index type supported by PostgreSQL</li>
 * <li><i>tablespace</i>: Optional tablespace name</li>
 * <li><i>predicate</i>: Optional index predicate</li>
 * </ul>
 */
const createIndex = exports.createIndex = (client, table, spec, schema) => {
    const statement = [
        "create", spec.unique === true ? " unique" : "",
        " index if not exists ", spec.name,
        " on ", getFqn(table, schema),
        spec.type ? " using " + spec.type : "",
        " (", spec.columns.join(", "), ")",
        spec.tablespace ? " tablespace " + spec.tablespace : "",
        spec.predicate ? " where " + spec.predicate : ""
    ].join("");
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
 * Returns a list of sequence names owned by a table
 * @param {Client} client The client to use
 * @param {String} table The table name
 * @returns {Array} An array containing objects for each sequence with the
 * properties "schema" and "name"
 */
const getTableSequences = exports.getTableSequences = (client, table) => {
    const columns = SqlTemplate.build("select table_schema as schema, table_name as table, column_name as column from information_schema.columns where table_name = #{table}")
            .execute(client, {"table": table});
    return columns.reduce((sequences, column) => {
        return SqlTemplate.build("select pg_get_serial_sequence(#{table}, #{column}) as name")
                .execute(client, column)
                .reduce((sequences, sequence) => {
                    if (sequence.name !== null) {
                        const [schema, name] = sequence.name.split(".");
                        sequences.push({
                            "schema": !name ? null : schema,
                            "name": !name ? schema : name
                        });
                    }
                    return sequences;
                }, sequences);
    }, []);
};

/**
 * Returns the indexes of a table
 * @param {Client} client The client to use
 * @param {String} table The table name
 * @returns {Array} An array containing objects for each index with the
 * properties "schema", "table" and "name"
 */
const getTableIndexes = exports.getTableIndexes = (client, table) => {
    return SqlTemplate.build("select schemaname as schema, tablename as table, indexname as name from pg_indexes where tablename = #{table}")
            .execute(client, {"table": table});
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
const dropSequence = exports.dropSequence = (client, name, schema) => {
    return SqlTemplate.build("drop sequence if exists " + getFqn(name, schema) + " cascade")
            .execute(client);
};

/**
 * Drops the index with the given name in the optional schema
 * @param {Client} client The client to use
 * @param {String} name The name of the index to drop
 * @param {String} schema Optional schema
 */
const dropIndex = exports.dropIndex = (client, name, schema) => {
    return SqlTemplate.build("drop index if exists " + getFqn(name, schema))
            .execute(client);
};

/**
 * Drops all tables and sequences
 * @param {Client} client The client to use
 * @param {String} schema Optional schema name
 */
const dropAll = exports.dropAll = (client, schema) => {
    getTables(client, schema).forEach(table => {
        dropTable(client, table.name, table.schema);
        getTableSequences(client, table.name).forEach(sequence => {
            dropSequence(client, sequence.name, sequence.schema);
        });
        getTableIndexes(client, table.name).forEach(index => {
            dropIndex(client, index.name, index.schema);
        });
    });
};
