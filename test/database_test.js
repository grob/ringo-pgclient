require("ringo/logging").setConfig(getResource("./log4j2.properties"));

const assert = require("assert");
const system = require("system");
const helpers = require("./helpers");
const Client = require("../lib/client");
const database = require("../lib/database");

let client = null;
let Author = null;

exports.setUp = () => {
    client = new Client(helpers.initPool());
    Author = client.defineModel("Author", {
        "table": "t_author",
        "id": {
            "column": "aut_id",
            "type": "int8",
            "sequence": "author_id",
            "constraint": "primary key"
        },
        "properties": {
            "name": {
                "column": "aut_name",
                "type": "varchar",
                "constraint": "not null"
            },
            "vectors": {
                "column": "aut_vectors",
                "type": "tsvector"
            }
        },
        "indexes": [
            {
                "name": "author_id_name",
                "columns": ["aut_id", "aut_name"],
                "unique": true
            },
            {
                "name": "author_vectors",
                "columns": ["aut_vectors"],
                "type": "gin"
            }
        ]
    });
};

exports.tearDown = () => {
    database.dropAll(client);
    client && client.close();
};

exports.testGetTables = () => {
    database.initModel(client, Author.mapping);
    const tables = database.getTables(client);
    assert.strictEqual(tables.length, 1);
    assert.strictEqual(tables[0].name, Author.mapping.table);
};

exports.testGetTableSequences = () => {
    database.initModel(client, Author.mapping);
    const sequences = database.getTableSequences(client, Author.mapping.table);
    assert.strictEqual(sequences.length, 1);
    assert.strictEqual(sequences[0].name, Author.mapping.id.sequence);
};

exports.testGetTableIndexes = () => {
    database.initModel(client, Author.mapping);
    const indexes = database.getTableIndexes(client, Author.mapping.table);
    assert.strictEqual(indexes.length, 3);
    assert.strictEqual(indexes[0].name, Author.mapping.table + "_pkey");
    assert.strictEqual(indexes[0].table, Author.mapping.table);
    assert.isTrue(Author.mapping.indexes.map(index => index.name).includes(indexes[1].name));
    assert.isTrue(Author.mapping.indexes.map(index => index.name).includes(indexes[2].name));
};

exports.testDropIndex = () => {
    database.initModel(client, Author.mapping);
    const indexes = database.getTableIndexes(client, Author.mapping.table);
    assert.strictEqual(indexes.length, 3);
    Author.mapping.indexes.forEach((spec, idx) => {
        database.dropIndex(client, spec.name);
        assert.strictEqual(database.getTableIndexes(client, Author.mapping.table).length,
                indexes.length - idx - 1);
    });
};

exports.testDropSequence = () => {
    database.initModel(client, Author.mapping);
    assert.strictEqual(database.getTableSequences(client, Author.mapping.table).length, 1);
    database.dropSequence(client, Author.mapping.id.sequence);
    assert.strictEqual(database.getTableSequences(client, Author.mapping.table).length, 0);
};

exports.testDropTable = () => {
    database.initModel(client, Author.mapping);
    assert.strictEqual(database.getTables(client).length, 1);
    database.dropTable(client, Author.mapping.table);
    assert.strictEqual(database.getTables(client).length, 0);
};

exports.testInitModel = () => {
    assert.strictEqual(database.getTableSequences(client, Author.mapping.table).length, 0);
    assert.strictEqual(database.getTableIndexes(client, Author.mapping.table).length, 0);
    assert.strictEqual(database.getTables(client).length, 0);
    database.initModel(client, Author.mapping);
    assert.strictEqual(database.getTableSequences(client, Author.mapping.table).length, 1);
    assert.strictEqual(database.getTableIndexes(client, Author.mapping.table).length, 3);
    assert.strictEqual(database.getTables(client).length, 1);
};

exports.testDropModel = () => {
    database.initModel(client, Author.mapping);
    assert.strictEqual(database.getTableSequences(client, Author.mapping.table).length, 1);
    assert.strictEqual(database.getTableIndexes(client, Author.mapping.table).length, 3);
    assert.strictEqual(database.getTables(client).length, 1);
    database.dropModel(client, Author.mapping);
    assert.strictEqual(database.getTableSequences(client, Author.mapping.table).length, 0);
    assert.strictEqual(database.getTableIndexes(client, Author.mapping.table).length, 0);
    assert.strictEqual(database.getTables(client).length, 0);
};

//start the test runner if we're called directly from command line
if (require.main == module.id) {
    system.exit(require("test").run.apply(null,
            [exports].concat(system.args.slice(1))));
}
