require("ringo/logging").setConfig(getResource("./log4j2.properties"));

const assert = require("assert");
const system = require("system");
const helpers = require("../helpers");
const Client = require("../../lib/client");
const database = require("../../lib/database");
const constants = require("../../lib/constants");

let client = null;
let Author = null;

exports.setUp = () => {
    client = new Client(helpers.initPool());
    Author = client.defineModel("Author", {
        "table": "t_author",
        "id": {
            "column": "aut_id",
            "type": "int8",
            "sequence": "author_id"
        },
        "properties": {
            "name": {
                "column": "aut_name",
                "type": "varchar"
            }
        }
    });
    Author.createTable();
};

exports.tearDown = () => {
    database.dropAll(client);
    client && client.close();
    Author = null;
};

exports.testCRUD = () => {
    assert.strictEqual(Author.all().length, 0);
    let author = new Author({"name": "John Doe"});
    assert.isUndefined(author.id);
    assert.strictEqual(author._state, constants.STATE_NEW);
    author.save();
    assert.strictEqual(Author.all().length, 1);
    assert.isNotNull(author.id);
    assert.isNotNull(author.name);
    assert.strictEqual(author._state, constants.STATE_CLEAN);
    author = Author.get(author.id);
    const name = "Jane Foo";
    author.name = name;
    assert.strictEqual(author._state, constants.STATE_DIRTY);
    author.save();
    assert.strictEqual(Author.all().length, 1);
    assert.strictEqual(author._state, constants.STATE_CLEAN);
    author = Author.get(author.id);
    assert.strictEqual(author.name, name);
    author.delete();
    assert.strictEqual(Author.all().length, 0);
    assert.strictEqual(author._state, constants.STATE_DELETED);
};

exports.testNullProps = () => {
    let author = new Author();
    assert.isNull(author.name);
    author.save();
    author = Author.get(1);
    assert.isNull(author.name);
};

exports.testQuery = () => {
    const author = new Author({"name": "Jane Foo"});
    author.save();
    let result = Author.query();
    assert.isTrue(Array.isArray(result));
    assert.strictEqual(result.length, 1);
    assert.isTrue(result[0] instanceof Author);
    assert.deepEqual(result[0]._data, author._data);
    result = Author.query("where aut_name like #{name}", {"name": "Jane%"});
    assert.strictEqual(result.length, 1);
    assert.isTrue(result[0] instanceof Author);
    assert.deepEqual(result[0]._data, author._data);
    result = Author.query("where aut_id = #{id}", {"id": 2});
    assert.strictEqual(result.length, 0);
};

exports.testInsertBatch = () => {
    const params = [
        {"name": "Jane Foo"},
        {"name": "John Doe"},
        {"name": "No Body"}
    ];
    assert.isTrue(Author.insertBatch(params));
    assert.strictEqual(Author.all().length, 3);
    assert.isTrue(Author.insertBatch(params, 2));
    assert.strictEqual(Author.all().length, 6);
};

//start the test runner if we're called directly from command line
if (require.main == module.id) {
    system.exit(require("test").run.apply(null,
            [exports].concat(system.args.slice(1))));
}
