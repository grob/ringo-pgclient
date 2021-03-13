require("ringo/logging").setConfig(getResource("./log4j2.properties"));

const system = require("system");
const assert = require("assert");
const Client = require("../lib/client");
const Cache = require("../lib/cache");
const helpers = require("./helpers");
const database = require("../lib/database");

let client = null;

exports.setUp = () => {
    client = new Client(helpers.initPool(), new Cache(10));
};

exports.tearDown = () => {
    database.dropAll(client);
    client && client.close();
};

exports.testQuery = () => {
    const Author = client.defineModel("Author", {
        "table": "t_author",
        "id": {
            "column": "aut_id",
            "type": "int8",
            "sequence": "author_id"
        },
        "properties": {
            "name": {
                "column": "aut_name",
                "type": "varchar",
                "constraint": "not null"
            }
        }
    });
    database.initModel(client, Author.mapping);
    const author = new Author({"name": "Jane Foo"});
    author.save();
    client.cache.clear();
    
    // default json mapper - ignores the client cache
    const query = "select * from t_author where aut_id = #{id}";
    const params = {"id": author.id};
    let result = client.query(query, params);
    assert.isTrue(Array.isArray(result));
    assert.strictEqual(result.length, 1);
    assert.deepEqual(result[0], author._data);

    // model mapper - utilizes the client cache
    assert.isFalse(client.cache.containsKey(author._key));
    result = client.query(query, params, client.mapToModel(Author));
    assert.strictEqual(result.length, 1);
    assert.isTrue(result[0] instanceof Author);
    assert.deepEqual(result[0]._data, author._data);
    // author retrieved must be in cache
    assert.isTrue(client.cache.containsKey(result[0]._key));
};

//start the test runner if we're called directly from command line
if (require.main == module.id) {
    system.exit(require("test").run.apply(null,
            [exports].concat(system.args.slice(1))));
}
