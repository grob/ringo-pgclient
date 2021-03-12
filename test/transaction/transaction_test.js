const assert = require("assert");
const {Worker} = require("ringo/worker");
const {Semaphore} = require("ringo/concurrent");
const system = require("system");
const helpers = require("../helpers");
const Client = require("../../lib/client");
const Cache = require("../../lib/cache");
const Transaction = require("../../lib/transaction");
const database = require("../../lib/database");
const constants = require("../../lib/constants");

let client = null;
let Author = null;
let Book = null;

exports.setUp = () => {
    client = new Client(helpers.initPool(), new Cache(10));
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
                "type": "varchar",
                "constraint": "not null"
            }
        }
    });
    database.initModel(client, Author.mapping);
};

exports.tearDown = () => {
    database.dropAll(client);
    client && client.close();
    Author = null;
};

exports.testCommit = function() {
    assert.strictEqual(Author.all().length, 0);
    let transaction = client.beginTransaction();
    const authors = [];
    // insert some test objects
    for (let i=0; i<5; i+=1) {
        let author = new Author({
            "name": "Author " + (i + 1)
        });
        author.save();
        authors.push(author);
    }
    assert.strictEqual(Object.keys(transaction.inserted).length, authors.length);
    assert.strictEqual(Author.all().length, 5);
    authors.forEach(author => assert.isNotNull(Author.get(author.id)));
    client.commitTransaction();
    assert.isFalse(client.hasTransaction());
    assert.strictEqual(Author.all().length, 5);
    assert.strictEqual(Object.keys(transaction.inserted).length, 0);

    // remove test objects
    client.beginTransaction();
    transaction = client.getTransaction();
    Author.all().forEach((author) => author.delete());
    assert.strictEqual(Object.keys(transaction.deleted).length, 5);
    assert.strictEqual(Author.all().length, 0);
    client.commitTransaction();
    assert.isNull(client.getTransaction());
};

exports.testRollback = () => {
    assert.strictEqual(Author.all().length, 0);
    const transaction = client.beginTransaction();
    const author = new Author({"name": "Author"});
    author.save();
    assert.strictEqual(Object.keys(transaction.inserted).length, 1);
    client.abortTransaction();
    assert.strictEqual(Object.keys(transaction.inserted).length, 0);
    assert.isFalse(client.hasTransaction());
    assert.strictEqual(Object.keys(transaction.inserted).length, 0);
};

exports.testMultipleModifications = () => {
    let author = new Author({
        "name": "John Doe"
    });
    author.save();
    client.beginTransaction();
    // step 1: modify author and save it, but don't commit the transaction
    author = Author.get(1);
    author.name = "Jane Foo";
    author.save();
    // step 2: modify author again, this time committing the transaction
    author.name = "John Doe";
    author.save();
    client.commitTransaction();
    assert.strictEqual(author.name, "John Doe");
    author = Author.get(1);
    assert.strictEqual(author.name, "John Doe");
};

exports.testConcurrentInserts = () => {
    const nrOfWorkers = 10;
    const cnt = 10;
    const semaphore = new Semaphore();

    for (let i=0; i<nrOfWorkers; i+=1) {
        let w = new Worker(module.resolve("./worker"));
        w.onmessage = () => {
            semaphore.signal();
        };
        w.postMessage({
            "workerNr": i,
            "cnt": cnt,
            "Author": Author
        }, true);
    }
    semaphore.tryWait(1000, nrOfWorkers);
    assert.strictEqual(Author.all().length, cnt * nrOfWorkers);
};

exports.testInsertIsolation = () => {
    client.beginTransaction();
    const author = new Author({
        "name": "John Doe"
    });
    author.save();
    assert.isNotNull(Author.get(1));
    // the above is not visible for other threads
    assert.isNull(spawn(() => {
        assert.isNull(Transaction.getInstance());
        return Author.get(1);
    }).get());
    // nor is the storable's _entity in cache
    assert.isFalse(client.cache.containsKey(author._key));
    // even after re-getting the storable its _entity isn't cached
    assert.isNotNull(Author.get(1));
    assert.isFalse(client.cache.containsKey(author._key));
    // same happens when querying for the newly created author instance
    assert.strictEqual(Author.query("aut_id = #{id}", {"id": author.id})[0].id, 1);
    assert.isFalse(client.cache.containsKey(author._key));
    client.commitTransaction();
    // after commit the storable is visible and it's _entity cached
    assert.isTrue(client.cache.containsKey(author._key));
    assert.isTrue(author._key.equals(spawn(() => {
        return Author.get(1)._key;
    }).get()));
};

exports.testUpdateIsolation = () => {
    const author = new Author({
        "name": "John Doe"
    });
    author.save();
    assert.isTrue(client.cache.containsKey(author._key));
    client.beginTransaction();
    author.name = "Jane Foo";
    author.save();
    // the above is not visible for other threads
    assert.strictEqual(spawn(() => {
        assert.isNull(Transaction.getInstance());
        return Author.get(1).name;
    }).get(), "John Doe");
    // nor is the change above in cache
    assert.strictEqual(client.cache.get(author._key).aut_name, "John Doe");
    // even after re-getting the storable its _entity isn't cached
    assert.strictEqual(Author.get(1).name, "Jane Foo");
    assert.strictEqual(client.cache.get(author._key).aut_name, "John Doe");
    // same happens when querying for the newly created author instance
    assert.strictEqual(Author.query("aut_id = #{id}", {"id": author.id})[0].name, "Jane Foo");
    assert.strictEqual(client.cache.get(author._key).aut_name, "John Doe");
    client.commitTransaction();
    // after commit the storable is visible and it's _entity cached
    assert.strictEqual(client.cache.get(author._key).aut_name, "Jane Foo");
    assert.strictEqual(spawn(() => {
        return Author.get(1).name;
    }).get(), "Jane Foo");
};

exports.testRemoveIsolation = () => {
    const author = new Author({
        "name": "John Doe"
    });
    author.save();
    client.beginTransaction();
    author.delete();
    // the above is not visible for other threads
    assert.isNotNull(spawn(() => {
        assert.isNull(Transaction.getInstance());
        return Author.get(1);
    }).get());
    // nor is the change above in cache
    assert.isTrue(client.cache.containsKey(author._key));
    client.commitTransaction();
    // after commit the storable is gone from cache and for other threads too
    assert.isFalse(client.cache.containsKey(author._key));
    assert.isNull(spawn(() => {
        return Author.get(1);
    }).get());
};

exports.testCommitEvent = () => {
    let mods = null;
    client.addListener("commit", (data) => {
        mods = data;
    });
    const author = new Author({
        "name": "John Doe"
    });
    // inserted
    assert.isNotNull(mods);
    assert.isTrue(mods.inserted.hasOwnProperty(author._key));
    // updated
    author.name = "Jane Foo";
    author.save();
    assert.isTrue(mods.updated.hasOwnProperty(author._key));
    // clear the entity cache
    client.cache.clear();
    client.beginTransaction();
    Author.get(1).delete();
    client.commitTransaction();
    assert.isTrue(mods.deleted.hasOwnProperty(author._key));
};

//start the test runner if we're called directly from command line
if (require.main == module.id) {
    system.exit(require("test").run.apply(null,
            [exports].concat(system.args.slice(1))));
}
