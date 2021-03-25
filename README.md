# About

`ringo-pgclient` is a [RingoJS](https://ringojs.org) client for PostgreSQL databases. It's inspired by [vert.x sql client templates](https://vertx.io/docs/vertx-sql-client-templates/java/) and features an orm layer similar to [ringo-sqlstore](https://github.com/grob/ringo-sqlstore) (see [below](#differences-to-ringo-sqlstore) for details on how this client differs from ringo-sqlstore).

# Installation

If you have the RingoJS package manager `rp` installed, simply install `ringo-pgclient` by executing the following command:

```
rp install ringo-pgclient
```

Otherwise you can clone this repository an put it into the package directory of your application (you might need to add `-m <path-to-package-directory>` to the ringo command line).

Whichever way you choose, you need to download the [PostgreSQL JDBC client](https://jdbc.postgresql.org/) that matches your database and put it into the directory `<path-to-ringo-pgclient>/jars/`.

# Usage

## Database Connection Pool

The first thing you'll need is a database connection pool. `ringo-pgclient` has a `newPool(options)` function for that:

```
const pg = require("ringo-pgclient");

const pool = pg.newPool({
    "url": "jdbc:postgresql://localhost/test",
    "user": "test",
    "password": "test"
});
```

`ringo-pgclient` uses [HikariCP](https://github.com/brettwooldridge/HikariCP) as connection pool, so you can pass any options supported by HikariCP (you'll most probably want to configure the size of the pool with `maximumPoolSize` and `minimumIdle` options).

Important: since RingoJS is multithreaded, it's best to store the connection pool in a singleton, so i.e. create a module `connectionpool.js` in your application with the following content:

```
const pg = require("ringo-pgclient");

module.exports = module.singletion("connectionpool", () => {
    return pg.newPool({
        "url": "jdbc:postgresql://localhost/test",
        "user": "test",
        "password": "test"
    });
});

```

## Database Client

The only thing needed to instantiate a database client is the connection pool described above:

```
const pg = require("ringo-pgclient");
const pool = require("./connectionpool.js")
const client = new pg.Client(pool);
```

You should **not** store the client in a singleton, as this most probably will lead to exceptions ("Current thread worker differs from scope worker"), instead simply instantiate the client for each thread (the connection pool and model data cache however should be stored in a singleton)

## Querying and Row Mapping

Once you have instantiated a client you can start using it for all database operations needed:

```
// create table
client.query("create table if not exists t_author (aut_id bigint primary key, aut_name varchar not null)");

// create sequence
client.query("create sequence author_id owned by t_author.aut_id");

// insert row
client.query("insert into t_author values (nextval('author_id'), 'Jane Doe')");
```

By default query result sets are mapped to arrays containing objects for each row. The property names of these objects are the column names, unless they are aliased in the sql query: 

```
// query without aliasing
client.query("select * from t_author");
// results in: [{aut_id: 1, aut_name: 'Jane Doe'}]

// query with column aliases
client.query("select aut_id as id, aut_name as name from t_author");
// results in: [{id: 1, name: 'Jane Doe'}] 
```

### Query parameters

Under the hood `ringo-pgclient` **always** uses JDBC prepared statements. To pass parameters to a query use the placeholder syntax `#{<parameter-name>}` and pass an object as second argument to the query method:

```
client.query("select * from t_author where aut_id = #{id}", {
    "id": 1
});

// same with insert...
client.query("insert into t_author values (nextval('author_id'), #{name})", {
    "name": "John Foo"
});

// or update...
client.query("update t_author set aut_name = #{name} where aut_id = #{id}", {
    "id": 2,
    "name": "Jane Foo"
});
```

### Custom row mapping

The `query()` method accepts a custom row mapper function as third argument. This function receives the JDBC result set instance as sole argument. If you're using a custom row mapper make sure to close the result set.

## Data types

`ringo-pgclient` currently supports nearly all data types of PostgreSQL except:

- cidr
- inet
- macaddr
- macaddr8
- money
- uuid
- xml

You can also use data type aliases supported by PostgreSQL, e.g. `bigint` instead of `int8`.

## Transactions

`ringo-pgclient` supports transactions too. When calling `client.beginTransaction()` a transaction is created and bound to the current thread, so it's not necessary to pass the transaction object to every client or model method used within the transaction:

```
client.beginTransaction()
...
client.commitTransaction()
// or client.abortTransaction()

// check if a transaction has been opened for the current thread:
client.hasTransaction()
```

## Model mappings

Similar to [ringo-sqlstore](https://github.com/grob/ringo-sqlstore) `ringo-pgclient` features a thin model mapping. To use it you have to define a model first:

```
const Author = client.defineModel("Author", {
    "table": "t_author",
    "id": {
        "column": "aut_id",
        "type": "bigint", // use any psql data type here
        "constraint": "primary key", // and any column constraint
        "sequence": "author_id" // define a sequence to use
    },
    "properties": {
        "name": {
            "column": "aut_name",
            "type": "varchar(200)",
            "constraint": "not null"
        }
    }
});
```

As you can see, defining a model is pretty straight forwardd: define the table, the model id (with a sequence if desired) and properties to map. Once you have the model defined, you will probably want to create the table and - if defined - the id sequence of your model

```
// creates the table/sequence of this model
Author.createTable();
```

Note that this method **does nothing if the table or sequence already exist**, any alterations of the table/sequence used must be done manually.

After that you can start creating instances, persisting them in the database and querying them:

```
let author = new Author({"name": "Frank Testa"});
author.save();

// retrieve author instance
author = Author.get(1);

// update
author.name = "New Author";
author.save();

// delete
author.delete();
```

Querying for Model instances is simple: just call the static model method `query()`, optionally passing an sql `where` clause:

```
Author.query("where aut_name like #{name}", {
    "name": "%rank%"
});

// retrieve all instances
Author.query()

// which is similar to
Author.all()
```

### Model data cache

Depending on your application models and their use it might boost performance to use a model data LRU cache. This cache is used transparently and can reduce database roundtrips or row-to-model conversion. To configure the model cache instantiate it and pass it to the client constructor:

```
const pg = require("ringo-pgclient");
const cache = pg.newCache(1000);
const client = new pg.Client(pool, cache);
```

The static method `Model.get(id)` does a cache lookup before querying the database, and - in case of a cache miss - inserts the data object received from database into the cache:

```
Author.get(1); // cache miss, inserts the data object into the cache
Author.get(1); // cache hit, completely skips the database roundtrip
```

When using `Model.query()` or `Model.all()` the query is still sent to the database, but when converting the result set rows the cache is queried and populated.

## Event emitting

Every database client instance is also a event emitter, so application code can register a listener to be notified whenever models are inserted, updated or deleted. The event callback receives an array of Model keys (Strings in the form of `<Model>#<id>`). You can e.g. use these keys to invalidate caches of clustered RingoJS applications.

```
client.addListener("commit", (keys) => {
    // do something with the model keys, i.e. send them over the network
});
```

## Differences to ringo-sqlstore

The main differences are that `ringo-pgclient`

- is limited to PostgreSQL only
- doesn't support relational mappings (object- or collection mappings)
- uses plain SQL statements instead of a custom sql query dialect
- has a lesser cache-efficiency due to the fact that query results are not cached (only the model data objects received from database)

