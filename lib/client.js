const SqlTemplate = require("./sqltemplate");
const Transaction = require("./transaction");
const Model = require("./model");
const {EventEmitter} = require("ringo/events");
const mapper = require("./mapper");

/**
 * Client constructor
 * @params {Pool} connectionPool The connection pool to use
 * @params {Cache} cache Optional model data cache
 * @returns {Client} A new Client instance
 * @constructor
 */
const Client = module.exports = function(connectionPool, cache) {

    EventEmitter.call(this);

    Object.defineProperties(this, {
        /**
         * Contains the connection pool of this store
         * @name Client#connectionPool
         * @type {HikariDataSource}
         * @ignore
         * @readonly
         */
        "connectionPool": {
            "value": connectionPool,
            "enumerable": true
        },
        /**
         * Contains the cache of this client
         * @name Client#cache
         * @type Cache
         * @ignore
         * @readonly
         */
        "cache": {
            "value": cache || null,
            "enumerable": true
        }
    });

    return this;
};

/** @ignore */
Client.prototype.toString = function() {
    return "[PSQL Client]";
};

/**
 * Closes all open connections to the database and clears all caches.
 */
Client.prototype.close = function() {
    this.connectionPool.close();
};

/**
 * Returns a database connection. If the thread has an open connection, this
 * method always returns the connection used by it, otherwise it returns a
 * connection from the pool.
 * @returns {java.sql.Connection} A database connection object
 */
Client.prototype.getConnection = function() {
    return (Transaction.getInstance() || this.connectionPool).getConnection();
};

/**
 * Starts a new transaction. Note that the transaction is bound to the thread,
 * so any SQL query issued during an open transaction is using the same
 * database connection.
 * @returns {Transaction} The newly opened transaction
 */
Client.prototype.beginTransaction = function() {
    return Transaction.createInstance(this);
};

/**
 * Returns the current transaction, or null if none has been opened.
 * @returns {Transaction} The current transaction
 */
Client.prototype.getTransaction = function() {
    return Transaction.getInstance();
};

/**
 * Returns true if there is a transaction bound to the current thread.
 * @returns {Boolean} True if a transaction is bound to the current thread
 */
Client.prototype.hasTransaction = function() {
    return Transaction.getInstance() !== null;
};

/**
 * Commits the transaction bound to the current thread and closes it.
 */
Client.prototype.commitTransaction = function() {
    const transaction = Transaction.getInstance();
    if (transaction == null) {
        throw new Error("No open transaction to commit");
    }
    transaction.commit();
};

/**
 * Aborts (i.e. rolls back) the transaction bound to the current thread and closes it.
 */
Client.prototype.abortTransaction = function() {
    const transaction = Transaction.getInstance();
    if (transaction == null) {
        throw new Error("No open transaction to abort");
    }
    transaction.rollback();
};

/**
 * Defines a model with the specified type and mapping
 * @param {String} type The model type
 * @param {Object} mapping The model mapping
 * @return {Function} The model constructor function 
 */
Client.prototype.defineModel = function(type, mapping) {
    return Model.define(this, type, mapping);
};

/**
 * Issue a query against the underlying database
 * @param {String} sql The SQL query, optionally containing parameter placeholders ("#{<parameter-name>}")
 * @param {Object} params The query parameters. Required if the query contains parameter placeholders
 * @param {Function} mapper Optional mapper function. If not specified the default JSON mapper is used
 * @returns {Array} An array containing query result objects (one for each result row)
 * @see Client.prototype.mapToModel
 */
Client.prototype.query = function(sql, params, mapper) {
    const sqlTemplate = SqlTemplate.build(sql);
    if (typeof(mapper) === "function") {
        sqlTemplate.setMapper(mapper);
    }
    return sqlTemplate.execute(this, params);
};

/**
 * Returns a function mapping a query result row to the specified Model. If a
 * cache is configured for this client, the mapper will utilize it.
 * @param {Function} Model The model constructor function
 * @returns {Function} The mapper function
 */
Client.prototype.mapToModel = function(Model) {
    return mapper.mapToModel(Model, this);
};

/**
 * Emits a "commit" event with the given model keys if there are any listeners registered
 * @param {Array} keys The keys to emit as "commit" event
 */
Client.prototype.onCommit = function(keys) {
    if (this.listeners("commit").length > 0) {
        this.emit("commit", !Array.isArray(keys) ? [keys] : keys);
    }
};
