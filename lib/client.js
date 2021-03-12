const SqlTemplate = require("./sqltemplate");
const Transaction = require("./transaction");
const Model = require("./model");
const {EventEmitter} = require("ringo/events");
const mapper = require("./mapper");

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
 * @this Client
 */
Client.prototype.close = function() {
    this.connectionPool.close();
};

/**
 * Returns a database connection object.
 * @returns {java.sql.Connection} A database connection object
 * @this Client
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
 * Aborts (i.e. rolls back) the transaction bound to the current thread and
 * closes it.
 */
Client.prototype.abortTransaction = function() {
    const transaction = Transaction.getInstance();
    if (transaction == null) {
        throw new Error("No open transaction to abort");
    }
    transaction.rollback();
};

Client.prototype.defineModel = function(type, mapping) {
    return Model.define(this, type, mapping);
};

Client.prototype.query = function(template, params, mappingFunc) {
    return SqlTemplate.build(template)
            .setMapper(mappingFunc || mapper.mapToJson)
            .execute(this, params);
};
