/**
 * @fileoverview Transaction implementation, keeping track of modifications
 * within a transaction and providing functionality for committing or rolling back.
 */
const constants = require("./constants");

/**
 * Returns a newly created Transaction instance
 * @class Instances of this class represent a database transaction, holding
 * information about inserted, updated and deleted objects and methods
 * to commit or rollback the transaction
 * @returns A newly created Transaction instance
 */
const Transaction = module.exports = function(client) {
    let connection = null;
    let inserted = {};
    let updated = {};
    let deleted = {};
    let keys = [];

    Object.defineProperties(this, {
        /**
         * The client
         * @type {Client}
         */
        "client": {"value": client},

        /**
         * Contains the keys of inserted objects
         * @type {Array}
         */
        "inserted": {
            "get": () => inserted
        },

        /**
         * Contains the keys of updated objects
         * @type {Array}
         */
        "updated": {
            "get": () => updated
        },

        /**
         * Contains the keys of deleted objects
         * @type {Array}
         */
        "deleted": {
            "get": () => deleted
        },

        /**
         * Contains the list of keys of all objects modified in this transaction
         * @type {Array}
         */
        "keys": {"value": keys}
    });

    /**
     * Resets this transaction.
     * @private
     * @ignore
     */
    const reset = function() {
        if (connection !== null) {
            connection.close();
            connection = null;
        }
        inserted = {};
        updated = {};
        deleted = {};
        keys.length = 0;
    };

    /**
     * Returns the connection of this transaction
     * @returns {java.sql.Connection} The connection
     */
    this.getConnection = function() {
        if (connection === null) {
            connection = client.connectionPool.getConnection();
            connection.setTransactionIsolation(java.sql.Connection.TRANSACTION_READ_COMMITTED);
            connection.setReadOnly(false);
            connection.setAutoCommit(false);
        }
        return connection;
    };
    
    /**
     * Commits all changes made in this transaction, and releases the connection
     * used by this transaction. This method must not be called directly, instead
     * use `Client.prototype.commitTransaction()`.
     * @see client#Client.prototype.commitTransaction
     */
    this.commit = function() {
        connection.commit();
        Transaction.removeInstance();
        const hasEntityCache = client.cache !== null;
        [inserted, updated].forEach((map) => {
            Object.keys(map).forEach((key) => {
                client.cache && client.cache.put(key, map[key]._data);
            });
        });
        Object.keys(deleted).forEach((key) => {
            client.cache && client.cache.remove(key);
        });
        if (client.listeners("commit").length > 0) {
            client.emit("commit", {
                "inserted": inserted,
                "updated": updated,
                "deleted": deleted
            });
        }
        reset();
    };

    /**
     * Rolls back all changes made in this transaction, and releases the
     * connection used by this transaction. This method must not be called
     * directly, instead use `Client.prototype.abortTransaction()`.
     * @see Client#Client.prototype.abortTransaction
     */
    this.rollback = function() {
        this.getConnection().rollback();
        Transaction.removeInstance();
        Object.keys(inserted).forEach((key) => {
            inserted[key]._state = constants.STATE_NEW;
        });
        Object.keys(updated).forEach((key) => {
            updated[key]._state = constants.STATE_DIRTY;
        });
        Object.keys(deleted).forEach((key) => {
            deleted[key]._state = constants.STATE_CLEAN;
        });
        reset();
    };

    return this;
};

/**
 * A static property containing the ThreadLocal instance used to bind
 * transactions to threads
 * @type java.lang.ThreadLocal
 * @name Transaction.threadLocal
 * @ignore
 */
Object.defineProperty(Transaction, "threadLocal", {
    "value": new java.lang.ThreadLocal()
});

/**
 * Creates a new Transaction and binds it to the local thread
 * @param {Client} client The client to use
 * @returns {Transaction} The Transaction instance
 * @name Transaction.createInstance
 * @ignore
 */
Transaction.createInstance = function(client) {
    let transaction = Transaction.threadLocal.get();
    if (transaction === null) {
        transaction = new Transaction(client);
        Transaction.threadLocal.set(transaction);
    }
    return transaction;
};

/**
 * Returns the transaction instance bound to the calling thread.
 * @returns {Transaction} The transaction, or null if none has been initialized
 * @name Transaction.getInstance
 * @ignore
 */
Transaction.getInstance = function() {
    return Transaction.threadLocal.get();
};

/**
 * Removes the transaction bound to the calling thread
 * @name Transaction.removeInstance
 * @ignore
 */
Transaction.removeInstance = function() {
    const transaction = Transaction.getInstance();
    if (transaction !== null) {
        Transaction.threadLocal.remove();
    }
};

/**
 * @ignore
 */
Transaction.prototype.toString = function() {
    return "[Transaction (" + Object.keys(this.inserted).length + " inserted, " +
            Object.keys(this.updated).length + " updated, " +
            Object.keys(this.deleted).length + " deleted)]";
};

/**
 * Helper method for adding a key and an object to the map passed as argument.
 * @param {Object} map The map to add to
 * @param {Key} key The key
 * @param {Object} obj The object value
 * @ignore
 */
Transaction.prototype.add = function(map, key, obj) {
    map[key] = obj;
    if (!this.keys.includes(key)) {
        this.keys.push(key);
    }
};

/**
 * Adds the model to the list of inserted ones
 * @param {Model} model The model to register
 * @ignore
 */
Transaction.prototype.addInserted = function(model) {
    return this.add(this.inserted, model._key, model);
};

/**
 * Adds the model to the list of updated ones
 * @param {Model} model The model to register
 * @ignore
 */
Transaction.prototype.addUpdated = function(model) {
    return this.add(this.updated, model._key, model);
};

/**
 * Adds the model to the list of deleted ones
 * @param {Model} model The model to register
 * @ignore
 */
Transaction.prototype.addDeleted = function(model) {
    return this.add(this.deleted, model._key, model);
};

/**
 * Returns true if this transaction contains the key passed as argument
 * @param {String} key The key
 * @returns {Boolean} True if this transaction contains the key
 * @ignore
 */
Transaction.prototype.containsKey = function(key) {
    return this.keys.indexOf(key) > -1;
};
