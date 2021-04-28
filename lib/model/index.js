const constants = require("../constants");
const utils = require("./utils");
const mapper = require("../mapper");
const SqlTemplate = require("../sqltemplate");
const database = require("../database");

const mergeModelData = (mapping, props, data) => {
    return Object.keys(mapping.properties).reduce((params, key) => {
        const propMapping = mapping.properties[key];
        params[key] = props.hasOwnProperty(key) ? props[key] : data[propMapping.column];
        return params;
    }, {"id": data[mapping.id.column] || props.id});
};

/**
 * Base model constructor. All models inherit from this.
 * @constructor
 */
const Model = function Model() {};

/**
 * Factory method for creating model constructor functions.
 * @param {Client} client The client
 * @param {String} type The name of the model entity (i.e. the name of the
 * constructor function)
 * @param {Object} mapping The mapping definition specifying the properties
 * @returns {Model} A constructor function for the creation of model instances.
 * @name define
 */
const defineModel = (client, type, mapping) => {

    // This uses explicit getters/setters so it requires an explicit mapping
    if (!mapping || !mapping.properties) {
        throw new Error("Model requires explicit property mapping");
    }

    /**
     * Creates a new Model instance. This constructor must not be called
     * directly, instead define an entity by calling `Client.prototype.defineModel()`
     * which returns a constructor function implementing this Model interface.
     * This constructor function can then be used for creating instances of
     * the defined entity.
     * @class Instances of this class represent a database persistable object
     * modeled after a mapping definition. Model constructors are defined
     * using `Store.prototype.defineEntity()`, which returns a constructor
     * function implementing this Model interface.
     * @param {Object} props Optional object containing the initial property
     * values of the model instance
     * @name Model
     * @constructor
     */
    const ctor = function(props) {
        Object.defineProperties(this, {
            "_state": {
                "value": constants.STATE_NEW,
                "enumerable": false,
                "writable": true
            },
            "_props": {
                "value": props || {},
                "enumerable": false,
                "writable": true
            },
            "_data": {
                "value": {},
                "enumerable": false,
                "writable": true
            }
        });
        return this;
    };

    const sqlTemplates = utils.buildModelTemplates(mapping);

    // define static Model functions
    Object.defineProperties(ctor, {
        /**
         * Contains the model type
         * @name Model.prototype.type
         * @type String
         */
        "type": {"value": type},
        /**
         * Contains the mapping that defines the properties of instances of
         * this constructor function
         * @name Model.prototype.mapping
         * @type {Object}
         */
        "mapping": {"value": mapping},
        /**
         * A resultset map function that produces instances of this model
         * @name Model.prototype.mapTo
         * @type Function
         * @ignore
         */
        "mapTo": {
            "value": mapper.mapToModel(ctor, client)
        },
        /**
         * Returns the model instance with the given id
         * @param {Number} id The ID of the model to return
         * @returns {Model} The model with the given ID
         * @name Model.prototype.get
         * @function
         */
        "get": {
            "value": (id) => {
                const key = utils.getKey(type, id);
                const transaction = client.getTransaction();
                const useCache = !!client.cache && (!transaction || !transaction.containsKey(key));
                if (useCache && client.cache.containsKey(key)) {
                    return ctor.createInstance(client.cache.get(key));
                }
                return SqlTemplate.newInstance(sqlTemplates.get)
                        .setMapper(ctor.mapTo)
                        .execute(client,{"id": id})[0] || null;
            }
        },
        "getMany": {
            "value": (ids) => {
                return SqlTemplate.newInstance(sqlTemplates.getMany)
                        .setMapper(ctor.mapTo)
                        .execute(client,{"ids": ids});
            }
        },
        /**
         * Returns all persisted model instances
         * @returns {Array} An array containing all persisted instances of a model
         * @name Model.prototype.all
         */
        "all": {
            "value": () => {
                return SqlTemplate.newInstance(sqlTemplates.getAll)
                        .setMapper(ctor.mapTo)
                        .execute(client);
            },
            "writable": true
        },
        /**
         * Returns all persisted model instances matching the given `where` clause
         * @param {String} clause The query string clause (`where ...`)
         * @param {Object} params An object containing the parameters referenced in the query clause
         * @name Model.prototype.query
         * @return {Array} An array containing all matching models
         */
        "query": {
            "value": (clause, params) => {
                return SqlTemplate.newInstance(utils.buildQueryTemplate(mapping, clause))
                        .setMapper(ctor.mapTo)
                        .execute(client, params);
            }
        },
        /**
         * Persists an array or models
         * @param {Array} params An array of model data objects
         * @param {Number} batchSize Optional batch size
         * @return {Boolean} True if batch insert succeeded, false otherwise
         */
        "insertBatch": {
            "value": (params, batchSize) => {
                return SqlTemplate.newInstance(sqlTemplates.insert)
                        .executeBatch(client, params, batchSize);
            }
        },
        /**
         * Creates the database table (and sequence(s) if defined for this model) 
         */
        "createTable": {
            "value": () => {
                return database.initModel(client, mapping);
            }
        },
        /** @ignore */
        "toString": {"value": () => "function " + type + "() {}"}
    });

    // property definitions for constructor prototype
    const ctorPropertyDefinitions = {
        /** @ignore */
        "constructor": {
            "value": ctor
        },
        "_key": {
            "get": function() {
                return utils.getKey(type, this.id);
            }
        },
        /**
         * The ID of this model (undefined for transient models)
         * @name Model.prototype.id
         * @type Number
         */
        "id": {
            "get": function() {
                return this._data.hasOwnProperty(mapping.id.column) ?
                        this._data[mapping.id.column] : undefined;
            },
            "enumerable": true
        },
        /**
         * Saves this model in the underlying database. Use this method for
         * both persisting a transient model or for storing modifications
         * in an already persisted model.
         * @name Model.prototype.save
         * @return Model The updated model instance
         * @function
         */
        "save": {
            "value": function() {
                const isNew = this._state === constants.STATE_NEW;
                const sqlTemplate = isNew ? sqlTemplates.insert : sqlTemplates.update;
                const data = this._data = SqlTemplate.newInstance(sqlTemplate)
                        .setMapper(mapper.mapToJson)
                        .execute(client, mergeModelData(mapping, this._props, this._data))[0];
                this._props = {};
                this._state = constants.STATE_CLEAN;
                const key = utils.getKey(type, data[mapping.id.column]);
                const transaction = client.getTransaction();
                if (transaction) {
                    transaction[isNew ? "addInserted" : "addUpdated"](this);
                } else {
                    client.cache && client.cache.put(key, data);
                    client.onCommit(key);
                }
                return this;
            },
            "enumerable": true
        },
        /**
         * Deletes this model instance from the underlying database.
         * @name Model.prototype.delete
         * @return {Boolean} True if this model instance was deleted , false otherwise
         * @function
         */
        "delete": {
            "value": function() {
                if (this._state === constants.STATE_CLEAN || this._state === constants.STATE_DIRTY) {
                    const isDeleted = SqlTemplate.newInstance(sqlTemplates.delete)
                            .execute(client, {"id": this.id}) === 1;
                    if (isDeleted) {
                        this._state = constants.STATE_DELETED;
                        const transaction = client.getTransaction();
                        if (transaction) {
                            transaction.addDeleted(this);
                        } else {
                            const key = this._key;
                            client.cache && client.cache.remove(key);
                            client.onCommit(key);
                        }
                    }
                    return isDeleted;
                }
            },
            "enumerable": true
        },
        /** @ignore */
        "toString": {
            "value": function() {
                return "[" + this._key + "]";
            },
            "writable": true
        },
        /**
         * Returns a JSON representation of this model. Note that this
         * method does not recurse, i.e. the resulting object does not contain
         * mapped objects or collections.
         * @returns Object
         * @name Model.prototype.toJSON
         */
        "toJSON": {
            "value": function() {
                return Object.keys(mapping.properties).reduce((json, key) => {
                        json[key] = this[key];
                        return json;
                    }, {
                        "id": this.id
                    });
            },
            "enumerable": true
        }
    };

    // define getters and setters for all properties defined by mapping
    Object.keys(mapping.properties).forEach(function(key) {
        const propMapping = mapping.properties[key];
        ctorPropertyDefinitions[key] = {
            "get": function() {
                if (this._props.hasOwnProperty(key)) {
                    // modified property or mapped collection/object
                    return this._props[key];
                }
                if (!this._data.hasOwnProperty(propMapping.column)) {
                    return null;
                }
                return this._data[propMapping.column];
            },
            "set": function(value) {
                this._props[key] = value;
                if (this._state === constants.STATE_CLEAN) {
                    this._state = constants.STATE_DIRTY;
                }
            },
            "enumerable": true
        };
    });

    // make it inherit Model
    ctor.prototype = Object.create(Model.prototype, ctorPropertyDefinitions);

    /**
     * Factory function used by the client to instantiate Models using data
     * received from database queries
     * @param {Object} data The model data as received from mapper
     * @returns {Model} A newly instantiated Model
     * @name Model.createInstance
     */
    ctor.createInstance = function(data) {
        return Object.create(ctor.prototype, {
            /**
             * Contains the state of a model instance
             * @type Number
             * @name Model.prototype._state
             * @ignore
             */
            "_state": {
                "value": constants.STATE_CLEAN,
                "enumerable": false,
                "writable": true
            },
            /**
             * An internal map containing the properties of this model
             * @type Object
             * @name Model.prototype._props
             * @ignore
             */
            "_props": {
                "value": {},
                "enumerable": false,
                "writable": true
            },
            /**
             * An internal map containing the column names as property names
             * and the values stored in the database row as values.
             * @type Object
             * @name Model.prototype._data
             * @ignore
             */
            "_data": {
                "value": data,
                "enumerable": false,
                "writable": true
            }
        });
    };

    return ctor;
};

module.exports = {
    "define": defineModel
};