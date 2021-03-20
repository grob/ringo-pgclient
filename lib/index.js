/**
 * Instantiates a new model data cache. Instances should be stored
 * in a singleton, since they are used by multiple clients/threads.
 * @name newCache
 * @function
 * @example
 * const pgClient = require("ringo-pgclient");
 * 
 * module.exports = module.singleton("model-cache", () => {
 *     return pgClient.newCache(1000));
 * });
 */
exports.newCache = require("./cache");

/**
 * Instantiates a new database connection pool. Instances should be stored
 * in a singleton, since they are used by multiple clients/threads.
 * @name newPool
 * @function
 * @example
 * const pgClient = require("ringo-pgclient");
 *
 * module.exports = module.singleton("connection-pool", () => {
 *     return pgClient.newPool({
 *         "url": "jdbc:postgresql://localhost/test",
 *         "user": "test",
 *         "password": "test",
 *         "maximumPoolSize": 10
 *     });
 * });
 */
exports.newPool = require("./pool");

/**
 * The Client constructor
 * @param {Pool} connectionPool The connection pool to use
 * @param {Cache} cache Optional model data cache
 * @name Client
 * @constructor
 * @example
 * const {Client} = require("ringo-pgclient");
 * const connectionPool = require("./connectionpool");
 * const client = new Client(connectionPool);
 * @see <a href="../client/index.html">Client</a>
 */
exports.Client = require("./client");
