/**
 * @fileoverview Database connection pool initialization
 * @module pool
 * @example
 * const newPool = require("ringo-pgclient/lib/pool");
 *
 * module.exports = module.singleton("connection-pool", () => newPool({
 *     "url": "jdbc:postgresql://localhost/test",
 *     "user": "test",
 *     "password": "test",
 *     "maximumPoolSize": 10
 * }));
 */

require("./utils").loadJars();

/**
 * Returns a newly instantiated connection pool using the properties
 * passed as argument
 * @param {Object} props The database properties
 * @returns {Pool} The connection pool
 * @function
 */
module.exports = (props) => {
    const config = new com.zaxxer.hikari.HikariConfig();
    config.setDriverClassName(props.driver || "org.postgresql.Driver");
    config.setJdbcUrl(props.url);
    config.setUsername(props.user);
    config.setPassword(props.password);
    Object.keys(props).forEach(key => {
        if (!["driver", "url", "user", "password"].includes(key)) {
            config[key] = props[key];
        }
    });
    return new com.zaxxer.hikari.HikariDataSource(config);
};