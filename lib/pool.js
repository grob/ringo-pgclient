/**
 * @module connectionpool
 * @typedef {Object} HikariDataSource
 */

/**
 * Loads all .jar files placed in the `jars` directory of SqlStore
 */
const loadJars = exports.loadJars = () => {
    // add all jar files in jars directory to classpath
    getRepository(module.resolve("../jars/"))
            .getResources()
            .filter(resource => resource.name.endsWith(".jar"))
            .forEach(file => addToClasspath(file));
};

/**
 * Returns a newly instantiated connection pool using the properties
 * passed as argument
 * @param {Object} props The database properties
 * @returns {HikariDataSource}
 */
exports.init = (props) => {
    loadJars();
    const config = new com.zaxxer.hikari.HikariConfig();
    config.setDriverClassName(props.driver);
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