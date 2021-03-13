require("./utils").loadJars();

/**
 * Returns a newly instantiated connection pool using the properties
 * passed as argument
 * @param {Object} props The database properties
 * @returns {HikariDataSource}
 */
exports.init = (props) => {
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