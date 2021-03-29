const newPool = require("../lib/pool");

exports.initPool = () => newPool(require("./config.json"));

exports.configureLogging = () => {
    const isVersion1 = typeof org.apache.log4j.Logger === "function";
    require("ringo/logging").setConfig(getResource(isVersion1 ? "./log4j.properties" :"./log4j2.properties" ));
};