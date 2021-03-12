const Pool = require("../lib/pool");

exports.initPool = () => {
    return Pool.init(require("./config.json"));
};