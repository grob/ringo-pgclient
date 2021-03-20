const newPool = require("../lib/pool");

exports.initPool = () => newPool(require("./config.json"));