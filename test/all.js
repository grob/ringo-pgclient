const system = require("system");

exports.testDataTypes = require("./datatypes_test");
exports.testModel = require("./model/model_test");
exports.testClient = require("./client_test");
exports.testTransaction = require("./transaction/transaction_test");

//start the test runner if we're called directly from command line
if (require.main == module.id) {
    system.exit(require("test").run.apply(null,
            [exports].concat(system.args.slice(1))));
}
