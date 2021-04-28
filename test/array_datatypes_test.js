require("./helpers").configureLogging();

const assert = require("assert");
const system = require("system");
const helpers = require("./helpers");
const Client = require("../lib/client");
const database = require("../lib/database");
const binary = require("binary");

let client = null;

exports.setUp = () => {
    client = new Client(helpers.initPool());
};

exports.tearDown = () => {
    database.dropAll(client);
    client && client.close();
};

const INTEGER_TESTS = [
    {"value": null, "expected": null},
    {"value": [1], "expected": [1]},
    {"value": [1,2,3], "expected": [1,2,3]}
];

const TESTS = [
    {
        "dataType": "bigint[]",
        "aliases": ["int8[]"],
        "tests": INTEGER_TESTS
    },
    {
        "dataType": "bit[]",
        "tests": [
            {"value": null, "expected": null},
            {"value": ["1"], "expected": ["1"]},
            {"value": ["1","0"], "expected": ["1","0"]},
        ]
    },
    {
        "dataType": "bit varying[]",
        "tests": [
            {"value": null, "expected": null},
            {"value": ["101"], "expected": ["101"]},
            {"value": ["101", "101010"], "expected": ["101", "101010"]}
        ]
    },
    {
        "dataType": "boolean[]",
        "aliases": ["bool[]"],
        "tests": [
            {"value": null, "expected": null},
            {"value": [true], "expected": [true]},
            {"value": [true, false], "expected": [true, false]}
        ]
    },
    {
        "dataType": "character[]",
        "aliases": ["char[]"],
        "tests": [
            {"value": null, "expected": null},
            {"value": ["a"], "expected": ["a"]}
        ]
    },
    {
        "dataType": "character varying[]",
        "aliases": ["varchar[]"],
        "tests": [
            {"value": null, "expected": null},
            {"value": ["öäüß"], "expected": ["öäüß"]}
        ]
    },
    {
        "dataType": "double precision[]",
        "aliases": ["float8[]"],
        "tests": [
            {"value": null, "expected": null},
            {"value": [1.1], "expected": [1.1]},
            {"value": [1.1,2.2,3.3], "expected": [1.1,2.2,3.3]}
        ]
    },
    {
        "dataType": "numeric[]",
        "aliases": ["decimal[]"],
        "tests": [
            {"value": null, "expected": null},
            {"value": [1.1], "expected": [1.1]},
            {"value": [1.1,2.2,3.3], "expected": [1.1,2.2,3.3]}
        ]
    },
    {
        "dataType": "smallint[]",
        "aliases": ["int2[]"],
        "tests": INTEGER_TESTS
    },
    {
        "dataType": "integer[]",
        "aliases": ["int[]", "int4[]"],
        "tests": INTEGER_TESTS
    },
    {
        "dataType": "real[]",
        "aliases": ["float4[]"],
        "tests": [
            {"value": null, "expected": null},
            {"value": [1.1], "expected": [1.100000023841858]},
            {"value": [1.1,2.2,3.3], "expected": [1.100000023841858,2.200000047683716,3.299999952316284]}
        ]
    },
    {
        "dataType": "text[]",
        "tests": [
            {"value": null, "expected": null},
            {"value": ["test"], "expected": ["test"]}
        ]
    },
];

const checkReceived = (dataType, received, expected) => {
    switch (dataType) {
        default:
            assert.deepEqual(received, expected);
    }
};

const getTestName = (type, idx) => {
    return "test" + type.split(/\s+/)
            .map(part => part.charAt(0).toUpperCase() + part.substr(1))
            .join("") + "-" + idx;
};

const createTest = (type, test) => {
    test.tests.forEach((params, idx) => {
        exports[getTestName(type, idx)] = () => {
            const Model = client.defineModel("TypeTest", {
                "table": "t_type_test",
                "id": {
                    "column": "tst_id",
                    "type": "bigint",
                    "constraint": "primary key"
                },
                "properties": {
                    "value": {
                        "column": "tst_value",
                        "type": type + (params.options || "")
                    }
                }
            });
            Model.createTable();
            (new Model({"id": idx, "value": params.value})).save();
            assert.strictEqual(Model.all().length, 1);
            checkReceived(type, Model.get(idx).value, params.expected);
        };
    });
};

TESTS.forEach((test) => {
    const typesToTest = [test.dataType].concat(test.aliases || []);
    typesToTest.forEach((typeToTest) => {
        createTest(typeToTest, test);
    });
});

//start the test runner if we're called directly from command line
if (require.main == module.id) {
    system.exit(require("test").run.apply(null,
            [exports].concat(system.args.slice(1))));
}
