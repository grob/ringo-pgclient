require("ringo/logging").setConfig(getResource("./log4j2.properties"));

const assert = require("assert");
const system = require("system");
const helpers = require("./helpers");
const Client = require("../lib/client");
const database = require("../lib/database");
const dataTypes = require("../lib/datatypes");
const binary = require("binary");

const {Arrays} = java.util;

let client = null;

exports.setUp = () => {
    client = new Client(helpers.initPool());
};

exports.tearDown = () => {
    database.dropAll(client);
    client && client.close();
};

const VALUE_DATE = new Date(2021, 2, 12, 0, 0, 0, 0);
const VALUE_TIMESTAMP = new Date(2021, 2, 12, 16, 24, 17, 342);

const TESTS = [
    {
        "dataType": dataTypes.BIGINT,
        "tests": [
            {"value": null, "expected": 0},
            {"value": 1, "expected": 1},
            {"value": 1.1, "expected": 1},
            {"value": java.lang.Long.MIN_VALUE, "expected": java.lang.Long.MIN_VALUE},
            {"value": java.lang.Long.MAX_VALUE, "expected": java.lang.Long.MAX_VALUE},
        ]
    },
    {
        "dataType": dataTypes.BIGSERIAL,
        "tests": [
            {"value": 1, "expected": 1},
            {"value": 1.1, "expected": 1},
            {"value": java.lang.Long.MIN_VALUE, "expected": java.lang.Long.MIN_VALUE},
            {"value": java.lang.Long.MAX_VALUE, "expected": java.lang.Long.MAX_VALUE},
        ]
    },
    {
        "dataType": dataTypes.BOOLEAN,
        "tests": [
            {"value": null, "expected": false},
            {"value": true, "expected": true},
            {"value": false, "expected": false}
        ]
    },
    {
        "dataType": dataTypes.BYTEA,
        "tests": [
            {"value": null, "expected": null},
            {"value": binary.toByteString("test"), "expected": binary.toByteString("test")}
        ]
    },
    {
        "dataType": dataTypes.CHARACTER,
        "tests": [
            {"value": null, "expected": null},
            {"value": "a", "expected": "a"},
            {"options": "(1)", "value": "a", "expected": "a"},
            {"options": "(2)", "value": "a", "expected": "a "} // character is padded with spaces up to defined length
        ]
    },
    {
        "dataType": dataTypes.CHARACTER_VARYING,
        "tests": [
            {"value": null, "expected": null},
            {"value": "öäüß", "expected": "öäüß"},
            {"options": "(1)", "value": "a", "expected": "a"},
            {"options": "(2)", "value": "a", "expected": "a"}
        ]
    },
    {
        "dataType": dataTypes.DATE,
        "tests": [
            {"value": null, "expected": null},
            {"value": VALUE_DATE, "expected": new Date(VALUE_DATE.getTime())}
        ]
    },
    {
        "dataType": dataTypes.DOUBLE_PRECISION,
        "tests": [
            {"value": null, "expected": 0},
            {"value": 1.234, "expected": 1.2339999675750732}
        ]
    },
    {
        "dataType": dataTypes.INTEGER,
        "tests": [
            {"value": null, "expected": 0},
            {"value": 12, "expected": 12}
        ]
    },
    {
        "dataType": dataTypes.JSON_TYPE,
        "tests": [
            {"value": null, "expected": null},
            {"value": {"test": 1}, "expected": {"test": 1}},
            {"value": [{"test": 1}], "expected": [{"test": 1}]}
        ]
    },
    {
        "dataType": dataTypes.JSONB,
        "tests": [
            {"value": null, "expected": null},
            {"value": {"test": 1}, "expected": {"test": 1}},
            {"value": [{"test": 1}], "expected": [{"test": 1}]}
        ]
    },
    {
        "dataType": dataTypes.NUMERIC,
        "tests": [
            {"value": null, "expected": 0},
            {"value": 1, "expected": 1},
            {"value": 1.23, "expected": 1.23},
            {"options": "(1)", "value": 1, "expected": 1},
            {"options": "(2)", "value": 10, "expected": 10},
            {"options": "(2,1)", "value": 1.22, "expected": 1.2},
            {"options": "(3,2)", "value": 1.22, "expected": 1.22},
        ]
    },
    {
        "dataType": dataTypes.NAME,
        "tests": [
            {"value": null, "expected": null},
            {"value": "öäüß", "expected": "öäüß"}
        ]
    },
    {
        "dataType": dataTypes.REAL,
        "tests": [
            {"value": null, "expected": 0},
            {"value": 1, "expected": 1},
            {"value": 1.23, "expected": 1.23}
        ]
    },
    {
        "dataType": dataTypes.SMALLINT,
        "tests": [
            {"value": null, "expected": 0},
            {"value": 1, "expected": 1},
            {"value": 1.23, "expected": 1}
        ]
    },
    {
        "dataType": dataTypes.SMALLSERIAL,
        "tests": [
            {"value": 1, "expected": 1},
            {"value": 1.23, "expected": 1}
        ]
    },
    {
        "dataType": dataTypes.SERIAL,
        "tests": [
            {"value": 1, "expected": 1},
            {"value": 1.23, "expected": 1}
        ]
    },
    {
        "dataType": dataTypes.TEXT,
        "tests": [
            {"value": null, "expected": null},
            {"value": "test", "expected": "test"}
        ]
    },
    {
        "dataType": dataTypes.TIMESTAMP,
        "tests": [
            {"value": null, "expected": null},
            {"value": VALUE_TIMESTAMP, "expected": new Date(VALUE_TIMESTAMP)},
            {"options": "(1)", "value": VALUE_TIMESTAMP, "expected": new Date(VALUE_TIMESTAMP - VALUE_TIMESTAMP % 100)}
        ]
    },
    {
        "dataType": dataTypes.TIMESTAMP_WITH_TIMEZONE,
        "tests": [
            {"value": null, "expected": null},
            {"value": VALUE_TIMESTAMP, "expected": new Date(VALUE_TIMESTAMP)}
        ]
    },
];

const checkReceived = (dataType, received, expected) => {
    switch (dataType) {
        case dataTypes.BYTEA:
            if (expected === null) {
                assert.isNull(received);
            } else {
                assert.isTrue(Arrays.equals(received.unwrap(), expected.unwrap()));
            }
            break;
        case dataTypes.DATE:
        case dataTypes.TIMESTAMP:
        case dataTypes.TIMESTAMP_WITH_TIMEZONE:
        case dataTypes.TIMESTAMPTZ:
            if (expected === null) {
                assert.isNull(received);
            } else {
                assert.strictEqual(received.constructor, Date);
                assert.strictEqual(received.getTime(), expected.getTime());
            }
            break;
        case dataTypes.JSON_TYPE:
        case dataTypes.JSONB:
            if (expected === null) {
                assert.isNull(received);
            } else {
                assert.strictEqual(JSON.stringify(received), JSON.stringify(expected));
            }
            break;
        default:
            assert.strictEqual(received, expected);
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
            database.initModel(client, Model.mapping);
            (new Model({"id": idx, "value": params.value})).save();
            assert.strictEqual(Model.all().length, 1);
            checkReceived(type, Model.get(idx).value, params.expected);
        };
    });
};

TESTS.forEach((test) => {
    const typesToTest = [test.dataType].concat(dataTypes.DATA_TYPE_ALIASES[test.dataType] || []);
    typesToTest.forEach((typeToTest) => {
        createTest(typeToTest, test);
    });
});

//start the test runner if we're called directly from command line
if (require.main == module.id) {
    system.exit(require("test").run.apply(null,
            [exports].concat(system.args.slice(1))));
}
