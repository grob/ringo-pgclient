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
        "values": [
            {"value": null, "expected": 0},
            {"value": 1, "expected": 1},
            {"value": 1.1, "expected": 1},
            {"value": java.lang.Long.MIN_VALUE, "expected": java.lang.Long.MIN_VALUE},
            {"value": java.lang.Long.MAX_VALUE, "expected": java.lang.Long.MAX_VALUE},
        ]
    },
    {
        "dataType": dataTypes.BIGSERIAL,
        "values": [
            {"value": 1, "expected": 1},
            {"value": 1.1, "expected": 1},
            {"value": java.lang.Long.MIN_VALUE, "expected": java.lang.Long.MIN_VALUE},
            {"value": java.lang.Long.MAX_VALUE, "expected": java.lang.Long.MAX_VALUE},
        ]
    },
    {
        "dataType": dataTypes.BOOLEAN,
        "values": [
            {"value": null, "expected": false},
            {"value": true, "expected": true},
            {"value": false, "expected": false}
        ]
    },
    {
        "dataType": dataTypes.BYTEA,
        "values": [
            {"value": null, "expected": null},
            {"value": binary.toByteString("test"), "expected": binary.toByteString("test")}
        ]
    },
    {
        // TODO: length
        "dataType": dataTypes.CHARACTER,
        "values": [
            {"value": null, "expected": null},
            {"value": "a", "expected": "a"}
        ]
    },
    {
        "dataType": dataTypes.CHARACTER_VARYING,
        "values": [
            {"value": null, "expected": null},
            {"value": "öäüß", "expected": "öäüß"}
        ]
    },
    {
        "dataType": dataTypes.DATE,
        "values": [
            {"value": null, "expected": null},
            {"value": VALUE_DATE, "expected": new Date(VALUE_DATE.getTime())}
        ]
    },
    {
        "dataType": dataTypes.DOUBLE_PRECISION,
        "values": [
            {"value": null, "expected": 0},
            {"value": 1.234, "expected": 1.2339999675750732}
        ]
    },
    {
        "dataType": dataTypes.INTEGER,
        "values": [
            {"value": null, "expected": 0},
            {"value": 12, "expected": 12}
        ]
    },
    {
        "dataType": dataTypes.JSON_TYPE,
        "values": [
            {"value": null, "expected": null},
            {"value": {"test": 1}, "expected": {"test": 1}},
            {"value": [{"test": 1}], "expected": [{"test": 1}]}
        ]
    },
    {
        "dataType": dataTypes.JSONB,
        "values": [
            {"value": null, "expected": null},
            {"value": {"test": 1}, "expected": {"test": 1}},
            {"value": [{"test": 1}], "expected": [{"test": 1}]}
        ]
    },
    {
        // TODO: precision, scale
        "dataType": dataTypes.NUMERIC,
        "values": [
            {"value": null, "expected": 0},
            {"value": 1, "expected": 1},
            {"value": 1.23, "expected": 1.23}
        ]
    },
    {
        "dataType": dataTypes.REAL,
        "values": [
            {"value": null, "expected": 0},
            {"value": 1, "expected": 1},
            {"value": 1.23, "expected": 1.23}
        ]
    },
    {
        "dataType": dataTypes.SMALLINT,
        "values": [
            {"value": null, "expected": 0},
            {"value": 1, "expected": 1},
            {"value": 1.23, "expected": 1}
        ]
    },
    {
        "dataType": dataTypes.SMALLSERIAL,
        "values": [
            {"value": 1, "expected": 1},
            {"value": 1.23, "expected": 1}
        ]
    },
    {
        "dataType": dataTypes.SERIAL,
        "values": [
            {"value": 1, "expected": 1},
            {"value": 1.23, "expected": 1}
        ]
    },
    {
        "dataType": dataTypes.TEXT,
        "values": [
            {"value": null, "expected": null},
            {"value": "test", "expected": "test"}
        ]
    },
    {
        "dataType": dataTypes.TIMESTAMP,
        "values": [
            {"value": null, "expected": null},
            {"value": VALUE_TIMESTAMP, "expected": new Date(VALUE_TIMESTAMP)}
        ]
    },
    {
        "dataType": dataTypes.TIMESTAMP_WITH_TIMEZONE,
        "values": [
            {"value": null, "expected": null},
            {"value": VALUE_TIMESTAMP, "expected": new Date(VALUE_TIMESTAMP)}
        ]
    },
];

TESTS.forEach((test) => {
    const types = [test.dataType].concat(dataTypes.DATA_TYPE_ALIASES[test.dataType] || []);
    types.forEach((type) => {
        exports["test" + type.charAt(0).toUpperCase() + type.substr(1)] = () => {
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
                        "type": test.dataType
                    }
                }
            });
            database.initModel(client, Model.mapping);
            test.values.forEach((value, idx) => {
                (new Model({"id": idx, "value": value.value})).save();
                assert.strictEqual(Model.all().length, idx + 1);
                const received = Model.get(idx).value;
                switch (test.dataType) {
                    case dataTypes.BYTEA:
                        if (value.value === null) {
                            assert.isNull(received);
                        } else {
                            assert.isTrue(Arrays.equals(received.unwrap(), value.expected.unwrap()), "Value " + value.value);
                        }
                        break;
                    case dataTypes.DATE:
                    case dataTypes.TIMESTAMP:
                    case dataTypes.TIMESTAMP_WITH_TIMEZONE:
                        if (value.value === null) {
                            assert.isNull(received);
                        } else {
                            assert.strictEqual(received.getTime(), value.expected.getTime());
                        }
                        break;
                    case dataTypes.JSON_TYPE:
                    case dataTypes.JSONB:
                        if (value.value === null) {
                            assert.isNull(received);
                        } else {
                            assert.strictEqual(JSON.stringify(received), JSON.stringify(value.expected));
                        }
                        break;
                    default:
                        assert.strictEqual(received, value.expected, "Value " + value.value);
                }
            });
        };
    });
});

//start the test runner if we're called directly from command line
if (require.main == module.id) {
    system.exit(require("test").run.apply(null,
            [exports].concat(system.args.slice(1))));
}
