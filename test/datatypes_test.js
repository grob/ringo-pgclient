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
const VALUE_TIME = (new Date(1970, 0, 1, 16, 24, 17, 342)).getTime();
const VALUE_BOX = [{"x": 1, "y": 2},{"x": 3, "y": 4}];
const VALUE_POINT = {"x": 1, "y": 2};
const VALUE_CIRCLE = {"center": {"x": 1, "y": 2}, "radius": 3};
const VALUE_LINE = {"x1": 100, "y1": 100, "x2": 300, "y2": 300};
const VALUE_LSEG = [{"x": 1, "y": 2},{"x": 3, "y": 4}];
const VALUE_PATH = {
    "points": [{"x": 1, "y": 2},{"x": 3, "y": 4},{"x": 5, "y": 6},{"x": 1, "y": 1}],
    "isOpen": false
};
const VALUE_PATH_OPEN = {
    "points": [{"x": 1, "y": 2},{"x": 3, "y": 4},{"x": 5, "y": 6}],
    "isOpen": true
};
const VALUE_POLYGON = [{"x": 1, "y": 2},{"x": 3, "y": 4},{"x": 5, "y": 6}];

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
        "dataType": dataTypes.BIT,
        "tests": [
            {"value": null, "expected": null},
            {"value": "1", "expected": "1"},
        ]
    },
    {
        "dataType": dataTypes.BIT_VARYING,
        "tests": [
            {"value": null, "expected": null},
            {"value": "101", "expected": "101"},
            {"value": "101010", "expected": "101010"},
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
        "dataType": dataTypes.BOX,
        "tests": [
            {"value": null, "expected": null},
            {"value": VALUE_BOX, "expected": VALUE_BOX.reverse()} // dunno why, but pg returns points in reverse order?
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
        "dataType": dataTypes.CIRCLE,
        "tests": [
            {"value": null, "expected": null},
            {"value": VALUE_CIRCLE, "expected": VALUE_CIRCLE}
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
        "dataType": dataTypes.INTERVAL,
        "tests": [
            {"value": null, "expected": null},
            {"value": {"years": 2}, "expected": {"years": 2, "months": 0, "days": 0, "hours": 0, "minutes": 0, "seconds": 0}},
            {"value": {"months": 2}, "expected": {"years": 0, "months": 2, "days": 0, "hours": 0, "minutes": 0, "seconds": 0}},
            {"value": {"days": 2}, "expected": {"years": 0, "months": 0, "days": 2, "hours": 0, "minutes": 0, "seconds": 0}},
            {"value": {"hours": 2}, "expected": {"years": 0, "months": 0, "days": 0, "hours": 2, "minutes": 0, "seconds": 0}},
            {"value": {"minutes": 2}, "expected": {"years": 0, "months": 0, "days": 0, "hours": 0, "minutes": 2, "seconds": 0}},
            {"value": {"seconds": 2.23}, "expected": {"years": 0, "months": 0, "days": 0, "hours": 0, "minutes": 0, "seconds": 2.23}}
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
        "dataType": dataTypes.LINE,
        "tests": [
            {"value": null, "expected": null},
            {"value": VALUE_LINE, "expected": {"a": 1, "b": -1, "c": 0}}
        ]
    },
    {
        "dataType": dataTypes.LSEG,
        "tests": [
            {"value": null, "expected": null},
            {"value": VALUE_LSEG, "expected": VALUE_LSEG}
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
        "dataType": dataTypes.PATH,
        "tests": [
            {"value": null, "expected": null},
            {"value": VALUE_PATH, "expected": VALUE_PATH},
            {"value": VALUE_PATH_OPEN, "expected": VALUE_PATH_OPEN}
        ]
    },
    {
        "dataType": dataTypes.POINT,
        "tests": [
            {"value": null, "expected": null},
            {"value": VALUE_POINT, "expected": VALUE_POINT}
        ]
    },
    {
        "dataType": dataTypes.POLYGON,
        "tests": [
            {"value": null, "expected": null},
            {"value": VALUE_POLYGON, "expected": VALUE_POLYGON}
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
        "dataType": dataTypes.TIME,
        "tests": [
            {"value": null, "expected": null},
            {"value": VALUE_TIME, "expected": VALUE_TIME}
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
        "dataType": dataTypes.TIME_WITH_TIMEZONE,
        "tests": [
            {"value": null, "expected": null},
            {"value": VALUE_TIME, "expected": VALUE_TIME}
        ]
    },
    {
        "dataType": dataTypes.TIMESTAMP_WITH_TIMEZONE,
        "tests": [
            {"value": null, "expected": null},
            {"value": VALUE_TIMESTAMP, "expected": new Date(VALUE_TIMESTAMP)}
        ]
    },
    {
        "dataType": dataTypes.TSQUERY,
        "tests": [
            {"value": null, "expected": null},
            {"value": "test", "expected": "'test'"},
            {"value": "fat & rat", "expected": "'fat' & 'rat'"},
            {"value": "fat & (rat | cat)", "expected": "'fat' & ( 'rat' | 'cat' )"},
        ]
    },
    {
        "dataType": dataTypes.TSVECTOR,
        "tests": [
            {"value": null, "expected": null},
            {"value": "test", "expected": "'test'"},
            {
                "value": "a fat cat sat on a mat and ate a fat rat",
                "expected": "'a' 'and' 'ate' 'cat' 'fat' 'mat' 'on' 'rat' 'sat'"},
            {
                "value": "a:1 fat:2 cat:3 sat:4 on:5 a:6 mat:7 and:8 ate:9 a:10 fat:11 rat:12",
                "expected": "'a':1,6,10 'and':8 'ate':9 'cat':3 'fat':2,11 'mat':7 'on':5 'rat':12 'sat':4"
            }
        ]
    }
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
        case dataTypes.INTERVAL:
        case dataTypes.BOX:
        case dataTypes.POINT:
        case dataTypes.CIRCLE:
        case dataTypes.LINE:
        case dataTypes.LSEG:
        case dataTypes.PATH:
        case dataTypes.POLYGON:
            assert.deepEqual(received, expected);
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
            Model.createTable();
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
