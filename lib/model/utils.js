/**
 * @fileoverview Model utility module
 * @ignore
 */
const SqlTemplate = require("../sqltemplate");
const database = require("../database");

const getColumns = (mapping) => {
    return Object.keys(mapping.properties).reduce((columns, key) => {
        columns.push(mapping.properties[key].column);
        return columns;
    }, [mapping.id.column]).join(", ");
};

/**
 * Returns a model key
 * @param {String} type The model type
 * @param {Number} id The model id
 * @return {String} The model key
 */
exports.getKey = (type, id) => type + "#" + id;

/**
 * Returns an object containing sql template descriptors for various
 * database queries
 * @param {Object} mapping The model mapping
 * @return {Object} The sql template descriptors for <code>insert</code>,
 * <code>update</code>, <code>delete</code>, <code>get</code> and <code>getAll</code>
 * queries
 */
exports.buildModelTemplates = (mapping) => {
    return {
        "insert": buildInsertTemplate(mapping),
        "update": buildUpdateTemplate(mapping),
        "delete": buildDeleteTemplate(mapping),
        "get": buildGetTemplate(mapping),
        "getAll": buildQueryTemplate(mapping),
        "getMany": buildGetManyTemplate(mapping)
    };
};

/**
 * Returns the sql template for inserting model instances
 * @param {Object} mapping The model mapping
 * @return {Object} The sql template descriptor for inserting model instances
 */
const buildInsertTemplate = exports.buildInsertTemplate = (mapping) => {
    const spec = Object.keys(mapping.properties).reduce((spec, name) => {
        spec.columns.push(mapping.properties[name].column);
        spec.params.push("#{" + name + "}");
        return spec;
    }, {
        "columns": [mapping.id.column],
        "params": [mapping.id.sequence ? "nextval('" + mapping.id.sequence + "')" : "#{id}"]
    });
    return SqlTemplate.parse([
        "insert into", database.getFqn(mapping.table, mapping.schema),
        "(" + spec.columns.join(", ") + ")",
        "values",
        "(" + spec.params.join(", ") + ")",
        "returning *"
    ].join(" "));
};

/**
 * Returns the sql template for updating model instances
 * @param {Object} mapping The model mapping
 * @return {Object} The sql template descriptor for updating model instances
 */
const buildUpdateTemplate = exports.buildUpdateTemplate = (mapping) => {
    const updates = Object.keys(mapping.properties).map(name => {
        return mapping.properties[name].column + " = #{" + name + "}";
    });
    return SqlTemplate.parse([
        "update", database.getFqn(mapping.table, mapping.schema),
        "set", updates.join(", "),
        "where", mapping.id.column, "= #{id}",
        "returning *"
    ].join(" "));
};

/**
 * Returns the sql template for deleting model instances
 * @param {Object} mapping The model mapping
 * @return {Object} The sql template descriptor for deleting model instances
 */
const buildDeleteTemplate = exports.buildDeleteTemplate = (mapping) => {
    return SqlTemplate.parse([
        "delete from", database.getFqn(mapping.table, mapping.schema),
        "where", mapping.id.column, "= #{id}"
    ].join(" "));
};

/**
 * Returns the sql template for retrieving a model instance from database
 * @param {Object} mapping The model mapping
 * @return {Object} The sql template descriptor for retrieving a model instance
 */
const buildGetTemplate = exports.buildGetTemplate = (mapping) => {
    return SqlTemplate.parse([
        "select", getColumns(mapping), "from", database.getFqn(mapping.table, mapping.schema),
        "where", mapping.id.column, "= #{id}"
    ].join(" "));
};

const buildGetManyTemplate = exports.buildGetManyTemplate = (mapping) => {
    return SqlTemplate.parse([
        "select", getColumns(mapping), "from", database.getFqn(mapping.table, mapping.schema),
        "where", mapping.id.column, "= any(#{ids}) order by array_position(#{ids}, ", mapping.id.column, ")"
    ].join(" "));
};

/**
 * Returns the sql template for model queries
 * @param {Object} mapping The model mapping
 * @param {String} clause Optional where-clause
 * @return {Object} The sql template descriptor for model queries
 */
const buildQueryTemplate = exports.buildQueryTemplate = (mapping, clause) => {
    const template = ["select", getColumns(mapping), "from", database.getFqn(mapping.table, mapping.schema)];
    if (typeof(clause) === "string" && clause.length > 0) {
        template.push(clause);
    }
    return SqlTemplate.parse(template.join(" ").trim());
};

/**
 * Returns the sql template for model count queries
 * @param {Object} mapping The model mapping
 * @param {String} clause Optional where-clause
 * @return {Object} The sql template descriptor for model queries
 */
const buildCountTemplate = exports.buildCountTemplate = (mapping, clause) => {
    const template = ["select count(" + mapping.id.column + ") as count from", database.getFqn(mapping.table, mapping.schema)];
    if (typeof(clause) === "string" && clause.length > 0) {
        template.push(clause);
    }
    return SqlTemplate.parse(template.join(" ").trim());
};