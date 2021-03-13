const SqlTemplate = require("../sqltemplate");
const database = require("../database");

exports.buildModelTemplates = (mapping) => {
    return {
        "insert": buildInsertTemplate(mapping),
        "update": buildUpdateTemplate(mapping),
        "delete": buildDeleteTemplate(mapping),
        "get": buildGetTemplate(mapping),
        "getAll": buildQueryTemplate(mapping)
    };
};

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

const buildDeleteTemplate = exports.buildDeleteTemplate = (mapping) => {
    return SqlTemplate.parse([
        "delete from", database.getFqn(mapping.table, mapping.schema),
        "where", mapping.id.column, "= #{id}"
    ].join(" "));
};

const buildGetTemplate = exports.buildGetTemplate = (mapping) => {
    const columns = Object.keys(mapping.properties).reduce((columns, key) => {
        columns.push(mapping.properties[key].column);
        return columns;
    }, [mapping.id.column]).join(", ");
    return SqlTemplate.parse([
        "select", columns, "from", database.getFqn(mapping.table, mapping.schema),
        "where", mapping.id.column, "= #{id}"
    ].join(" "));
};

const buildQueryTemplate = exports.buildQueryTemplate = (mapping, clause) => {
    const columns = Object.keys(mapping.properties).reduce((columns, key) => {
        columns.push(mapping.properties[key].column);
        return columns;
    }, [mapping.id.column]).join(", ");
    const template = ["select", columns, "from", database.getFqn(mapping.table, mapping.schema)];
    if (typeof(clause) === "string" && clause.length > 0) {
        template.push("where", clause);
    }
    return SqlTemplate.parse(template.join(" "));
};
