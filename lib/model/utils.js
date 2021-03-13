const SqlTemplate = require("../sqltemplate");

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
        "insert into", mapping.table,
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
        "update", mapping.table,
        "set", updates.join(", "),
        "where", mapping.id.column, "= #{id}",
        "returning *"
    ].join(" "));
};

const buildDeleteTemplate = exports.buildDeleteTemplate = (mapping) => {
    return SqlTemplate.parse([
        "delete from", mapping.table,
        "where", mapping.id.column, "= #{id}"
    ].join(" "));
};

const buildGetTemplate = exports.buildGetTemplate = (mapping) => {
    const columns = Object.keys(mapping.properties).reduce((columns, key) => {
        columns.push(mapping.properties[key].column);
        return columns;
    }, [mapping.id.column]).join(", ");
    return SqlTemplate.parse([
        "select", columns, "from", mapping.table,
        "where", mapping.id.column, "= #{id}"
    ].join(" "));
};

const buildQueryTemplate = exports.buildQueryTemplate = (mapping, clause) => {
    const columns = Object.keys(mapping.properties).reduce((columns, key) => {
        columns.push(mapping.properties[key].column);
        return columns;
    }, [mapping.id.column]).join(", ");
    const template = ["select", columns, "from", mapping.table];
    if (typeof(clause) === "string" && clause.length > 0) {
        template.push("where", clause);
    }
    return SqlTemplate.parse(template.join(" "));
};
