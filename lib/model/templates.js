const SqlTemplate = require("../sqltemplate");

exports.buildTemplates = (mapping) => {
    return {
        "insert": buildInsertTemplate(mapping),
        "update": buildUpdateTemplate(mapping),
        "delete": buildDeleteTemplate(mapping),
        "get": buildGetTemplate(mapping),
        "getAll": buildQueryTemplate(mapping)
    };
};

const buildInsertTemplate = exports.buildInsertTemplate = (mapping) => {
    const columns = Object.keys(mapping.properties).reduce((columns, name) => {
        columns.push(mapping.properties[name].column);
        return columns;
    }, [mapping.id.column]);
    const keys = Object.keys(mapping.properties).map(name => "#{" + name + "}");
    keys.unshift(mapping.id.sequence ? "nextval('" + mapping.id.sequence + "')" : "#{id}");
    return SqlTemplate.parse([
        "insert into", mapping.table,
        "(" + columns.join(", ") + ")",
        "values",
        "(" + keys.join(", ") + ")",
        "returning *"
    ].join(" "));
};

const buildUpdateTemplate = exports.buildUpdateTemplate = (mapping) => {
    const propMappings = Object.keys(mapping.properties)
            .map(name => ({
                "name": name,
                "column": mapping.properties[name].column
            }));
    return SqlTemplate.parse([
        "update", mapping.table, "set",
        propMappings.map(propMapping => {
            return [propMapping.column, " = #{", propMapping.name, "}"].join("")
        }).join(", "),
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
    return SqlTemplate.parse(["select", columns, "from", mapping.table,
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
