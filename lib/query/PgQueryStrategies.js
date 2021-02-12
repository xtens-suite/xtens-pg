/**
 * @module
 * @name PgCrudStrategies
 * @author Massimiliano Izzo
 * @description this handler works as a context for the transaction strategy
 *
 */
/* jshint node: true */
/* jshint esnext: true */
"use strict";

let _ = require("lodash");
const DataTypeClasses = require("../Utils").DataTypeClasses;
const FieldTypes = require("../Utils.js").FieldTypes;
let determineTableByModel = require("../Utils.js").determineTableByModel;
let allowedComparators = require("../Utils.js").allowedComparators;
let specializedProperties = require("../Utils.js").specializedProperties;
let pdProperties = require("../Utils.js").pdProperties;

let queryOutput = {
    lastPosition: 0,
    cteCount: 0,
    parameters: []
};

let fieldsForMainQueryMap = new Map([
    [DataTypeClasses.SUBJECT, ["code", "sex"]],
    [DataTypeClasses.SAMPLE, ["biobank", "biobank_code"]],
    [DataTypeClasses.DATA, []]
]);

let fieldsForSubqueriesMap = new Map([
    [DataTypeClasses.SUBJECT, "id, code, sex, personal_info"],
    [DataTypeClasses.SAMPLE, "id, biobank_code"],
    [DataTypeClasses.DATA, "id"],
    [undefined, "id"] // default to DATA
]);

/**
 * @name JoinTableMap
 * key formatted as childtable_parenttable
 */
const joinPoolingTables =
    new Map([
        ["data_data", {
            name: "data_childrendata__data_parentdata",
            childColumn: "data_parentData",
            parentColumn: "data_childrenData",
            alias: "dtdt"
        }],
        ["data_sample", {
            name: "data_parentsample__sample_childrendata",
            childColumn: "data_parentSample",
            parentColumn: "sample_childrenData",
            alias: "dtsm"
        }],
        ["data_subject", {
            name: "data_parentsubject__subject_childrendata",
            childColumn: "data_parentSubject",
            parentColumn: "subject_childrenData",
            alias: "dtsb"
        }],
        ["sample_sample", {
            name: "sample_parentsample__sample_childrensample",
            childColumn: "sample_parentSample",
            parentColumn: "sample_childrenSample",
            alias: "smsm"
        }],
        ["sample_subject", {
            name: "sample_donor__subject_childrensample",
            childColumn: "sample_donor",
            parentColumn: "subject_childrenSample",
            alias: "smsb"
        }],
        ["subject_subject", {
            name: "subject_parentsubject__subject_childrensubject",
            childColumn: "subject_parentSubject",
            parentColumn: "subject_childrenSubject",
            alias: "sbsb"
        }]
    ]);

String.prototype.toUnderscore = function () {
    return this.replace(/([A-Z])/g, function ($1) {
        return "_" + $1.toLowerCase();
    });
};

/**
 * @class
 * @name PgQueryStrategy
 */
class PgQueryStrategy {
    /**
  * @abstract
  * @method
  * @name compose
  * @param{Object} criteria - the criteria object
  */
    compose (criteria) {
        throw new Error("Abstract method. Not implemented.");
    }
}

/**
 * @class
 * @name PgJSONQueryStrategy
 * @extends PgQueryStrategy
 */
class PgJSONQueryStrategy extends PgQueryStrategy {
    /**
  * @method
  * @name getSubqueryRow
  * @param{Object} element - a leaf in the query criteria object. It must contain the following fields:
  *                          1) fieldName
  *                          2) fieldType [TEXT, INTEGER, FLOAT, DATE, BOOLEAN]
  *                          2) comparator
  *                          3) fieldValue
  *                          4) fieldUnit [optional]
  *  @param{Object} previousOutput
  *  @param{String} tablePrefix
  */
    getSubqueryRow (element, previousOutput, tablePrefix) {
        if (_.isEmpty(element)) {
            return null;
        }
        if (allowedComparators.indexOf(element.comparator) < 0) {
            console.log(element.comparator);
            throw new Error("Operation not allowed. Trying to inject a forbidden comparator!!");
        }
        let nameParam = '$' + (++previousOutput.lastPosition);
        let valueParam; let subquery;
        if (element.isList) {
            let values = [];
            for (let i = 0; i < element.fieldValue.length; i++) {
                values.push('$' + (++previousOutput.lastPosition));
            }
            console.log(values);
            valueParam = values.join();
            subquery = "(" + tablePrefix + "metadata->" + nameParam + "->>'value')::" + element.fieldType.toLowerCase() +
                " " + element.comparator + " (" + valueParam + ")";
        } else {
            valueParam = '$' + (++previousOutput.lastPosition);
            subquery = "(" + tablePrefix + "metadata->" + nameParam + "->>'value')::" + element.fieldType.toLowerCase() +
                " " + element.comparator + " " + valueParam;
        }
        previousOutput.parameters.push(element.fieldName, element.fieldValue);
        if (element.fieldUnit) {
            let unitParam = '$' + (++previousOutput.lastPosition);
            subquery += " AND ";
            subquery += "(" + tablePrefix + "metadata->" + nameParam + "->>'unit')::text LIKE " + unitParam;
            previousOutput.parameters.push(element.fieldUnit);
        }
        // flatten nested arrays in parameters
        previousOutput.parameters = _.flatten(previousOutput.parameters);
        return {
            subquery: subquery
        };
    }

    /**
  * @method
  * @name composeSpecializedQuery
  * @description compose the part of the query relative to the specialized Model (Model here is intended in the sails.js sense)
  * @return {Object} - the query for the specialized parameters
  */
    composeSpecializedQuery (criteria, previousOutput, tablePrefix) {
        let lastParameterPosition = previousOutput.lastPosition || 0;
        previousOutput.parameters = previousOutput.parameters || [];
        let dataTypeClass = criteria.specializedQuery;
        tablePrefix = tablePrefix || ''; // 'd.';
        let query = {};
        let clauses = [];
        let comparator;
        specializedProperties[dataTypeClass].forEach(function (property) {
            if (criteria[property]) {
                if (_.isArray(criteria[property])) { // if it is a list of options (like in sex)
                    comparator = allowedComparators.indexOf(criteria[property + "Comparator"]) >= 0 ? criteria[property + "Comparator"] : 'IN';
                    let values = [];
                    for (let i = 0; i < criteria[property].length; i++) {
                        values.push('$' + (++lastParameterPosition));
                    }
                    clauses.push(tablePrefix + property.toUnderscore() + " " + comparator + " (" + values.join() + ")");
                } else {
                    comparator = allowedComparators.indexOf(criteria[property + "Comparator"]) >= 0 ? criteria[property + "Comparator"] : '=';
                    clauses.push(tablePrefix + property.toUnderscore() + " " + comparator + " $" + (++lastParameterPosition));
                }
                previousOutput.parameters.push(criteria[property]);
            }
        });
        if (clauses.length) {
            query.subquery = clauses.join(" AND "); // TODO add possibility to switch and/or
        }
        query.lastParameterPosition = lastParameterPosition;
        query.parameters = _.flatten(previousOutput.parameters);
        return query;
    }

    /**
  * @method
  * @name composeSpecializedPersonalDetailsQuery
  * @description compose the part of a query pertaining to the personal_details table (personal data)
  * @return {Object}
  */
    composeSpecializedPersonalDetailsQuery (pdCriteria, previousOutput, isNested) {
        if (!previousOutput) {
            previousOutput = {
                lastPosition: 0,
                cteCount: 0,
                parameters: []
            };
        }

        let pos = isNested ? '_' + (previousOutput.lastPosition - 1) : '';

        let query = {
            alias: 'pd' + pos
        };
        query.select = "SELECT id, given_name, surname, birth_date FROM personal_details";
        query.where = "";
        let whereClauses = []; // comparator;

        pdProperties.forEach(function (property) {
            if (pdCriteria[property]) {
                let comparator = allowedComparators.indexOf(pdCriteria[property + "Comparator"]) >= 0 ? pdCriteria[property + "Comparator"] : '=';
                whereClauses.push(query.alias + "." + property.toUnderscore() + " " + comparator + " $" + (++previousOutput.lastPosition));
                let value = ['givenName', 'surname'].indexOf(property) > -1 ? pdCriteria[property].toUpperCase() : pdCriteria[property];
                previousOutput.parameters.push(value);
            }
        });
        if (whereClauses.length) {
            // query.where = "WHERE " + whereClauses.join(" AND ");
            query.subquery = whereClauses.join(" AND ");
        }
        query.previousOutput = previousOutput;
        return query;
    }

    /**
  * @method
  * @name composeSingle
  * @description composes a query based on a single DataType
  */
    composeSingle (criteria, previousOutput, query) { // should I pass the parent params??
        if (!previousOutput) {
            previousOutput = {
                lastPosition: 0,
                cteCount: 0,
                parameters: []
            };
        }
        if (!query) {
            query = {};
        }
        query.subqueries = [];
        query.table = determineTableByModel(criteria.model);
        console.log("PostgresJSONQueryStrategy.prototype.composeSingle -  model: " + criteria.model);
        console.log("PostgresJSONQueryStrategy.prototype.composeSingle - mapped fields: " + fieldsForSubqueriesMap.get(criteria.model));
        query.select = "SELECT " + fieldsForSubqueriesMap.get(criteria.model);
        if (criteria.getMetadata) {
            query.select += ", metadata";
            query.getMetadata = true;
        }
        if (criteria.label) {
            query.label = criteria.label;
        }
        query.select = query.select + " FROM " + query.table;
        let tableAlias = previousOutput.lastPosition ? "" : " d";
        let tablePrefix = previousOutput.lastPosition ? "" : "d.";
        let whereClause = "";
        if (_.isArray(criteria.dataType)) {
            whereClause = "type IN (";
            let first = true;
            _.forEach(criteria.dataType, function (dt) {
                let clause = first ? "$" + (++previousOutput.lastPosition) : ",$" + (++previousOutput.lastPosition);
                whereClause = whereClause + clause;
                previousOutput.parameters.push(dt);
                first = false;
            });
            whereClause = whereClause + ")";
        } else {
            whereClause = "type = $" + (++previousOutput.lastPosition);
            previousOutput.parameters.push(criteria.dataType);
        }
        query.where = "WHERE " + tablePrefix + whereClause;
        // _.isArray(criteria.dataType) ? criteria.dataType = criteria.dataType.toString() : null;
        let fieldQueries = [];
        let value;
        if (criteria.content) {
            for (let i = 0; i < criteria.content.length; i++) {
                let res; let op; let element = criteria.content[i];
                if (element.dataType) {
                    res = this.composeSingle(element, previousOutput, {
                        alias: 'nested_' + (++previousOutput.cteCount)
                    });
                    previousOutput = res.previousOutput;
                    query.subqueries.push(_.omit(res, 'previousOutput'));
                } else if (element.personalDetails) {
                    let isNested =
                    op = this.composeSpecializedPersonalDetailsQuery(element, previousOutput, !!query.alias);
                    previousOutput = op.previousOutput;
                    query.subqueries.push(_.omit(op, ['previousOutput', 'subquery']));
                    fieldQueries.push(op.subquery);
                } else if (element.specializedQuery) {
                    op = this.composeSpecializedQuery(element, previousOutput, tablePrefix);
                    if (!op) {
                        continue;
                    }
                    fieldQueries.push(op.subquery);
                    previousOutput.lastPosition = op.lastParameterPosition;
                    previousOutput.parameters = op.parameters;
                } else {
                    op = this.getSubqueryRow(element, previousOutput, tablePrefix);
                    if (!op) {
                        continue;
                    }
                    fieldQueries.push(op.subquery);
                }
            }
        }
        fieldQueries = _.compact(fieldQueries);
        if (fieldQueries.length) {
            let junction = criteria.junction === 'OR' ? 'OR' : 'AND';
            query.where += " AND (" + fieldQueries.map(function (row) {
                return "(" + row + ")";
            }).join(" " + junction + " ") + ")";
        }
        query.select += tableAlias;
        // query.previousOutput =  _.extend(previousOutput, {parameters: _.flatten(previousOutput.parameters)});
        query.previousOutput = previousOutput;
        return query; // _.extend(previousOutput, {statement: query, parameters: _.flatten(previousOutput.parameters)});
    }

    /**
  * @name composeCommonTableExpression
  * @description given a list of sub-queries, the procedure stores them in a WITH statement (a.k.a Common Table Expression)
  * @return {Object} - ctes: the complete WITH statement
  */
    composeCommonTableExpression (query, leafSearch, ctes, parentAlias, parentTable, idx) {
        let qLen = query.subqueries && query.subqueries.length;

        if (!ctes) {
            ctes = [];
        } else if (query.alias.indexOf("pd") > -1) { // PERSONAL_DETAILS table
            // let joinClause = "INNER JOIN " + query.alias + " ON " + query.alias + ".id = " + parentAlias + ".personal_info";
            let ctePd = "(" + _.compact([query.select, query.where]).join(" ") + ") AS " + query.alias;
            ctes.push({
                alias: query.alias,
                commonTableExpression: ctePd,
                joinClause: "LEFT JOIN " + ctePd + " ON " + query.alias + ".id = " + parentAlias + ".personal_info"
            });
            return ctes;
        } else {
            let filesToBeAggregate = !!(leafSearch && (qLen > 0 && query.subqueries.findIndex(r => r.label) > -1));
            let orderby = !filesToBeAggregate && leafSearch ? " ORDER BY id )" : " )";
            let cte = "(" + query.select + " " + query.where + orderby;
            let joinInfo = joinPoolingTables.get(query.table + '_' + parentTable);
            let alias = joinInfo.alias + '_' + idx;
            let joinClause = 'INNER JOIN ' + joinInfo.name + ' AS ' + alias + ' ON ' + alias + '."' + joinInfo.parentColumn + '" = ' + parentAlias + '.id';
            joinClause += ' INNER JOIN ' + cte + ' AS ' + query.alias + ' ON ' + alias + '."' + joinInfo.childColumn + '" = ' + query.alias + '.id';

            ctes.push({
                alias: query.alias,
                commonTableExpression: query.alias + " AS " + cte,
                joinClause: joinClause,
                getMetadata: !!(leafSearch && query.getMetadata),
                label: leafSearch && query.label ? query.label : query.alias,
                filesToBeAggregate,
                model: query.table
            });
        }
        let alias = query.alias || 'd';
        let label = query.label || '';
        for (let i = 0; i < qLen; i++) {
            ctes = this.composeCommonTableExpression(query.subqueries[i], leafSearch, ctes, alias, query.table, idx + i + 1);
        }
        return ctes;
    }

    /**
  * @method
  * @override
  * @name compose
  * @description composes a query based on a single DataType
  * @param{Object} criteria - the query criteria object
  */
    compose (criteria) {
        let query = this.composeSingle(criteria);
        let ctes = [];
        let groupRoot = {
            alias: "d",
            filesToBeAggregate: criteria.leafSearch
        };
        let specificFields = [];
        specificFields.push({
            cte: groupRoot,
            fieldsName: fieldsForMainQueryMap.get(criteria.model)
        });
        // = this.getAggrFieldForSelect(groupRoot, fieldsForMainQueryMap.get(criteria.model));
        // let groupBy = "group by ";

        // No subject and personal details info are required if querying on subjects
        if (criteria.model !== DataTypeClasses.SUBJECT && criteria.wantsSubject) {
            let joinInfo = joinPoolingTables.get(query.table + '_subject');
            let firstJoin = 'LEFT JOIN ' + joinInfo.name + ' AS ' + joinInfo.alias + ' ON ' + joinInfo.alias + '."' + joinInfo.childColumn + '" = ' + 'd.id';
            let secondJoin = 'LEFT JOIN (SELECT id, code, sex, personal_info FROM subject) s ON s.id = ' + joinInfo.alias + '."' + joinInfo.parentColumn + '"';
            ctes.push({
                alias: 's',
                commonTableExpression: 's AS (SELECT id, code, sex, personal_info FROM subject)',
                joinClause: firstJoin + ' ' + secondJoin
            });
            let aggrS = {
                alias: "s",
                filesToBeAggregate: groupRoot.filesToBeAggregate
            };

            specificFields.push({
                cte: aggrS,
                fieldsName: ["code", "sex"]
            });
            // specificFields += this.getAggrFieldForSelect(aggrS, ["code", "sex"]);

            if (criteria.wantsPersonalInfo) {
                ctes.push({
                    alias: 'pd',
                    commonTableExpression: 'pd AS (SELECT id, given_name, surname, birth_date FROM personal_details)',
                    joinClause: 'LEFT JOIN (SELECT id, given_name, surname, birth_date FROM personal_details) pd ON pd.id = s.personal_info'
                });
                let aggrPd = {
                    alias: "pd",
                    filesToBeAggregate: groupRoot.filesToBeAggregate
                };

                specificFields.push({
                    cte: aggrPd,
                    fieldsName: ["given_name", "surname", "birth_date"]
                });
                // specificFields += this.getAggrFieldForSelect(aggrPd, ["given_name", "surname", "birth_date"]);
            }
        }

        if (criteria.model === DataTypeClasses.SUBJECT && criteria.wantsPersonalInfo) {
            let aggrPd = {
                alias: "pd",
                filesToBeAggregate: groupRoot.filesToBeAggregate
            };
            specificFields.push({
                cte: aggrPd,
                fieldsName: ["given_name", "surname", "birth_date"]
            });
            // specificFields += this.getAggrFieldForSelect(aggrPd, ["given_name", "surname", "birth_date"]);
        }

        if (criteria.model === DataTypeClasses.SAMPLE) {
            ctes.push({
                alias: 'bb',
                commonTableExpression: 'bb AS (SELECT id, biobank_id, acronym as biobank_acronym, name FROM biobank)',
                joinClause: 'LEFT JOIN (SELECT id, biobank_id, acronym as biobank_acronym, name FROM biobank) bb ON bb.id = d.biobank'
            });
            let aggrBB = {
                alias: "bb",
                filesToBeAggregate: groupRoot.filesToBeAggregate
            };

            specificFields.push({
                cte: aggrBB,
                fieldsName: ["biobank_acronym"]
            });
            // specificFields += this.getAggrFieldForSelect(aggrBB, "acronym");
        }

        ctes = ctes.concat(this.composeCommonTableExpression(query, criteria.leafSearch, null, null, null, 0));
        if (criteria.leafSearch && ctes.length > 0) {
            for (var i = 0; i < ctes.length; i++) {
                if (ctes[i].label) {
                    specificFields.push({
                        cte: ctes[i],
                        fieldsName: ["id"]
                    });
                    // specificFields += this.getAggrFieldForSelect(ctes[i], "id");
                    if (ctes[i].getMetadata) {
                        specificFields.push({
                            cte: ctes[i],
                            fieldsName: ["metadata"]
                        });
                        // specificFields += this.getAggrFieldForSelect(ctes[i], "metadata");
                        if (ctes[i].model && ctes[i].model == 'sample') {
                            specificFields.push({
                                cte: ctes[i],
                                fieldsName: ["biobank_code"]
                            });
                            // specificFields += this.getAggrFieldForSelect(ctes[i], "metadata");
                        } else if (ctes[i].model && ctes[i].model == 'subject') {
                            specificFields.push({
                                cte: ctes[i],
                                fieldsName: ["code"]
                            });
                            specificFields.push({
                                cte: ctes[i],
                                fieldsName: ["sex"]
                            });
                            // specificFields += this.getAggrFieldForSelect(ctes[i], "metadata");
                        }
                    }
                }
            }
        }

        console.log(ctes);
        // let commonTableExpressions = "";
        let joins = " ";
        let tempSelect = criteria.leafSearch ? "SELECT " : "SELECT DISTINCT ";

        specificFields.push({
            cte: groupRoot,
            fieldsName: ["id", "type", "owner", "metadata"]
        });

        var [fFields, groupFields] = this.formatSelectAndGroupFields(specificFields, criteria.leafSearch);

        query.select = tempSelect + fFields + " FROM " + query.table + " d";

        if (ctes.length) {
            // commonTableExpressions = "WITH " + _.pluck(ctes, 'commonTableExpression').join(", ");
            joins = " " + _.pluck(ctes, 'joinClause').join(" ") + " ";
        }

        let mainStatement = query.select + joins + query.where + " " + groupFields;
        // mainStatement = (commonTableExpressions + " " + mainStatement).trim() + ";";
        mainStatement = mainStatement.trim() + ";";
        return {
            statement: mainStatement,
            parameters: query.previousOutput.parameters
        };
    }

    formatSelectAndGroupFields (infoArray, isLeafSearch) {
        if (!infoArray) {
            return ["id ", ""];
        }
        let fields = [];
        let aggrFields = [];
        let groupBy = [];

        _.forEach(infoArray, (info) => {
            if (!info.cte || !info.fieldsName) {
                return;
            }
            if (!_.isArray(info.fieldsName)) {
                info.fieldsName = [info.fieldsName];
            }

            _.forEach(info.fieldsName, (fieldName) => {
                let label = "";
                let attribute = info.cte.alias + "." + fieldName;
                if (info.cte.label) {
                    label = info.cte.label;
                    if (fieldName == "id") {
                        label += "_id";
                    } else if (fieldName == "biobank_code") {
                        label += "_biobank_code";
                    } else if (fieldName == "code") {
                        label += "_code";
                    } else if (fieldName == "sex") {
                        label += "_sex";
                    }
                } else {
                    label = fieldName;
                }
                let result;
                if (isLeafSearch) {
                    if (info.cte.filesToBeAggregate) {
                        label = isLeafSearch && info.cte.filesToBeAggregate ? "'" + label + "'" : label;
                        result = label + ', ' + attribute;
                        aggrFields.push(result);
                    } else {
                        groupBy.push(attribute);
                        result = info.cte.label ? attribute + " AS " + label : attribute;
                        fields.push(result);
                    }
                } else {
                    result = info.cte.label ? attribute + " AS " + label : attribute;
                    fields.push(result);
                }
            }); // fine fieldsName foreach
        }); // fine infoArray foreach
        let formattedFields = "";
        if (aggrFields.length > 0) {
            formattedFields = 'array_agg( json_build_object(' + aggrFields.join(", ") + ')) as parents, ';
        }
        formattedFields += fields.join(", ");
        if (groupBy.length > 0) {
            groupBy = "group by " + groupBy.join(", ");
        }
        return [formattedFields, groupBy];
    }
}

/**
 * @class
 * @name PgJSONBQueryStrategy
 * @extends PgJSONQueryStrategy
 */
class PgJSONBQueryStrategy extends PgJSONQueryStrategy {
    /**
  * @method
  * @override
  * @name getSubqueryRow
  * @description compose a (sub)query fragment based a a single criterium (a single paeameter and a condition
  *              over the parameter)
  */
    getSubqueryRow (element, previousOutput, tablePrefix) {
        if (_.isEmpty(element)) {
            return null;
        }

        if (allowedComparators.indexOf(element.comparator) < 0) {
            console.log(element.comparator);
            throw new Error("Operation not allowed. Trying to inject a forbidden comparator!!");
        }

        if (element.isInLoop) {
            console.log("PostgresJSONBQueryStrategy - executing loop composition algorithm - " + element.isInLoop);
            return this.getSubqueryRowLoop(element, previousOutput, tablePrefix);
        }

        return this.getSubqueryRowAttribute(element, previousOutput, tablePrefix);
    }

    /**
  * @method
  * @name getSubqueryRowAttribute
  * @description
  */
    getSubqueryRowAttribute (element, previousOutput, tablePrefix) {
        let boolValue; let i; let subquery = "";
        let subqueries = [];
        let param = {};
        let operatorPrefix;

        if (element.fieldType === "boolean") {
            boolValue = _.isBoolean(element.fieldValue) ? element.fieldValue : (element.fieldValue.toLowerCase() === 'true');

            subquery = "(" + tablePrefix + "metadata->$" + (++previousOutput.lastPosition) + "->>'value')::" + element.fieldType.toLowerCase() +
                " " + element.comparator + "($" + (++previousOutput.lastPosition) + ")::" + element.fieldType.toLowerCase();

            previousOutput.parameters.push(element.fieldName);
            previousOutput.parameters.push(boolValue);
        } else if (element.isList) {
            subqueries = [];
            param = "\"" + element.fieldName + "\"->>\"value\")::" + element.fieldType.toLowerCase() + " " + element.comparator + " (";
            for (i = 0; i < element.fieldValue.length; i++) {
                subqueries.push("($" + (++previousOutput.lastPosition) + ")::" + element.fieldType.toLowerCase());
                previousOutput.parameters.push(element.fieldValue[i]);
            }

            subquery = "(" + tablePrefix + "metadata->$" + (++previousOutput.lastPosition) + "->>'value')::" + element.fieldType.toLowerCase() +
            " " + element.comparator + " (" + subqueries.join(",") + ")";
            previousOutput.parameters.push(element.fieldName);
        } else {
            // otherwise use the standard JSON/JSONB accessor (->/->>) operator
            subquery = "(" + tablePrefix + "metadata->$" + (++previousOutput.lastPosition) + "->>'value')::" + element.fieldType.toLowerCase() +
                " " + element.comparator + " ($" + (++previousOutput.lastPosition) + ")::" + element.fieldType.toLowerCase();
            previousOutput.parameters.push(element.fieldName);
            previousOutput.parameters.push(element.fieldValue);
        }

        // add condition on unit if present
        if (element.fieldUnit && element.fieldUnit.length > 0) {
            // var splitted = element.fieldUnit.split(',');
            subquery += " AND (";
            subquery += tablePrefix + "metadata->'" + element.fieldName + "'->>'unit')::text IN (";
            _.forEach(element.fieldUnit, (s, key) => {
                subquery += "$" + (++previousOutput.lastPosition);
                if (key + 1 < element.fieldUnit.length) {
                    subquery += ",";
                }
                previousOutput.parameters.push(s);
            });

            subquery += ")";
        }
        // flatten nested arrays (if any)
        console.log(previousOutput.parameters);
        previousOutput.parameters = _.flatten(previousOutput.parameters);
        return {
            subquery: subquery,
            previousOutput: previousOutput
        };
    }

    /**
  * @method
  * @name getSubqueryRowLoop
  */
    getSubqueryRowLoop (element, previousOutput, tablePrefix) {
        let subquery = "";
        let operatorPrefix; let jsonbValue = {};

        if (element.comparator === '=' || element.comparator === '<>') {
            operatorPrefix = element.comparator === '<>' ? 'NOT ' : '';
            subquery = operatorPrefix + tablePrefix + "metadata @> $" + (++previousOutput.lastPosition);
            // if case-insensitive turn the value to uppercase
            let val = element.fieldType === FieldTypes.INTEGER ? _.parseInt(element.fieldValue)
                : element.fieldType === FieldTypes.FLOAT ? Number(element.fieldValue)
                    : element.fieldType === FieldTypes.BOOLEAN ? element.fieldValue === 'true'
                        : element.caseInsensitive ? element.fieldValue.toUpperCase() : element.fieldValue;
            jsonbValue[element.fieldName] = {
                values: [val]
            };
        }

        // ALL VALUES operator
        else if (element.comparator === '?&') {
            operatorPrefix = element.comparator !== '?&' ? 'NOT ' : ''; // TODO so far no negative query implemented for this one
            subquery = operatorPrefix + tablePrefix + "metadata @> $" + (++previousOutput.lastPosition);
            let val = element.fieldType === FieldTypes.INTEGER ? _.map(element.fieldValue, el => _.parseInt(el))
                : element.fieldType === FieldTypes.FLOAT ? _.map(element.fieldValue, el => Number(el))
                    : element.fieldType === FieldTypes.BOOLEAN ? element.fieldValue === 'true'
                        : element.caseInsensitive ? _.map(element.fieldValue, el => el.toUpperCase()) : element.fieldValue;
            jsonbValue[element.fieldName] = {
                values: val
            };
        }

        // ANY OF THE VALUES operator
        else if (element.comparator === '?|') {
            operatorPrefix = ""; // TODO: so far no operator prefix
            subquery = "(" + operatorPrefix + tablePrefix + "metadata->$" + (++previousOutput.lastPosition) + "->'values' " + element.comparator + " $" + (++previousOutput.lastPosition) + ")";
            // if case-insensitive turn all values to uppercase
            jsonbValue = element.caseInsensitive ? _.map(element.fieldValue, el => el.toUpperCase()) : element.fieldValue;
            previousOutput.parameters.push(element.fieldName);
        }

        // string pattern matching queries (both case sensitive and insensitive)
        else if (['LIKE', 'ILIKE', 'NOT LIKE', 'NOT ILIKE'].indexOf(element.comparator) > -1) {
            subquery = "EXISTS (SELECT 1 FROM jsonb_array_elements_text(d.metadata->$" + (++previousOutput.lastPosition) +
                "->'values') WHERE value " + element.comparator + " $" + (++previousOutput.lastPosition) + ")";
            previousOutput.parameters.push(element.fieldName, element.fieldValue);
        }

        // add the jsonb value only if it not empty
        if (!_.isEmpty(jsonbValue)) {
            previousOutput.parameters.push(_.isArray(jsonbValue) ? jsonbValue : JSON.stringify(jsonbValue));
        }

        return {
            subquery: subquery,
            previousOutput: previousOutput
        };
    }
}

module.exports.PgQueryStrategy = PgQueryStrategy;
module.exports.PgJSONQueryStrategy = PgJSONQueryStrategy;
module.exports.PgJSONBQueryStrategy = PgJSONBQueryStrategy;
