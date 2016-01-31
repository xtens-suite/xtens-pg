/**
 * @author Massimiliano Izzo
 * @description unit test
 */
/* jshint node: true */
/* jshint esnext: true */
/* jshint mocha: true */
"use strict";

let expect = require('chai').expect;
let sinon = require('sinon');
let _ = require("lodash");
let DataTypeClasses = require("../../lib/Utils").DataTypeClasses;
let PgJSONQueryStrategy = require('../../lib/query/PgQueryStrategies.js').PgJSONQueryStrategy;
let PgJSONBQueryStrategy = require('../../lib/query/PgQueryStrategies.js').PgJSONBQueryStrategy;

describe("PgJSONQueryStrategy", function() {

    let criteriaRowWithSQLInjection = {
        "fieldName":"Diagnosis",
        "fieldType":"text",
        "isList":true,
        "comparator":">= 0; DROP table data;",
        "fieldValue":["Neuroblastoma"]
    };

    let criteriaObj = {
        "dataType": 1,
        "model": "Data",
        "content": [
            {
            "fieldName": "constellation",
            "fieldType": "text",
            "comparator": "=",
            "fieldValue": "cepheus",
            "isList": false
        },
        {
            "fieldName": "type", // the stellar type
            "fieldType": "text",
            "comparator": "IN",
            "fieldValue": ["hypergiant","supergiant","main-sequence star"],
            "isList": true
        },
        {
            "fieldName": "mass",
            "fieldType": "float",
            "comparator": ">=",
            "fieldValue": "1.5",
            "fieldUnit": "M☉"
        },
        {
            "comparator": ">",
            "fieldName": "distance",
            "fieldType": "integer",
            "fieldUnit": "pc",
            "fieldValue": "50"
        }
        ]
    };

    let nestedParamsObj = {
        "dataType":1,
        "model": "Subject",
        "content":[{
            "fieldName":"Diagnosis Age",
            "fieldType":"integer",
            "isList":false,
            "comparator":"<=",
            "fieldValue":"365",
            "fieldUnit":"days"
        },{
            "fieldName":"Overall Status",
            "fieldType":"text",
            "isList":true,
            "comparator":"IN",
            "fieldValue":["Diseased"]
        },{
            "dataType":2,
            "model":"Sample",
            "content":[{
                "fieldName":"Diagnosis",
                "fieldType":"text",
                "isList":true,
                "comparator":"IN",
                "fieldValue":["Neuroblastoma"]
            },{
                "dataType":6,
                "model":"Sample",
                "content":[{
                    "fieldName":"quantity",
                    "fieldType":"float",
                    "isList":false,
                    "comparator":">=",
                    "fieldValue":"1.0",
                    "fieldUnit":"μl"
                },{
                    "dataType":3,
                    "model":"Data",
                    "content":[{
                        "fieldName":"Overall Result",
                        "fieldType":"text",
                        "isList":true,
                        "comparator":"IN",
                        "fieldValue":["SCA","NCA"]
                    }]
                }]
            },{
                "dataType":7,
                "model":"Sample",
                "content":[{
                    "fieldName":"quantity",
                    "fieldType":"float",
                    "isList":false,
                    "comparator":">=",
                    "fieldValue":"1.2",
                    "fieldUnit":"µg"
                },{
                    "dataType":8,
                    "model":"Data",
                    "content":[{
                        "fieldName":"hypoxia signature",
                        "fieldType":"text",
                        "isList":true,
                        "comparator":"IN",
                        "fieldValue":["high"]
                    }]
                }]
            }]
        }]
    };

    let subjectParamsObj = {
        "dataType":1,
        "model":"Subject",
        "content": [
            {
            "personalDetails":true,
            "surnameComparator":"LIKE",
            "surname":"Pizzi",
            "givenNameComparator":"NOT LIKE",
            "givenName":"Pippo",
            "birthDateComparator":"="
        },{
            "specializedQuery":"Subject",
            "codeComparator":"LIKE",
            "code":"PAT002"
        },{
            "specializedQuery":"Subject",
            "sexComparator":"IN",
            "sex":["F","M"]
        },{
            "fieldName":"overall_status",
            "fieldType":"text",
            "isList":true,
            "comparator":"IN",
            "fieldValue":["Diseased","Deceased"]
        },{
            "dataType":2,
            "model":"Sample",
            "content":[
                {
                "specializedQuery":"Sample",
                "biobankCodeComparator":"LIKE",
                "biobankCode":"SAMPOO1"
            },{
                "fieldName":"Diagnosis",
                "fieldType":"text",
                "isList":true,
                "comparator":"IN",
                "fieldValue":["Neuroblastoma"]
            }]
        }
        ]
    };

    before(function() {
        this.strategy = new PgJSONQueryStrategy();
    });

    describe("#getSubqueryRow", function() {
        it("#should throw an error if a comparator is not allowed (SQL injection)", function() {
            expect(this.strategy.getSubqueryRow.bind(this.strategy.getSubqueryRow, criteriaRowWithSQLInjection)).to.throw(
                "Operation not allowed. Trying to inject a forbidden comparator!!"
            );
        });
    });

    describe("#composeSpecializedPersonalDetailsQuery", function() {
        it("composes a query from a criteria object containing specialized fields on subject and personal details", function() {
            let pdProperties = subjectParamsObj.content[0];
            let parameteredQuery = this.strategy.composeSpecializedPersonalDetailsQuery(pdProperties);
            let selectStatement = "SELECT id, given_name, surname, birth_date FROM personal_details";
            let subquery = "pd.surname "+pdProperties.surnameComparator+" $1 AND pd.given_name "+pdProperties.givenNameComparator+" $2";
            let parameters = [pdProperties.surname.toUpperCase(), pdProperties.givenName.toUpperCase()];
            expect(parameteredQuery).to.have.property('select');
            expect(parameteredQuery).to.have.property('where');
            expect(parameteredQuery).to.have.property('previousOutput');
            expect(parameteredQuery.select).to.equal(selectStatement);
            expect(parameteredQuery.subquery).to.equal(subquery);
            expect(parameteredQuery.previousOutput.parameters).to.eql(parameters);
        });
    });

    describe("#composeSpecializedQuery", function() {
        it("composes a query from a criteria object containing specialized fields on subject (code)", function() {
            let subjProperties = subjectParamsObj.content[1];
            let parameteredQuery = this.strategy.composeSpecializedQuery(subjProperties, {}, "d.");
            let subquery = "d.code LIKE $1";
            expect(parameteredQuery).to.have.property('subquery');
            expect(parameteredQuery.subquery).to.equal(subquery);
            // TODO add parameters check into the array
        });

        it("composes a query from a criteria object containing specialized fields on subject (sex)", function() {
            let subjSex = subjectParamsObj.content[2];
            let parameteredQuery = this.strategy.composeSpecializedQuery(subjSex, { parameters: [subjectParamsObj.dataType]}, "d.");
            let subquery = "d.sex IN ($2,$3)";
            expect(parameteredQuery.parameters).to.eql(_.flatten([subjectParamsObj.dataType, subjectParamsObj.content[2].sex]));
        });

    });

    describe("#composeSingle", function() {

        it("composes a query from a criteria object containing only nonrecursive fields", function() {
            let parameteredQuery = this.strategy.composeSingle(criteriaObj);
            let selectStatement = "SELECT id, parent_subject, parent_sample, parent_data FROM data d";
            let whereClause = "WHERE d.type = $1 AND (" +
                "((d.metadata->$2->>'value')::text = $3) AND " +
                "((d.metadata->$4->>'value')::text IN ($5,$6,$7)) AND " +
                "((d.metadata->$8->>'value')::float >= $9 AND " + "(d.metadata->$8->>'unit')::text LIKE $10) AND " +
                "((d.metadata->$11->>'value')::integer > $12 AND " + "(d.metadata->$11->>'unit')::text LIKE $13))";
            let parameters = [ criteriaObj.dataType,
                criteriaObj.content[0].fieldName, criteriaObj.content[0].fieldValue,
                criteriaObj.content[1].fieldName, criteriaObj.content[1].fieldValue[0],
                criteriaObj.content[1].fieldValue[1], criteriaObj.content[1].fieldValue[2],
                criteriaObj.content[2].fieldName, criteriaObj.content[2].fieldValue, criteriaObj.content[2].fieldUnit,
                criteriaObj.content[3].fieldName, criteriaObj.content[3].fieldValue, criteriaObj.content[3].fieldUnit
            ];
            expect(parameteredQuery).to.have.property('select');
            expect(parameteredQuery).to.have.property('where');
            expect(parameteredQuery).to.have.property('previousOutput');
            expect(parameteredQuery.select).to.equal(selectStatement);
            expect(parameteredQuery.where).to.equal(whereClause);
            expect(parameteredQuery.previousOutput.parameters).to.eql(parameters);
            expect(parameteredQuery.previousOutput.lastPosition).to.equal(13);
        });

        it("composes a query from a criteria object containing specialized fields on subject and personal details", function() {
            let commonTableExpr = [
                "SELECT id, given_name, surname, birth_date FROM personal_details pd WHERE pd.surname NOT LIKE "
            ];
        });

        it("composes a set of queries from a nested criteria object", function() {
            let commonTableExpressions = [
                "SELECT id, parent_subject, parent_sample, parent_data FROM data ",
                "WHERE type = $14 AND (((metadata->$15->>'value')::text IN ($16,$17)))", //CGH

                "SELECT id, biobank_code, parent_subject, parent_sample FROM sample ",
                "WHERE type = $10 AND (((metadata->$11->>'value')::float >= $12 AND (metadata->$11->>'unit')::text LIKE $13))",

                "SELECT id, parent_subject, parent_sample, parent_data FROM data ",
                "WHERE type = $22 AND (((metadata->$23->>'value')::text IN ($24)))", // Microarray

                "SELECT id, biobank_code, parent_subject, parent_sample FROM sample ",
                "WHERE type = $18 AND (((metadata->$19->>'value')::float >= $20 AND (metadata->$19->>'unit')::text LIKE $21))",

                "SELECT id, biobank_code, parent_subject, parent_sample FROM sample WHERE type = $7 AND (((metadata->$8->>'value')::text IN ($9)))"
            ];

            let selectStatement = "SELECT id, code, sex FROM subject d";
            let whereClause = "WHERE d.type = $1 AND (((d.metadata->$2->>'value')::integer <= $3 ";
            whereClause += "AND (d.metadata->$2->>'unit')::text LIKE $4) AND ((d.metadata->$5->>'value')::text IN ($6)))";
            let parameters = [ nestedParamsObj.dataType,
                nestedParamsObj.content[0].fieldName, nestedParamsObj.content[0].fieldValue, nestedParamsObj.content[0].fieldUnit, // Subject
                nestedParamsObj.content[1].fieldName, nestedParamsObj.content[1].fieldValue[0],
                nestedParamsObj.content[2].dataType, nestedParamsObj.content[2].content[0].fieldName, //Tissue
                nestedParamsObj.content[2].content[0].fieldValue[0],
                nestedParamsObj.content[2].content[1].dataType, nestedParamsObj.content[2].content[1].content[0].fieldName, // DNA Sample
                nestedParamsObj.content[2].content[1].content[0].fieldValue, nestedParamsObj.content[2].content[1].content[0].fieldUnit,
                nestedParamsObj.content[2].content[1].content[1].dataType, // CGH
                nestedParamsObj.content[2].content[1].content[1].content[0].fieldName,
                nestedParamsObj.content[2].content[1].content[1].content[0].fieldValue[0],
                nestedParamsObj.content[2].content[1].content[1].content[0].fieldValue[1],
                nestedParamsObj.content[2].content[2].dataType, nestedParamsObj.content[2].content[2].content[0].fieldName, // RNA Sample
                nestedParamsObj.content[2].content[2].content[0].fieldValue, nestedParamsObj.content[2].content[2].content[0].fieldUnit,
                nestedParamsObj.content[2].content[2].content[1].dataType, // Microarray
                nestedParamsObj.content[2].content[2].content[1].content[0].fieldName,
                nestedParamsObj.content[2].content[2].content[1].content[0].fieldValue[0]
            ];
            console.log(parameters);
            console.log(parameters.length);
            let nestedParameteredQuery = this.strategy.composeSingle(nestedParamsObj);
            let res = this.strategy.composeCommonTableExpression(nestedParameteredQuery);
            console.log(nestedParameteredQuery.parameters);
            expect(nestedParameteredQuery.select).to.equal(selectStatement);
            expect(nestedParameteredQuery.where).to.equal(whereClause);
            // expect(_.pluck(nestedParameteredQuery.commonTableExpressions, 'statement')).to.eql(commonTableExpressions);
            expect(nestedParameteredQuery.previousOutput.parameters).to.eql(parameters);
            expect(nestedParameteredQuery.previousOutput.lastPosition).to.equal(parameters.length);
        });
    });

    describe("#compose", function() {
        it("composes a query from a nested criteria object (containing only nonrecursive fields)", function() {
            let query = this.strategy.compose(nestedParamsObj);

            let commonTableExpr = [
                "WITH nested_1 AS (SELECT id, biobank_code, parent_subject, parent_sample FROM sample ",
                "WHERE type = $7 AND (((metadata->$8->>'value')::text IN ($9)))), ",
                "nested_2 AS (SELECT id, biobank_code, parent_subject, parent_sample FROM sample WHERE type = $10 ",
                "AND (((metadata->$11->>'value')::float >= $12 AND (metadata->$11->>'unit')::text LIKE $13))), ",
                "nested_3 AS (SELECT id, parent_subject, parent_sample, parent_data FROM data ",
                "WHERE type = $14 AND (((metadata->$15->>'value')::text IN ($16,$17)))), ",
                "nested_4 AS (SELECT id, biobank_code, parent_subject, parent_sample FROM sample WHERE type = $18 ",
                "AND (((metadata->$19->>'value')::float >= $20 AND (metadata->$19->>'unit')::text LIKE $21))), ",
                "nested_5 AS (SELECT id, parent_subject, parent_sample, parent_data FROM data ",
                "WHERE type = $22 AND (((metadata->$23->>'value')::text IN ($24))))"
            ].join("");
            let mainQuery = [
                "SELECT DISTINCT d.id, d.code, d.sex, d.metadata FROM subject d ",
                "INNER JOIN nested_1 ON nested_1.parent_subject = d.id ",
                "INNER JOIN nested_2 ON nested_2.parent_sample = nested_1.id ",
                "INNER JOIN nested_3 ON nested_3.parent_sample = nested_2.id ",
                "INNER JOIN nested_4 ON nested_4.parent_sample = nested_1.id ",
                "INNER JOIN nested_5 ON nested_5.parent_sample = nested_4.id ",
                "WHERE d.type = $1 ",
                "AND (((d.metadata->$2->>'value')::integer <= $3 AND (d.metadata->$2->>'unit')::text LIKE $4) ",
                "AND ((d.metadata->$5->>'value')::text IN ($6)));"
            ].join("");
            expect(query).to.have.property('statement');
            expect(query).to.have.property('parameters');
            expect(query.statement).to.equal(commonTableExpr + " " + mainQuery);
        });

        it("composes a query from a nested subject criteria object (containing specialized fields only)", function() {
            let query = this.strategy.compose(subjectParamsObj);

            let commonTableExpr = [
                "WITH pd AS (SELECT id, given_name, surname, birth_date FROM personal_details), ",
                "nested_1 AS (SELECT id, biobank_code, parent_subject, parent_sample FROM sample ",
                "WHERE type = $10 AND ((biobank_code LIKE $11) AND ((metadata->$12->>'value')::text IN ($13))))"
            ].join("");
            let mainQuery = [
                "SELECT DISTINCT d.id, d.code, d.sex, d.metadata FROM subject d ",
                "LEFT JOIN pd ON pd.id = d.personal_info ",
                "INNER JOIN nested_1 ON nested_1.parent_subject = d.id ",
                "WHERE d.type = $1 ",
                "AND ((pd.surname LIKE $2 AND pd.given_name NOT LIKE $3) ",
                "AND (d.code LIKE $4) AND (d.sex IN ($5,$6)) AND ((d.metadata->$7->>'value')::text IN ($8,$9)));"
            ].join("");
            expect(query).to.have.property('statement');
            expect(query).to.have.property('parameters');
            expect(query.statement).to.equal(commonTableExpr + " " + mainQuery);
            console.log("Parameters for query with sex options: ");
            console.log(query.parameters);
            expect(query.parameters).to.have.length(13);
            // name and surname searches should be set to uppercase
            expect(query.parameters[1]).to.equal(subjectParamsObj.content[0].surname.toUpperCase());
            expect(query.parameters[2]).to.equal(subjectParamsObj.content[0].givenName.toUpperCase());
        });

        it("composes a query from a nested subject criteria object (containing specialized fields only)", function() {

            let query = this.strategy.compose(_.assign({wantsPersonalInfo: true}, _.cloneDeep(subjectParamsObj)));
            let commonTableExpr = [
                "WITH pd AS (SELECT id, given_name, surname, birth_date FROM personal_details), ",
                "nested_1 AS (SELECT id, biobank_code, parent_subject, parent_sample FROM sample ",
                "WHERE type = $10 AND ((biobank_code LIKE $11) AND ((metadata->$12->>'value')::text IN ($13))))"
            ].join("");
            let mainQuery = [
                "SELECT DISTINCT d.id, d.code, d.sex, pd.given_name, pd.surname, pd.birth_date, d.metadata FROM subject d ",
                "LEFT JOIN pd ON pd.id = d.personal_info ",
                "INNER JOIN nested_1 ON nested_1.parent_subject = d.id ",
                "WHERE d.type = $1 AND ((pd.surname LIKE $2 AND pd.given_name NOT LIKE $3) ",
                "AND (d.code LIKE $4) AND (d.sex IN ($5,$6)) AND ((d.metadata->$7->>'value')::text IN ($8,$9)));"
            ].join("");
            expect(query).to.have.property('statement');
            expect(query).to.have.property('parameters');
            expect(query.statement).to.equal(commonTableExpr + " " + mainQuery);
            console.log(query.parameters);
            expect(query.parameters).to.have.length(13);

        });

    });

});

/**
 *
 */
describe("PgJSONBQueryStrategy", function() {

    let criteriaObj = {
        "dataType": 1,
        "model": "Data",
        "content": [
            {
            "fieldName": "constellation",
            "fieldType": "text",
            "comparator": "=",
            "fieldValue": "cepheus",
            "isList": false
        },
        {
            "fieldName": "type", // the stellar type
            "fieldType": "text",
            "comparator": "IN",
            "fieldValue": ["hypergiant","supergiant","main-sequence star"],
            "isList": true
        },
        {
            "fieldName": "mass",
            "fieldType": "float",
            "comparator": ">=",
            "fieldValue": "1.5",
            "fieldUnit": "M☉"
        },
        {
            "comparator": ">",
            "fieldName": "distance",
            "fieldType": "integer",
            "fieldUnit": "pc",
            "fieldValue": "50"
        }
        ]
    };

    let comparisonCriteriaObj = {
        "dataType": 8,
        "model": "Data",
        "content": [{
            "comparator": "LIKE",
            "fieldName": "name",
            "fieldType": "text",
            "fieldValue": "Aldebaran",
            "isList": false
        },{
            "comparator": "ILIKE",
            "fieldName": "constellation",
            "fieldType": "text",
            "fieldValue": "Orio%",
            "isList": false
        }]
    };

    let negativeComparisonCriteriaObj = {
        "dataType": 8,
        "model": "Data",
        "content": [{
            "comparator": "NOT LIKE",
            "fieldName": "name",
            "fieldType": "text",
            "fieldValue": "Ald%",
            "isList": false
        },{
            "comparator": "NOT ILIKE",
            "fieldName": "constellation",
            "fieldType": "text",
            "fieldValue": "%rIO%",
            "isList": false
        }]
    };

    let caseInsensitiveCriteriaObj = {
        "dataType": 1,
        "model": "Data",
        "content": [{
            "comparator": "=",
            "fieldName": "name",
            "fieldType": "text",
            "fieldValue": "Aldebaran",
            "isList": false,
            "caseInsensitive": true
        }]
    };

    let numericCriteriaObj = {
        "dataType": 1,
        "model": "Data",
        "content": [{
            "comparator": "=",
            "fieldName": "distance",
            "fieldType": "float",
            "fieldValue": "8.25",
            "fieldUnit": "pc"
        }, {
            "comparator": "=",
            "fieldName": "temperature",
            "fieldType": "integer",
            "fieldValue": "7500",
            "fieldUnit": "K"
        }
        ]
    };

    let booleanCriteriaObj = {
        "dataType": 1,
        "model": "Data",
        "content": [
            {
            "comparator": "=",
            "fieldName": "is_neutron_star",
            "fieldType": "boolean",
            "fieldValue": true,
            "isList": false
        },
        {
            "comparator": "=",
            "fieldName": "is_black_hole",
            "fieldType": "boolean",
            "fieldValue": false
        }
        ]
    };

    let booleanStringCriteriaObj = {
        "dataType": 1,
        "model": "Data",
        "content": [
            {
            "comparator": "=",
            "fieldName": "is_neutron_star",
            "fieldType": "boolean",
            "fieldValue": "true",
            "isList": false
        },
        {
            "comparator": "=",
            "fieldName": "is_black_hole",
            "fieldType": "boolean",
            "fieldValue": "false"
        }
        ]
    };

    let loopCriteriaObj = {
        "dataType": 7,
        "content": [
            {
            "comparator": "=",
            "fieldName": "gene_name",
            "fieldType": "text",
            "fieldValue": "Corf44",
            "isInLoop": true
        }
        ]
    };

    let loopPatternMatchingCriteriaObj = {
        "dataType": 7,
        "content": [
            {
            "comparator": "ILIKE",
            "fieldName": "gene_name",
            "fieldType": "text",
            "fieldValue": "CORF%",
            "isInLoop": true
        }
        ]
    };

    let loopListCriteriaObj = {
        "dataType": 7,
        "content": [
            {
            "comparator": "?&",
            "fieldName": "gene_name",
            "fieldType": "text",
            "fieldValue": ["MYCN","ALK","CD44","SOX4", "Corf44"],
            "isList": true,
            "isInLoop": true
        }
        ]
    };

    let sampleParamsObj = {"dataType":4,"model":"Sample","wantsSubject":true,"wantsPersonalInfo":true,"content":[{"specializedQuery":"Sample","biobank":1, "biobankComparator":"="},{"fieldName":"quantity","fieldType":"float","isList":false,"comparator":">=","fieldValue":"1.0","fieldUnit":"μg"},{"dataType":6,"model":"Data","content":[{"fieldName":"platform","fieldType":"text","isList":true,"comparator":"IN","fieldValue":["Agilent"]},{"fieldName":"array","fieldType":"text","isList":true,"comparator":"IN","fieldValue":["4x180K"]},{"dataType":7,"model":"Data","content":[{"fieldName":"genome","fieldType":"text","isList":true,"comparator":"IN","fieldValue":["hg19"]},{"dataType":8,"model":"Data","content":[{"fieldName":"chr","fieldType":"text","isList":true,"comparator":"IN","fieldValue":["chr11","chr17"]},{"fieldName":"is_amplification","fieldType":"boolean","isList":false,"comparator":"=","fieldValue":"true"}]}]}]}]};

    let emptySampleObj = {
        "dataType": 2,
        "model": "Sample",
        "content": [
            {"specializedQuery": "Sample"},
            {}
        ]
    };

    before(function() {
        this.strategy = new PgJSONBQueryStrategy();
    });

    describe("#getSubqueryRow", function() {

        it("should return a clause with a containment operator", function() {
            let i = 1;
            let previousOutput = {lastPosition: i, parameters: []};
            let res = this.strategy.getSubqueryRow(criteriaObj.content[0], previousOutput, 'd.');
            expect(res.subquery).to.equal("d.metadata @> $"+ (i+1));
            expect(res.previousOutput).to.have.property("parameters");
            expect(res.previousOutput.parameters).to.eql(['{\"constellation\":{\"value\":\"cepheus\"}}']);
        });

    });

    describe("#getSubqueryRowAttribute", function() {

        it("should return a clause with a containment operator", function() {
            let i = 1;
            let previousOutput = {lastPosition: i, parameters: []};
            let res = this.strategy.getSubqueryRowAttribute(criteriaObj.content[0], previousOutput, 'd.');
            expect(res.subquery).to.equal("d.metadata @> $"+ (i+1));
            expect(res.previousOutput).to.have.property("parameters");
            expect(res.previousOutput.parameters).to.eql(['{\"constellation\":{\"value\":\"cepheus\"}}']);
        });

        it("should return a containment (@>) clause with uppercase metadata value (case insensitive search)", function() {
            let i = 1;
            let previousOutput = {lastPosition: i, parameters: []};
            let res = this.strategy.getSubqueryRowAttribute(caseInsensitiveCriteriaObj.content[0], previousOutput, 'd.');
            expect(res.subquery).to.equal("d.metadata @> $"+ (i+1));
            expect(res.previousOutput).to.have.property("parameters");
            expect(res.previousOutput.parameters).to.eql(['{\"name\":{\"value\":\"ALDEBARAN\"}}']);
        });

        it("should return a containment (@>) clause with integer metadata value", function() {
            let i = 1;
            let previousOutput = {lastPosition: i, parameters: []};
            let res = this.strategy.getSubqueryRowAttribute(numericCriteriaObj.content[1], previousOutput, 'd.');
            expect(res.subquery).to.equal("d.metadata @> $" + (++i) + " AND d.metadata @> $" + (++i));
            expect(res.previousOutput).to.have.property("parameters");
            expect(res.previousOutput.parameters).to.eql(['{\"temperature\":{\"value\":7500}}', '{\"temperature\":{\"unit\":\"K\"}}']);
        });

        it("should return a containment (@>) clause with floating point metadata value", function() {
            let i = 1;
            let previousOutput = {lastPosition: i, parameters: []};
            let res = this.strategy.getSubqueryRowAttribute(numericCriteriaObj.content[0], previousOutput, 'd.');
            expect(res.subquery).to.equal("d.metadata @> $" + (++i) + " AND d.metadata @> $" + (++i));
            expect(res.previousOutput).to.have.property("parameters");
            expect(res.previousOutput.parameters).to.eql(['{\"distance\":{\"value\":8.25}}', '{\"distance\":{\"unit\":\"pc\"}}']);
        });

    });

    describe("#getSubqueryRowLoop", function() {

        it("should return a clause with the element exists [?] jsonb operator", function() {
            let i = 1;
            let previousOutput = {lastPosition: i, parameters: []};
            let res = this.strategy.getSubqueryRowLoop(loopCriteriaObj.content[0], previousOutput, 'd.');
            /*
               expect(res.subquery).to.equal("(d.metadata->$"+(++i)+"->'values' ? $"+(++i)+")");
               expect(res.previousOutput).to.have.property("parameters");
               expect(res.previousOutput.parameters).to.eql([loopCriteriaObj.content[0].fieldName, loopCriteriaObj.content[0].fieldValue]);
               */
            expect(res.subquery).to.equal("d.metadata @> $" + (++i));
            expect(res.previousOutput).to.have.property("parameters");
            expect(res.previousOutput.parameters).to.eql(['{\"gene_name\":{\"values\":[\"Corf44\"]}}']);
        });

        it("should return a clause with the element exists [?] jsonb operator with case insensitive values", function() {
            let i = 1;
            let previousOutput = {lastPosition: i, parameters: []};
            let caseInsensitiveLoopRow = _.extend(_.clone(loopCriteriaObj.content[0]), {caseInsensitive: true});
            let res = this.strategy.getSubqueryRowLoop(caseInsensitiveLoopRow, previousOutput, 'd.');
            /*
               expect(res.subquery).to.equal("(d.metadata->$"+(++i)+"->'values' ? $"+(++i)+")");
               expect(res.previousOutput).to.have.property("parameters");
               expect(res.previousOutput.parameters).to.eql([loopCriteriaObj.content[0].fieldName, loopCriteriaObj.content[0].fieldValue.toUpperCase()]);
               */
            expect(res.subquery).to.equal("d.metadata @> $" + (++i));
            expect(res.previousOutput).to.have.property("parameters");
            expect(res.previousOutput.parameters).to.eql(['{\"gene_name\":{\"values\":[\"CORF44\"]}}']);
        });


        it("should return a clause with the element exists [?] jsonb operator with NOT condition", function() {
            let i = 1;
            let previousOutput = {lastPosition: i, parameters: []};
            loopCriteriaObj.content[0].comparator = '<>';
            let res = this.strategy.getSubqueryRowLoop(loopCriteriaObj.content[0], previousOutput, 'd.');
            expect(res.subquery).to.equal("NOT d.metadata @> $" + (++i));
            expect(res.previousOutput).to.have.property("parameters");
            expect(res.previousOutput.parameters).to.eql(['{\"gene_name\":{\"values\":[\"Corf44\"]}}']);
            /*
               expect(res.subquery).to.equal("(NOT d.metadata->$"+(++i)+"->'values' ? $"+(++i)+")");
               expect(res.previousOutput).to.have.property("parameters");
               expect(res.previousOutput.parameters).to.eql([loopCriteriaObj.content[0].fieldName, loopCriteriaObj.content[0].fieldValue]);
               */
        });

        it("should return a clause with the element exists all [?&] jsonb operator", function() {
            let i = 1;
            let previousOutput = {lastPosition: i, parameters: []};
            let res = this.strategy.getSubqueryRowLoop(loopListCriteriaObj.content[0], previousOutput, 'd.');
            expect(res.subquery).to.equal("d.metadata @> $" + (++i));
            expect(res.previousOutput).to.have.property("parameters");
            expect(res.previousOutput.parameters).to.eql(['{\"gene_name\":{\"values\":[\"MYCN\",\"ALK\",\"CD44\",\"SOX4\",\"Corf44\"]}}']);
            /*
               expect(res.subquery).to.equal("(d.metadata->$"+(++i)+"->'values' ?& $"+(++i)+")");
               expect(res.previousOutput).to.have.property("parameters");
               expect(res.previousOutput.parameters).to.eql([loopListCriteriaObj.content[0].fieldName, loopListCriteriaObj.content[0].fieldValue]);
               */
        });

        it("should return a clause with the element exists all [?&] jsonb operator (case insensitive)", function() {
            let i = 1;
            let previousOutput = {lastPosition: i, parameters: []};
            let caseInsensitiveLoopListRow = _.extend(_.cloneDeep(loopListCriteriaObj.content[0]), {caseInsensitive: true});
            let values = _.map(loopListCriteriaObj.content[0].fieldValue, el => el.toUpperCase());
            let res = this.strategy.getSubqueryRowLoop(caseInsensitiveLoopListRow, previousOutput, 'd.');
            expect(res.subquery).to.equal("d.metadata @> $" + (++i));
            expect(res.previousOutput).to.have.property("parameters");
            expect(res.previousOutput.parameters).to.eql(['{\"gene_name\":{\"values\":[\"MYCN\",\"ALK\",\"CD44\",\"SOX4\",\"CORF44\"]}}']);
            /*
               expect(res.subquery).to.equal("(d.metadata->$"+(++i)+"->'values' ?& $"+(++i)+")");
               expect(res.previousOutput).to.have.property("parameters");
               expect(res.previousOutput.parameters).to.eql([loopListCriteriaObj.content[0].fieldName, values]); */
        });

        it("should return a clause with the element exists any [?|] jsonb operator", function() {
            let i = 1;
            let previousOutput = {lastPosition: i, parameters: []};
            loopListCriteriaObj.content[0].comparator = '?|';
            let res = this.strategy.getSubqueryRowLoop(loopListCriteriaObj.content[0], previousOutput, 'd.');
            expect(res.subquery).to.equal("(d.metadata->$"+(++i)+"->'values' ?| $"+(++i)+")");
            expect(res.previousOutput).to.have.property("parameters");
            expect(res.previousOutput.parameters).to.eql([loopListCriteriaObj.content[0].fieldName, loopListCriteriaObj.content[0].fieldValue]);
        });

        /* TODO LIKE/ILIKE search on loops (use json_array_elements?) */
        it("should return a clause with a pattern matching search", function() {
            let i = 1;
            let previousOutput = {lastPosition: i, parameters: []};
            let res = this.strategy.getSubqueryRowLoop(loopPatternMatchingCriteriaObj.content[0], previousOutput, 'd.');
            expect(res.subquery).to.equal("EXISTS (SELECT 1 FROM jsonb_array_elements_text(d.metadata->$"+(++i)+"->'values') WHERE value ILIKE $"+(++i)+")");
            expect(res.previousOutput).to.have.property("parameters");
            expect(res.previousOutput.parameters).to.eql([loopPatternMatchingCriteriaObj.content[0].fieldName,
                loopPatternMatchingCriteriaObj.content[0].fieldValue]);
        });

    });

    describe("#composeSingle", function() {

        it("compose a query from criteria with positive matching and range conditions on nonrecursive fields", function() {
            let parameteredQuery = this.strategy.composeSingle(criteriaObj);
            let selectStatement = "SELECT id, parent_subject, parent_sample, parent_data FROM data d";
            let whereClause = "WHERE d.type = $1 AND (" +
                "(d.metadata @> $2) AND (d.metadata @> $3 OR d.metadata @> $4 OR d.metadata @> $5) AND " +
                "((d.metadata->$6->>'value')::float >= $7 AND " + "d.metadata @> $8) AND " +
                "((d.metadata->$9->>'value')::integer > $10 AND " + "d.metadata @> $11))";
            let parameters = [ criteriaObj.dataType,
                '{\"constellation\":{\"value\":\"cepheus\"}}', '{\"type\":{\"value\":\"hypergiant\"}}',
                '{\"type\":{\"value\":\"supergiant\"}}', '{\"type\":{\"value\":\"main-sequence star\"}}',
                criteriaObj.content[2].fieldName, criteriaObj.content[2].fieldValue, '{\"mass\":{\"unit\":\"M☉\"}}',
                criteriaObj.content[3].fieldName, criteriaObj.content[3].fieldValue, '{\"distance\":{\"unit\":\"pc\"}}'
            ];

            expect(parameteredQuery).to.have.property('select');
            expect(parameteredQuery).to.have.property('where');
            expect(parameteredQuery).to.have.property('previousOutput');
            expect(parameteredQuery.select).to.equal(selectStatement);
            expect(parameteredQuery.where).to.equal(whereClause);
            expect(parameteredQuery.previousOutput.parameters).to.eql(parameters);

        });

        it("compose a query from criteria with positive string pattern matching (LIKE/Insensitive LIKE)", function() {
            let parameteredQuery = this.strategy.composeSingle(comparisonCriteriaObj);
            let selectStatement = "SELECT id, parent_subject, parent_sample, parent_data FROM data d";
            let whereClause = "WHERE d.type = $1 AND (" +
                "((d.metadata->$2->>'value')::text LIKE $3) AND " +
                "((d.metadata->$4->>'value')::text ILIKE $5))";
            let parameters = [ comparisonCriteriaObj.dataType,
                comparisonCriteriaObj.content[0].fieldName, comparisonCriteriaObj.content[0].fieldValue,
                comparisonCriteriaObj.content[1].fieldName, comparisonCriteriaObj.content[1].fieldValue
            ];

            expect(parameteredQuery).to.have.property('select');
            expect(parameteredQuery).to.have.property('where');
            expect(parameteredQuery).to.have.property('previousOutput');
            expect(parameteredQuery.select).to.equal(selectStatement);
            expect(parameteredQuery.where).to.equal(whereClause);
            expect(parameteredQuery.previousOutput.parameters).to.eql(parameters);

        });

        it("compose a query from criteria with negative string pattern matching (NOT LIKE/NOT Insensitive LIKE)", function() {
            let parameteredQuery = this.strategy.composeSingle(negativeComparisonCriteriaObj);
            let selectStatement = "SELECT id, parent_subject, parent_sample, parent_data FROM data d";
            let whereClause = "WHERE d.type = $1 AND (" +
                "((d.metadata->$2->>'value')::text NOT LIKE $3) AND " +
                "((d.metadata->$4->>'value')::text NOT ILIKE $5))";
            let parameters = [ negativeComparisonCriteriaObj.dataType,
                negativeComparisonCriteriaObj.content[0].fieldName, negativeComparisonCriteriaObj.content[0].fieldValue,
                negativeComparisonCriteriaObj.content[1].fieldName, negativeComparisonCriteriaObj.content[1].fieldValue
            ];

            expect(parameteredQuery).to.have.property('select');
            expect(parameteredQuery).to.have.property('where');
            expect(parameteredQuery).to.have.property('previousOutput');
            expect(parameteredQuery.select).to.equal(selectStatement);
            expect(parameteredQuery.where).to.equal(whereClause);
            expect(parameteredQuery.previousOutput.parameters).to.eql(parameters);

        });

        it("compose a query from criteria with exclusion matching and range conditions on nonrecursive fields", function() {

            criteriaObj.content[0].comparator = "<>";
            criteriaObj.content[1].comparator = "NOT IN";
            let selectStatement = "SELECT id, parent_subject, parent_sample, parent_data FROM data d";
            let whereClause = "WHERE d.type = $1 AND (" +
                "(NOT d.metadata @> $2) AND (NOT d.metadata @> $3 OR NOT d.metadata @> $4 OR NOT d.metadata @> $5) AND " +
                "((d.metadata->$6->>'value')::float >= $7 AND " + "d.metadata @> $8) AND " +
                "((d.metadata->$9->>'value')::integer > $10 AND " + "d.metadata @> $11))";
            let parameters = [ criteriaObj.dataType,
                '{\"constellation\":{\"value\":\"cepheus\"}}', '{\"type\":{\"value\":\"hypergiant\"}}',
                '{\"type\":{\"value\":\"supergiant\"}}', '{\"type\":{\"value\":\"main-sequence star\"}}',
                criteriaObj.content[2].fieldName, criteriaObj.content[2].fieldValue, '{\"mass\":{\"unit\":\"M☉\"}}',
                criteriaObj.content[3].fieldName, criteriaObj.content[3].fieldValue, '{\"distance\":{\"unit\":\"pc\"}}'
            ];
            let parameteredQuery = this.strategy.composeSingle(criteriaObj);
            expect(parameteredQuery).to.have.property('select');
            expect(parameteredQuery).to.have.property('where');
            expect(parameteredQuery).to.have.property('previousOutput');
            expect(parameteredQuery.select).to.equal(selectStatement);
            expect(parameteredQuery.where).to.equal(whereClause);
            expect(parameteredQuery.previousOutput.parameters).to.eql(parameters);

        });

        it("compose a query with two boolean fields (from string)", function() {

            let selectStatement = "SELECT id, parent_subject, parent_sample, parent_data FROM data d";
            let whereClause = "WHERE d.type = $1 AND ((d.metadata @> $2) AND (d.metadata @> $3))";
            let parameters = [booleanStringCriteriaObj.dataType,
                '{\"is_neutron_star\":{\"value\":true}}', '{\"is_black_hole\":{\"value\":false}}'];
                let parameteredQuery = this.strategy.composeSingle(booleanStringCriteriaObj);
                expect(parameteredQuery).to.have.property('select');
                expect(parameteredQuery).to.have.property('where');
                expect(parameteredQuery).to.have.property('previousOutput');
                expect(parameteredQuery.select).to.equal(selectStatement);
                expect(parameteredQuery.where).to.equal(whereClause);
                expect(parameteredQuery.previousOutput.parameters).to.eql(parameters);

        });

        it("compose a query with two boolean fields (from boolean)", function() {

            let selectStatement = "SELECT id, parent_subject, parent_sample, parent_data FROM data d";
            let whereClause = "WHERE d.type = $1 AND ((d.metadata @> $2) AND (d.metadata @> $3))";
            let parameters = [booleanCriteriaObj.dataType,
                '{\"is_neutron_star\":{\"value\":true}}', '{\"is_black_hole\":{\"value\":false}}'];
                let parameteredQuery = this.strategy.composeSingle(booleanCriteriaObj);
                expect(parameteredQuery).to.have.property('select');
                expect(parameteredQuery).to.have.property('where');
                expect(parameteredQuery).to.have.property('previousOutput');
                expect(parameteredQuery.select).to.equal(selectStatement);
                expect(parameteredQuery.where).to.equal(whereClause);
                expect(parameteredQuery.previousOutput.parameters).to.eql(parameters);

        });

        it("compose a query with a loop array condition", function() {

            let selectStatement = "SELECT id, parent_subject, parent_sample, parent_data FROM data d";
            let whereClause = "WHERE d.type = $1 AND (((d.metadata->$2->'values' " + loopListCriteriaObj.content[0].comparator + " $3)))";
            let parameters = [loopListCriteriaObj.dataType, loopListCriteriaObj.content[0].fieldName, loopListCriteriaObj.content[0].fieldValue];
            let parameteredQuery = this.strategy.composeSingle(loopListCriteriaObj);
            expect(parameteredQuery).to.have.property('select');
            expect(parameteredQuery).to.have.property('where');
            expect(parameteredQuery).to.have.property('previousOutput');
            expect(parameteredQuery.select).to.equal(selectStatement);
            expect(parameteredQuery.where).to.equal(whereClause);
            expect(parameteredQuery.previousOutput.parameters).to.eql(parameters);

        });

        /*
           it("compose a query with a loop array condition", function() {

           let selectStatement = "SELECT id, parent_subject, parent_sample, parent_data FROM data d";
           let whereClause = "WHERE d.type = $1 AND (((d.metadata->$2->'values' " + loopListCriteriaObj.content[0].comparator + " $3)))";
           let parameters = [loopListCriteriaObj.dataType, loopListCriteriaObj.content[0].fieldName,
           _.map(loopListCriteriaObj.content[0].fieldValue, elem => elem.toUpperCase())];
           loopCriteriaObj.content[0].caseInsensitive = true;
           let parameteredQuery = this.strategy.composeSingle(loopListCriteriaObj);
           expect(parameteredQuery).to.have.property('select');
           expect(parameteredQuery).to.have.property('where');
           expect(parameteredQuery).to.have.property('previousOutput');
           expect(parameteredQuery.select).to.equal(selectStatement);
           expect(parameteredQuery.where).to.equal(whereClause);
           expect(parameteredQuery.previousOutput.parameters).to.eql(parameters);

           }); */

    });

    describe("#compose", function() {

        it("composes a query from a nested sample criteria object", function() {
            let query = this.strategy.compose(sampleParamsObj);
            let commonTableExpr = [
                "WITH s AS (SELECT id, code, sex, personal_info FROM subject), ",
                "pd AS (SELECT id, given_name, surname, birth_date FROM personal_details), ",
                "bb AS (SELECT id, biobank_id, acronym, name FROM biobank), ",
                "nested_1 AS (SELECT id, parent_subject, parent_sample, parent_data FROM data ",
                "WHERE type = $6 AND ((metadata @> $7) AND (metadata @> $8))), ",
                "nested_2 AS (SELECT id, parent_subject, parent_sample, parent_data FROM data WHERE type = $9 AND ((metadata @> $10))), ",
                "nested_3 AS (SELECT id, parent_subject, parent_sample, parent_data FROM data ",
                "WHERE type = $11 AND ((metadata @> $12 OR metadata @> $13) AND (metadata @> $14)))"
            ].join("");
            let mainQuery = [
                "SELECT DISTINCT d.id, d.biobank, d.biobank_code, s.code, s.sex, pd.given_name, pd.surname, pd.birth_date, bb.acronym AS biobank_acronym, d.metadata FROM sample d ",
                "LEFT JOIN s ON s.id = d.parent_subject ",
                "LEFT JOIN pd ON pd.id = s.personal_info ",
                "LEFT JOIN bb ON bb.id = d.biobank ",
                "INNER JOIN nested_1 ON nested_1.parent_sample = d.id ",
                "INNER JOIN nested_2 ON nested_2.parent_data = nested_1.id ",
                "INNER JOIN nested_3 ON nested_3.parent_data = nested_2.id ",
                "WHERE d.type = $1 AND ((d.biobank = $2) AND ((d.metadata->$3->>'value')::float >= $4 AND d.metadata @> $5));"
            ].join("");
            expect(query).to.have.property('statement');
            expect(query).to.have.property('parameters');
            expect(query.statement).to.equal(commonTableExpr + " " + mainQuery);
            console.log(query.parameters);
            expect(query.parameters).to.have.length(14);
        });

        it("composes a query from an empty sample criteria (containing an empty specialized criteria)", function() {
            let query = this.strategy.compose(emptySampleObj);
            let expectedStatement = ["WITH bb AS (SELECT id, biobank_id, acronym, name FROM biobank) ",
                "SELECT DISTINCT d.id, d.biobank, d.biobank_code, bb.acronym AS biobank_acronym, d.metadata FROM sample d ",
                "LEFT JOIN bb ON bb.id = d.biobank ",
                "WHERE d.type = $1;"].join("");
                expect(query.statement).to.equal(expectedStatement);
                expect(query.parameters).to.eql([emptySampleObj.dataType]);
        });

});

});
