/**
 * @module
 * @name Utils
 * @author Massimiliano Izzo
 */
/* jshint node: true */
/* jshint esnext: true */
"use strict";

const DataTypeClasses = {
    SUBJECT: 'Subject',
    SAMPLE: 'Sample',
    GENERIC: 'Generic',
    DATA: 'Data'
};

const FieldTypes = {
    TEXT: 'text',
    INTEGER: 'integer',
    FLOAT: 'float',
    DATE: 'date',
    BOOLEAN: 'boolean'
};

// list of comparators allowed by the query system
const allowedComparators = ['=', '<', '>', '<=', '>=', '<>', 'IN', 'NOT IN', 'LIKE', 'NOT LIKE', '?&', '?|'];

function extend(subClass, superClass) {
    var F = function() {};
    F.prototype = superClass.prototype;
    subClass.prototype = new F();
    subClass.prototype.constructor = subClass;
    subClass.superclass = superClass.prototype;
    if(superClass.prototype.constructor == Object.prototype.constructor) {
        superClass.prototype.constructor = superClass;
    }
}

function determineTableByModel(model) {
    switch (model) {
        case DataTypeClasses.SUBJECT:
            return "subject";
        case DataTypeClasses.SAMPLE:
            return "sample";
        case DataTypeClasses.DATA:
            return "data";
        default:
            return "data";
    }
}

var specializedProperties = {};
specializedProperties[DataTypeClasses.SUBJECT] = ["code", "sex"];
specializedProperties[DataTypeClasses.SAMPLE] = ["biobank", "biobankCode"];
var pdProperties = ["surname", "givenName", "birthDate"];  // list of specialized properties in the PERSONAL_DETAILS table

// list of module exports
exports.specializedProperties = specializedProperties;
exports.pdProperties = pdProperties;
exports.allowedComparators = allowedComparators;
exports.extend = extend;
exports.determineTableByModel = determineTableByModel;
exports.DataTypeClasses = DataTypeClasses;
exports.FieldTypes = FieldTypes;
