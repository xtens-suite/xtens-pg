/**
 * @module
 * @author Massimiliano Izzo
 * @description this handler works as a context for the transaction strategy
 *
 */
/* jshint node: true */
/* jshint esnext: true */
"use strict";

let PgKnexCrudStrategy = require("./PgCrudStrategies.js").PgKnexCrudStrategy;
let PgPromiseCrudStrategy = require("./PgCrudStrategies.js").PgPromiseCrudStrategy;

/**
 * @class
 * @name PgCrudManager
 */
class PgCrudManager {
    /**
     * @constructor
     */
    constructor (strategy, connection, fileSystemConnection) {
        if (!strategy) {
            strategy = new PgKnexCrudStrategy(connection, fileSystemConnection);
        }
        this.strategy = strategy;
        this.streamStrategy = new PgPromiseCrudStrategy(connection, fileSystemConnection);
    }

    /**
     * @method
     */
    get strategy () {
        return this._strategy;
    }

    /**
     * @method
     */
    set strategy (strategy) {
        if (strategy) {
            this._strategy = strategy;
        }
    }

    get streamStrategy () {
        return this._streamStrategy;
    }

    /**
     * @method
     */
    set streamStrategy (streamStrategy) {
        if (streamStrategy) {
            this._streamStrategy = streamStrategy;
        }
    }

    createDataType (dataType) {
        return this.strategy.createDataType(dataType);
    }

    updateDataType (dataType) {
        return this.strategy.updateDataType(dataType);
    }

    deleteDataType (idDataType) {
        return this.strategy.deleteDataType(idDataType);
    }

    countData (criteria) {
        return this.strategy.countData(criteria);
    }

    findData (criteria) {
        return this.strategy.findData(criteria);
    }

    /**
     * @method
     * @name getDataTypesByRolePrivileges
     * @description fetch a list of datatypes given user/groups access permissions
     * @param{Object} criteria - may contain the following parameters:
     *                  - idOperator [integer]: the ID of the operator doing the current request
     *                  - model [string]: the MODEL of the dataTypes
     *                  - parentDataType [integer]: the ID of the parent dataType
     *                  - idDataTypes [Array<integer>]: an array of the IDs of the allowed dataTypes

     */

    getDataTypesByRolePrivileges (criteria) {
        return this.strategy.getDataTypesByRolePrivileges(criteria);
    }

    createData (data, dataTypeName) {
        return this.strategy.createData(data, dataTypeName);
    }

    updateData (data, dataTypeName) {
        return this.strategy.updateData(data, dataTypeName);
    }

    deleteData (idData) {
        return this.strategy.deleteData(idData);
    }

    createSample (sample, sampleTypeName, project) {
        return this.strategy.createSample(sample, sampleTypeName, project);
    }

    updateSample (sample, sampleTypeName) {
        return this.strategy.updateSample(sample, sampleTypeName);
    }

    deleteSample (idSample) {
        return this.strategy.deleteSample(idSample);
    }

    createSubject (subject, subjectTypeName) {
        return this.strategy.createSubject(subject, subjectTypeName);
    }

    updateSubject (subject) {
        return this.strategy.updateSubject(subject);
    }

    deleteSubject (idSubject) {
        return this.strategy.deleteSubject(idSubject);
    }

    putMetadataFieldsIntoEAV (idDataType, metadataField) {
        return this.strategy.putMetadataFieldsIntoEAV(idDataType, metadataField);
    }

    putMetadataValuesIntoEAV (data, eavValueTableMap) {
        console.log("CrudManager.putMetadataValuesIntoEAV - here we are! " + data.id);
        return this.strategy.putMetadataValuesIntoEAV(data, eavValueTableMap);
    }

    /**
     * @method
     * @name query
     * @param{Object} queryObj - the prepared/parametrized statement
     * @param{function} next - callback function
     */
    query (queryObj, next) {
        if (global && global.sails && global.sails.models && global.sails.models.data &&
            global.sails.models.data.query && typeof global.sails.models.data.query === 'function') {
            global.sails.models.data.query({
                text: queryObj.statement,
                values: queryObj.parameters
            }, next);
        } else {
            next(new Error("Missing sails Data.query() method"));
        }
    }

    queryStream (queryObj, next) {
        return this.streamStrategy.queryStream(queryObj.statement, queryObj.parameters, next);
    }

    getNextBiobankCode (params, next) {
        var that = this;
        return this.strategy.knex.transaction(function (trx) {
            return that.strategy.getNextBiobankCode(params.sample, params.project, trx, next);
        });
    }

    getNextSubjectCode (params, next) {
        var that = this;
        return this.strategy.knex.transaction(function (trx) {
            return that.strategy.getNextSubjectCode(params, trx, next);
        });
    }

    findByBiobankCode (params, next) {
        return this.strategy.findByBiobankCode(params.biobankCode, params.project, next);
    }

    getCountsForDashboard (projectId, next) {
        return this.strategy.getCountsForDashboard(projectId, next);
    }

    getInfoForBarChart (dataTypeId, fieldName, model, period, next) {
        return this.strategy.getInfoForBarChart(dataTypeId, fieldName, model, period, next);
    }

    getInfoForBarChartDatediff (
        fromModel, fromDataTypeId, fromFieldName, fromHasSample, fromIsChild,
        toModel, toDataTypeId, toFieldName, toHasSample, toIsChild,
        period, next) {
        return this.strategy.getInfoForBarChartDatediff (
            fromModel, fromDataTypeId, fromFieldName, fromHasSample, fromIsChild,
            toModel, toDataTypeId, toFieldName, toHasSample, toIsChild, 
            period, next);
    }
}

module.exports = PgCrudManager;
