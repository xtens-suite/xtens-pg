/**
 * @module
 * @author Massimiliano Izzo
 * @description this handler works as a context for the transaction strategy
 *  
 */
/*jshint node: true */
/*jshint esnext: true */
"use strict";

let PgKnexCrudStrategy = require("./PgCrudStrategies.js").PgKnexCrudStrategy;

/**
 * @class
 * @name PgCrudManager
 */
class PgCrudManager {
    
    /**
     * @constructor
     */
    constructor(strategy, connection, fileSystemConnection) {
        if (!strategy) {
            strategy = new PgKnexCrudStrategy(connection, fileSystemConnection);
        }
        this.strategy = strategy;    
    }
    
    /**
     * @method
     */
    get strategy() {
        return this._strategy;
    }
    
    /**
     * @method 
     */
    set strategy(strategy) {
        if (strategy) {
            this._strategy = strategy;
        }
    }

    createDataType(dataType) {
        return this.strategy.createDataType(dataType);
    }

    updateDataType(dataType) {
        return this.strategy.updateDataType(dataType);
    }

    deleteDataType(idDataType) {
        return this.strategy.deleteDataType(idDataType);
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
    getDataTypesByRolePrivileges(criteria) {
        return this.strategy.getDataTypesByRolePrivileges(criteria);
    }

    createData(data, dataTypeName) {
        return this.strategy.createData(data, dataTypeName);
    }

    updateData(data) {
        return this.strategy.updateData(data);
    }

    deleteData(idData) {
        return this.strategy.deleteData(idData);
    }

    createSample(sample, sampleTypeName) {
        return this.strategy.createSample(sample, sampleTypeName);
    }

    updateSample(sample) {
        return this.strategy.updateSample(sample);
    }

    deleteSample(idSample) {
        return this.strategy.deleteDataType(idSample);
    }

    createSubject(subject, subjectTypeName) {
        return this.strategy.createSubject(subject, subjectTypeName);
    }

    updateSubject(subject) {
        return this.strategy.updateSubject(subject);
    }

    deleteSubject(idSubject) {
        return this.strategy.deleteDataType(idSubject);
    }

    putMetadataFieldsIntoEAV(idDataType, metadataField) {
        return this.strategy.putMetadataFieldsIntoEAV(idDataType, metadataField);
    }

    putMetadataValuesIntoEAV(data, eavValueTableMap) {
        console.log("CrudManager.putMetadataValuesIntoEAV - here we are! " + data.id);
        return this.strategy.putMetadataValuesIntoEAV(data, eavValueTableMap);
    }

    /**
     * @method 
     * @name query
     * @param{Object} queryObj - the prepared/parametrized statement
     * @param{function} next - callback function
     */
    query(queryObj, next) {
        
        if (global && global.sails && global.sails.models && global.sails.models.data && 
            global.sails.models.data.query && typeof global.sails.models.data.query === 'function') {
            global.sails.models.data.query({
                text: queryObj.statement,
                values: queryObj.parameters
            }, next);
        }
        else {
            next(new Error("Missing sails Data.query() method"));
        }

    }
}

module.exports = PgCrudManager;
