/**
 * @module
 * @author Massimiliano Izzo
 * @description this handler works as a context for the transaction strategy
 *  
 */
/*jshint node: true */
/*jshint esnext: true */
"use strict";

let PgKnexStrategy = require("./PgCrudStrategies.js").PgKnexStrategy;

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
            strategy = new PgKnexStrategy(connection, fileSystemConnection);
        }
        this.strategy = strategy;    
    }
    
    /**
     * @method
     */
    get strategy() {
        return this._strategy();
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

    createData(data, dataTypeName) {
        return this.strategy.createData(data, dataTypeName);
    }

    updateData(data) {
        return this.strategy.updateData(data);
    }

    createSample(sample, sampleTypeName) {
        return this.strategy.createSample(sample, sampleTypeName);
    }

    updateSample(sample) {
        return this.strategy.updateSample(sample);
    }

    createSubject(subject, subjectTypeName) {
        return this.strategy.createSubject(subject, subjectTypeName);
    }

    updateSubject(subject) {
        return this.strategy.updateSubject(subject);
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
     * @param{String} statement - the prepared/parametrized statement
     * @param{Array} params - the parameters array
     * @return{Promise} a promise with args an array with retrieved items
     */
    query(statement, params) {
        return this.strategy.query(statement, params);
    }
}

module.exports = PgCrudManager;
