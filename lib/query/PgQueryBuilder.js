/**
 * @module
 * @name PgQueryBuilder
 * @author Massimiliano Izzo
 * @description this builder works as a context for the query/search strategy
 */
/*jshint node: true */
/*jshint esnext: true */
"use strict";

let PgJSONBQueryStrategy = require("../../lib/query/PgQueryStrategies.js").PgJSONBQueryStrategy;

/**
 * @class 
 * @name PgQueryBuilder
 */
class PgQueryBuilder {
    
    /**
     * @constructor
     * @param{PgQueryStrategy} strategy
     */

    constructor(strategy) {
        if (!strategy) {
            strategy = new PgJSONBQueryStrategy();
        }
        this.strategy = strategy;
    }

    get strategy() {
        return this._strategy;
    }

    set strategy(strategy) {
        if (strategy) {
            this._strategy = strategy;
        }
    }
    
    /**
     * @method
     * @name compose
     * @description compose a query given a JSON query object
     * @param{Object} queryParams - a valid query object
     */
    compose(queryParams) {
        return this.strategy.compose(queryParams);
    }

} 

module.exports = PgQueryBuilder;
