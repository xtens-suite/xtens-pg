/**
 * @author Massimiliano Izzo
 */
/* jshint node: true */
/* jshint esnext: true */
/* jshint mocha: true */
"use strict";

let expect = require('chai').expect;
let sinon = require('sinon');
let PgQueryBuilder = require('../../lib/query/PgQueryBuilder.js');
let PgJSONQueryStrategy = require('../../lib/query/PgQueryStrategies.js').PgJSONQueryStrategy;
let PgJSONBQueryStrategy = require('../../lib/query/PgQueryStrategies.js').PgJSONBQueryStrategy;


describe('#PgQueryBuilder', function() {
    
    describe('#constructor', function() {
        
        it('should have a strategy property with a compose method', function() {
            let builder = new PgQueryBuilder();
            expect(builder).to.have.property('strategy');
            expect(builder.strategy).to.be.an.instanceof(PgJSONBQueryStrategy);
        });

    });

    describe('#compose', function() {
        
        beforeEach(function() {
            this.builder = new PgQueryBuilder();
            sinon.spy(this.builder.strategy, "compose");
        });

        it('should call the compose method of the strategy object', function() {
            let params = { dataType: {id: 1, name:'Test'}};
            this.builder.compose(params);
            expect(this.builder.strategy.compose.calledOnce);
        });

    });

});
