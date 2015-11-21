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
let PgKnexCrudStrategy = require('../../lib/crud/PgCrudStrategies.js').PgKnexCrudStrategy;
let FileSystemManager = require('xtens-fs').FileSystemManager;


/* a test connection according to sails format */
let dbConnection = require('../../config/local.js').connection;
let fsConnection = require('../../config/local.js').fsConnection;

// test data object
let dataObj = {
    files: [
        {uri: "/path/to/file01.ext"},
        {uri: "/another/path/to/file02.ext"},
        {uri: "/yet/another/path/to/file03.ext"}
    ],
    type: 2,
    date: new Date(),
    tags: ["tag", "another tag"],
    notes: "let me test you with knex",
    metadata: {
        attribute1: { value: ["test value"]},
        attribute2: { value: [1.0], unit: ["s"]}
    },
    parentData: undefined,
    parentSample: undefined,
    parentSubject: 1
};

describe('PgKnexCrudStrategy', function() {

    describe('#constructor', function() {

        
        it ("should create a new knex object with the proper connection", function() {
            let strategy = new PgKnexCrudStrategy(dbConnection, fsConnection);
            expect(strategy.knex).to.exist;
            expect(strategy.knex).to.have.property('select');
            expect(strategy.knex).to.have.property('insert');
            expect(strategy.knex).to.have.property('update');
        });

    });

    describe('#createData', function() {

        let strategy = new PgKnexCrudStrategy(dbConnection, fsConnection);
        
        before(function() {
            return strategy.knex('data').truncate()
            .then(function() {
                strategy.knex("data_file").truncate();
            }).then(function() {
                console.log("tables truncated");
            });
        });

        it("# should create the proper query strategy", function() {
            let mock = sinon.mock(FileSystemManager.prototype);
            let dataTypeName = "testDataType";
            /* TODO find a way to mock connections to DB and FileSystem
            return strategy.createData(dataObj, dataTypeName).then(function() {
                console.log("done");
            }).catch(function(err) {
                console.log("error");
            }) ;
           */
        });
    
    });

});
