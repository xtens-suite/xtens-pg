/**
 * @module
 * @name PgCrudStrategies
 * @author Massimiliano Izzo
 * @description this handler works as a context for the transaction strategy
 *
 */
/*jshint node: true */
/*jshint esnext: true */
"use strict";

const PG = 'pg';
const START_SAMPLE_CODE = '080001';
const SUBJ_CODE_PREFIX = 'CPN-';

let BluebirdPromise = require('bluebird');
let FileSystemManager = require('xtens-fs').FileSystemManager;
let _ = require("lodash");
let QueryStream = require('pg-query-stream');
let pgp = require('pg-promise')();
let InvalidFormatError = require('xtens-utils').Errors.InvalidFormatError;
let TransactionError = require('xtens-utils').Errors.TransactionError;

let pgData2DataFileAssociationMap = new Map();

pgData2DataFileAssociationMap.set('sample', {
    table: 'datafile_samples__sample_files',
    data: 'sample_files',
    dataFile: 'datafile_samples'
});

pgData2DataFileAssociationMap.set('data', {
    table: 'data_files__datafile_data',
    data: 'data_files',
    dataFile: 'datafile_data'
});

/**
 * @private
 * @description evaluate whether the metadata field has a measure unit. Only numeric values are allowed a unit.
 * @return {boolean} - true if the metadata field has unit
 */
function isUnitAllowed(field, fieldInstance) {
    // if its not numeric return false
    if (field.field_type !== 'Integer' && field.field_type !== 'Float') return false;
    if (!field.has_unit) return false;
    if (field.unit || _.isArray(field.units)) return true;
    return false;
}

/**
 * @class
 * @private
 * @description Invalid Format error
 *
class InvalidFormatError extends Error {

    constructor(message) {
        super();
        this.name = "InvalidFormatError";
        this.message = (message || "");
    }

} */

/**
 * @class
 * @private
 * @description Transaction error
 *
class TransactionError extends Error {

    constructor(message) {
        super();
        this.name = "TransactionError";
        this.message = (message || "");
    }

} */

/**
 *  @method
 *  @description from camelCase to under_score
 */
String.prototype.toUnderscore = function(){
    return this.replace(/([A-Z])/g, function($1){return "_"+$1.toLowerCase();});
};

/**
 * @class
 * @name PgCrudStrategy
 * @description abstract class for crud strategy
 */
class PgCrudStrategy {

    /**
     * @constructor
     * @param{Object} dbConnection
     * @param{Object} fsConnection
     */
    constructor(dbConnection, fsConnection) {
        console.log("PgCrudStrategy - FS Connection: ");
        console.log(fsConnection);
        if (!dbConnection || !dbConnection.adapter) {
            throw new Error("You must specify a valid database connection (according to sails.js connection format)");
        }
        if (!fsConnection) {
            throw new Error("You must specify a valid database connection (according to sails.js connection format)");
        }
        this.fileSystemManager = BluebirdPromise.promisifyAll(new FileSystemManager(fsConnection));
    }

    get fileSystemManager() {
        return this._fileSystemManager;
    }

    set fileSystemManager(fileSystemManager) {
        if (fileSystemManager) {
            this._fileSystemManager = fileSystemManager;
        }
    }

}

class PgPromiseCrudStrategy extends PgCrudStrategy {


    /**
     * @constructor
     * @param{Object} dbConnection
     * @param{Object} fsConnection
     */
    constructor(dbConnection, fsConnection) {
        super(dbConnection, fsConnection);

        this.db = pgp({
            host: dbConnection.host,
            port: dbConnection.port,
            user: dbConnection.user,
            password: dbConnection.password,
            database: dbConnection.database
        });
    }

    get pgp() {
        return this._pgp;
    }

    set pgp(pgp) {
        if (pgp) {
            this._pgp = pgp;
        }
    }

    queryStream(statement, parameters, next) {

        let query = new QueryStream(statement, parameters);
        return this.db.stream(query, next);
    }
}

class PgKnexCrudStrategy extends PgCrudStrategy {

    /**
     * @constructor
     * @param{Object} dbConnection
     * @param{Object} fsConnection
     */
    constructor(dbConnection, fsConnection) {
        super(dbConnection, fsConnection);

        this.knex = require('knex')({
            client: PG,
            connection: {
                host: dbConnection.host,
                port: dbConnection.port,
                user: dbConnection.user,
                password: dbConnection.password,
                database: dbConnection.database
            }
        });

    }

    get knex() {
        return this._knex;
    }

    set knex(knex) {
        if (knex) {
            this._knex = knex;
        }
    }

    /**
     * @method
     * @name createDataType
     * @description transactional DataType creation
     */
    createDataType(dataType) {
        let knex = this.knex;
        let createdDataType;

        return knex.transaction(function(trx) {
          // create the new SuperType instance if superType are present and superType has not id (duplication)
            return BluebirdPromise.try(function() {
                if (!dataType.superType || (dataType.superType && dataType.superType.id)) {
                    return;
                }
                else {
                    return knex.returning('*').insert({
                        'name': dataType.superType.name,
                        'uri': dataType.superType.uri,
                        'schema': dataType.superType.schema,
                        'created_at': new Date(),
                        'updated_at': new Date()
                    }).into('super_type').transacting(trx);
                }
            })

          .then(function(rows) {
              let createdSuperType = rows && _.mapKeys(rows[0], (value, key) => { return _.camelCase(key); });
              dataType.superType = createdSuperType ? createdSuperType.id : dataType.superType.id ;
              return knex.returning('*').insert({
                  'name': dataType.name,
                  'model': dataType.model,
                  'project': dataType.project,
                  'super_type': dataType.superType,
                  'created_at': new Date(),
                  'updated_at': new Date()
              }).into('data_type').transacting(trx)

            .then(function(rows) {
                createdDataType = _.mapKeys(rows[0], (value, key) => { return _.camelCase(key); });
                return BluebirdPromise.map(dataType.parents || [], function(idParent) {
                    // NOTE: for some reason the column nomenclature is inverted here (must be preserved for Sails associations to work)
                    return knex.returning('id').insert({
                        'datatype_parents': createdDataType.id,
                        'datatype_children': idParent
                    }).into('datatype_children__datatype_parents').transacting(trx);
                });
            });
          });
        })

        .then(function() {
            return _.assign(dataType, createdDataType);
        })

        .catch(function(error) {
            throw new TransactionError(error.message);
        });

    }

    /**
     * @method
     * @name updateDataType
     * @description transactional DataType creation
     *
     */
    updateDataType(dataType) {
        let knex = this.knex, updatedDataType;

        return knex.transaction(function(trx) {

          // Update or create Super Type
            return BluebirdPromise.try(function() {
                console.log("KnexStrategy.updateDataType - trying to create/edit SuperType: " + dataType.superType);

              // if no superType is provided just skip this step (no creation/update)
                if (!_.isObject(dataType.superType)) {
                    return;
                }

              // you have to create a new super_type entity (i.e. row)
                else if(!dataType.superType.id) {
                    return knex.returning('*').insert({
                        'name': dataType.superType.name,
                        'uri': dataType.superType.uri,
                        'schema': dataType.superType.schema,
                        'created_at': new Date(),
                        'updated_at': new Date()
                    }).into('super_type').transacting(trx);
                }

              // otherwise update super_type
              else {
                    return knex('super_type').returning('*').where('id','=',dataType.superType.id).update({
                        'name': dataType.superType.name,
                        'uri': dataType.superType.uri,
                        'schema': dataType.superType.schema,
                        'updated_at': new Date()
                    }).transacting(trx);
                }

            })

          // update DataType entity
          .then(function(rows) {
              console.log("KnexStrategy.updateDataType - updating DataType...");
              console.log(rows);

              if(rows && rows[0]) {
                  let updatedSuperType = _.mapKeys(rows[0], (value, key) => { return _.camelCase(key); });
                  dataType.superType = updatedSuperType.id;
                  // console.log(updatedPersonalDetails);
                  // console.log(dataType.superType);
              }

              return knex('data_type').returning('*').where('id','=',dataType.id).update({
                  'name': dataType.name,
                  'super_type': dataType.superType,
                // 'project': dataType.project, // can not change project
                  'updated_at': new Date()
              }).transacting(trx)

            // find all the existing associations
            .then(function(rows) {
                updatedDataType = _.mapKeys(rows[0], (value, key) => { return _.camelCase(key); });
                return knex('datatype_children__datatype_parents').where('datatype_parents', dataType.id).transacting(trx);
            })

            .then(function(foundDTAssociations) {

                return BluebirdPromise.map(dataType.parents || [], function(idParent) {

                    // if the associations exists, leave it alone
                    if (_.findWhere(foundDTAssociations, {'datatype_children': idParent})) {
                        console.log("KnexStrategy.updateDataType - child-parent association found: " + dataType.id + "-" + idParent);
                        return;
                    }

                    // otherwise insert a new association (no support for deleting associations is currently provided for consistency)
                    else {
                        return knex.returning('id').insert({
                            'datatype_parents': dataType.id,
                            'datatype_children': idParent
                        }).into('datatype_children__datatype_parents').transacting(trx);
                    }

                });

            });
          });
        })

        .then(function() {
            return _.assign(dataType, updatedDataType);
        })

        .catch(function(error) {
            throw new TransactionError(error.message);
        });

    }

    /**
     * @method
     * @name deleteDataType
     * @description transactional dataType delete
     * @param{integer} id - dataType ID
     */
    deleteDataType(id) {
        return this.knex('data_type').where('id', id).del()
        .then(function(res) {
            return res;
        })
        .catch(function(error) {
            throw new TransactionError(error.message);
        });
    }

    /**
     * @method
     * @name findData
     * @param{Object} criteria, may contain the following parameters:
     *                  - idOperator [integer]: the ID of the operator doing the current request
     *                  - model [string]: the MODEL of the dataTypes
     *                  - project [integer]: the ID of the working project
     *                  - parentDataType [integer]: the ID of the parent dataType
     *                  - idDataTypes [Array<integer>/String]: an array of the IDs of the allowed dataTypes
     *                  - privilegeLevel [enum]: can be one of {view_overview, view_details, download, edit} (ordered list)
     */
    findData(criteria) {

        let knex = this.knex, query ={};
        let model = criteria.model.toLowerCase();
        let modelType= model+'.type';
        switch (criteria.model) {
            case "Data":
                query = knex.distinct('data.id', 'data.type', 'data.metadata', 'data_type.project', 'data_type.name', 'datatype_privileges.privilege_level', 'data.notes', 'data.tags', 'data.parent_subject', 'data.parent_data', 'data.parent_sample', 'data.created_at', 'data.updated_at').select().from('data');
                if (criteria.parentData) {
                    query.where('data.parent_data', criteria.parentData);
                }
                break;

            case "Subject":
                query = knex.distinct('subject.id', 'subject.type', 'subject.metadata', 'data_type.project', 'data_type.name', 'datatype_privileges.privilege_level', 'subject.notes', 'subject.tags', 'subject.code', 'subject.sex', 'subject.created_at', 'subject.updated_at').select().from('subject');

                if (criteria.canAccessPersonalData) {
                    query.distinct('personal_details.given_name', 'personal_details.surname', 'personal_details.birth_date', 'personal_details.id as pd_id');
                    query.innerJoin('personal_details', 'subject.personal_info', 'personal_details.id');
                }else {
                    query.distinct('subject.personal_info');
                }
                break;

            case "Sample":
                query = knex.distinct('sample.id', 'sample.type', 'sample.metadata', 'data_type.project', 'data_type.name', 'subject.code', 'datatype_privileges.privilege_level', 'sample.notes', 'sample.tags', 'sample.parent_subject', 'sample.biobank', 'biobank.acronym', 'sample.biobank_code', 'sample.parent_sample', 'sample.created_at', 'sample.updated_at').select().from('sample');

                query.innerJoin('biobank', 'sample.biobank', 'biobank.id');
                query.innerJoin('subject', 'sample.parent_subject', 'subject.id');
                break;

            default:
                query = knex.distinct().select().from('data');

        }
        query.innerJoin('data_type', modelType, 'data_type.id');
        query.innerJoin('datatype_privileges', 'data_type.id', 'datatype_privileges.data_type');
        query.innerJoin('xtens_group', 'xtens_group.id', 'datatype_privileges.xtens_group');
        query.innerJoin('group_members__operator_groups', 'xtens_group.id', 'group_members__operator_groups.group_members');
        query.innerJoin('operator', 'operator.id', 'group_members__operator_groups.operator_groups');

        query.where('operator.id', criteria.idOperator).andWhere('data_type.model', criteria.model);

        if (criteria.project) {
            query.where('data_type.project', criteria.project);
        }
        if (criteria.privilegeLevel) {
            query.where('datatype_privileges.privilege_level', '>=', criteria.privilegeLevel);
        }

        if (criteria.parentSample) {
            query.where(model+'.parent_sample', criteria.parentSample);
        }

        if (criteria.parentSubject || criteria.donor) {
            query.where(model+'.parent_subject', criteria.parentSubject ? criteria.parentSubject : criteria.donor);
        }

        if (criteria.type) {
            let idDataTypes = _.isArray(criteria.type) ? criteria.type : criteria.type.split(',').map(val => _.parseInt(val));
            query.whereIn('data_type.id', idDataTypes);
        }

        if (criteria.limit) {
            query.limit(criteria.limit);
        }

        if (criteria.skip) {
            query.offset(criteria.skip);
        }

        if (criteria.sort) {
            let sort = criteria.sort.split(" ");
            console.log(sort);
            query.orderBy(sort[0], sort[1]);
        }

        // added for debugging
        console.log(query.toString());

        return query.then(function(results) {
            return results;
        });

    }


    /**
     * @method
     * @name countData
     * @param{Object} criteria, may contain the following parameters:
     *                  - idOperator [integer]: the ID of the operator doing the current request
     *                  - model [string]: the MODEL of the dataTypes
     *                  - project [integer]: the ID of the working project
     *                  - parentDataType [integer]: the ID of the parent dataType
     *                  - idDataTypes [Array<integer>/String]: an array of the IDs of the allowed dataTypes
     *                  - privilegeLevel [enum]: can be one of {view_overview, view_details, download, edit} (ordered list)
     */
    countData(criteria) {
        let knex = this.knex;
        let model = criteria.model.toLowerCase();
        let modelType= model+'.type';
        let query = knex(model).count();
        query.innerJoin('data_type', modelType, 'data_type.id');
        query.innerJoin('datatype_privileges', 'data_type.id', 'datatype_privileges.data_type');
        query.innerJoin('xtens_group', 'xtens_group.id', 'datatype_privileges.xtens_group');
        query.innerJoin('group_members__operator_groups', 'xtens_group.id', 'group_members__operator_groups.group_members');
        query.innerJoin('operator', 'operator.id', 'group_members__operator_groups.operator_groups');

        query.where('operator.id', criteria.idOperator).andWhere('data_type.model', criteria.model);

        if (criteria.project) {
            query.where('data_type.project', criteria.project);
        }
        if (criteria.privilegeLevel) {
            query.where('datatype_privileges.privilege_level', '>=', criteria.privilegeLevel);
        }

        if (criteria.parentData) {
            query.where(model+'.parent_data', criteria.parentData);
        }

        if (criteria.parentSample) {
            query.where(model+'.parent_sample', criteria.parentSample);
        }

        if (criteria.parentSubject || criteria.donor) {
            query.where(model+'.parent_subject', criteria.parentSubject ? criteria.parentSubject : criteria.donor);
        }

        if (criteria.type) {
            let idDataTypes = _.isArray(criteria.type) ? criteria.type : criteria.type.split(',').map(val => _.parseInt(val));
            query.whereIn('data_type.id', idDataTypes);
        }

        return query.then(function(count) {
            return count[0] && count[0].count ? _.parseInt(count[0].count) : null;
        });
    }

    /**
     * @method
     * @name getDataTypesByRolePrivileges
     * @param{Object} criteria, may contain the following parameters:
     *                  - idOperator [integer]: the ID of the operator doing the current request
     *                  - model [string]: the MODEL of the dataTypes
     *                  - parentDataType [integer]: the ID of the parent dataType
     *                  - idDataTypes [Array<integer>/String]: an array of the IDs of the allowed dataTypes
     *                  - privilegeLevel [enum]: can be one of {view_overview, view_details, download, edit} (ordered list)
     */
    getDataTypesByRolePrivileges(criteria) {

        let knex = this.knex;
        let query = knex.select('data_type.id', 'data_type.name', 'super_type.schema', 'data_type.project', 'datatype_privileges.privilege_level').from('data_type');
        query.innerJoin('datatype_privileges', 'data_type.id', 'datatype_privileges.data_type');
        query.innerJoin('xtens_group', 'xtens_group.id', 'datatype_privileges.xtens_group');
        query.innerJoin('super_type', 'super_type.id', 'data_type.super_type');
        query.innerJoin('group_members__operator_groups', 'xtens_group.id', 'group_members__operator_groups.group_members');
        query.innerJoin('operator', 'operator.id', 'group_members__operator_groups.operator_groups');

        query.where('operator.id', criteria.idOperator).andWhere('data_type.model', criteria.model);

        if (criteria.project) {
            query.where('data_type.project', criteria.project);
        }

        if (criteria.privilegeLevel) {
            query.where('datatype_privileges.privilege_level', '>=', criteria.privilegeLevel);
        }

        if (criteria.parentDataType) {
            query.innerJoin('datatype_children__datatype_parents', 'data_type.id', 'datatype_children__datatype_parents.datatype_parents');
            query.where('datatype_children__datatype_parents.datatype_children', criteria.parentDataType);
        }

        if (criteria.idDataTypes) {
            let idDataTypes = _.isArray(criteria.idDataTypes) ? criteria.idDataTypes : criteria.idDataTypes.split(',').map(val => _.parseInt(val));
            query.whereIn('data_type.id', idDataTypes);
        }

        // added for debugging
        console.log(query.toString());

        return query.then(function(results) {
            return results;
        });

    }


    /**
     * @method
     * @name handleFiles
     * @description store files within a database transaction
     * @param{Array} files - the array of dataFiles, containing at lest a uri or name property
     * @param{integer} idData - the identifier of the data instance to create/update
     * @param{string} dataTypeName - the name of the dataType
     * @param{Object} trx - the current transaction object
     * @param{string} tableName - the table where the entity associated to the files is stored ('subject', 'sample' or 'data');
     * @return{BluebirdPromise} a bluebird promise object
     */
    handleFiles(files, idData, dataTypeName, trx, tableName) {

        let knex = this.knex;
        let fileSystemManager = this.fileSystemManager;
        tableName = tableName || 'data';

        return BluebirdPromise.map(files, function(file) {
            console.log("PgKnexCrudStrategy.handleFiles - handling file: ");
            console.log(file);
            return fileSystemManager.storeFileAsync(file, idData, dataTypeName);
        })

        // insert the DataFile instances on the database
        .then(function(results) {
            if (results.length) {   // if there are files store their URIs on the database
                console.log("PgKnexCrudStrategy.handleFiles - inserting files..");
                return knex.returning('id').insert(
                    _.each(files, function(file) { _.extend(file, { 'created_at': new Date(), 'updated_at': new Date()}); })
                ).into('data_file').transacting(trx);
            }
            else {  // else return an empty array
                return [];
            }
        })

        // create the associations between the Data instance and the DataFile instances
        .then(function(idFiles) {
            console.log(idFiles);
            console.log("PgKnexCrudStrategy.createData - creating associations...");
            return BluebirdPromise.map(idFiles, function(idFile) {

                let associationTable = pgData2DataFileAssociationMap.get(tableName);
                let association = {};
                association[associationTable.data] = idData;
                association[associationTable.dataFile] = idFile;
                return knex.insert(association).into(associationTable.table).transacting(trx);

            });
        });

    }

    /**
     * @method
     * @name createData
     * @description transactional Data creation with File Upload to the File System (e.g iRODS)
     * @param {Object} data - an xtens-app Data entity
     * @param {string} dataTypeName - the name of the DataType (used only for file storage)
     * @return idData the ID of the newly created Data
     */
    createData(data, dataTypeName) {
        let knex = this.knex, that = this;
        let files = data.files ? _.cloneDeep(data.files) : [];
        delete data.files;
        let createdData = null;

        // transaction-safe data creation
        return knex.transaction(function(trx) {
            console.log ("KnexStrategy.createData - creating new data instance...");
            console.log("KnexStrategy.createData - acquisition Date: " + data.date);

            // save the new Data entity
            return knex.returning('*').insert({
                'type': data.type,
                'tags': JSON.stringify(data.tags),
                'notes': data.notes,
                'metadata': data.metadata,
                'owner':data.owner,
                'acquisition_date': data.date,
                'parent_subject': data.parentSubject,
                'parent_sample': data.parentSample,
                'parent_data': data.parentData,
                'created_at': new Date(),
                'updated_at': new Date()
            }).into('data').transacting(trx)

            // store files on the FileSystem of choice (e.g. iRODS) in their final collection
            .then(function(rows) {
                createdData = _.mapKeys(rows[0], (value, key) => { return _.camelCase(key); });
                console.log("KnexStrategy.createData - data instance created with ID: " + createdData.id);
                return that.handleFiles(files, createdData.id, dataTypeName, trx);
            })

            .then(function() {
                createdData.files = _.map(files, file => {
                    return _.mapKeys(file, (value, key) => { return _.camelCase(key); });
                });
            });

        }) // Knex supports implicit commit/rollback
        .then(function() {
            console.log("KnexStrategy.createData: transaction committed for new Data: " + createdData.id);
            console.log(createdData);
            // _.each(createdData.files, console.log);
            return createdData;
        })
        .catch(function(error) {
            console.log("KnexStrategy.createData - error caught");
            console.log(error.stack);
            throw new TransactionError(error.message);
        });

    }


    /**
     * @method
     * @name updateData
     * @description transactional Data update. Files should not be changed after creation (at least in the current implementation are not)
     * @return{Object} updatedData - via Promise
     */
    updateData(data, dataTypeName) {
        let that = this, knex = this.knex, updatedData;
        let partitionedFiles = _.partition(data.files, file => {
            return !file.id;
        });
        let existingFiles = partitionedFiles[1], notExistingFiles = partitionedFiles[0];
        /*
        let files = data.files ? _.cloneDeep(_.filter(data.files, file => {
            return !file.id;
        })) : []; */
        console.log("KnexStrategy.updateData - new files to insert...");
        console.log(notExistingFiles);
        delete data.files;

        // transaction-safe Data update
        return knex.transaction(function(trx) {

            return knex('data').returning('*').where({
                'id': data.id,
                'type': data.type       // match dataType as well
            }).update({
                // NOTE: should I also update parent_subject, parent_sample and/or parent_data? Should it be proper/safe?
                'tags': JSON.stringify(data.tags),
                'notes': data.notes,
                'acquisition_date': data.date,
                'metadata': data.metadata,
                'updated_at': new Date()
            }).transacting(trx)

            .then(function(rows) {
                updatedData = _.mapKeys(rows[0], (value, key) => { return _.camelCase(key); });
                console.log("KnexStrategy.updateData - data instance updated for ID: " + updatedData.id);
                return that.handleFiles(notExistingFiles, updatedData.id, dataTypeName, trx);
            })

            .then(function() {
                notExistingFiles = _.map(notExistingFiles, file => {
                    return _.mapKeys(file, (value, key) => { return _.camelCase(key); });
                });
                data.files = existingFiles.concat(notExistingFiles);
            });

        })
        .then(function() {
            console.log("KnexStrategy.updateData: transaction committed updating Data with ID: " + updatedData.id);
            return updatedData;
        })
        .catch(function(error){
            console.log("KnexStrategy.updateData - error caught");
            console.log(error);
            throw new TransactionError(error.message);
        });
    }

    /**
     * @method
     * @name deleteData
     * @param{id}
     */
    deleteData(id) {
        return this.knex('data').where('id', id).del()
        .then(function(res) {
            return res;
        })
        .catch(function(error) {
            console.log(error);
            throw new TransactionError(error.message);
        });
    }

    /**
     * @method
     * @name createSample
     * @description transactional Sample creation with File upload to the File System (e.g. iRODS)
     * @return{integer} idSample - created sample ID
     */
    createSample(sample, sampleTypeName) {
        let that = this, knex = this.knex;
        let fileSystemManager = this.fileSystemManager;
        let files = sample.files ? _.cloneDeep(sample.files) : [];
        delete sample.files;
        let createdSample = null;

        // transaction-safe sample creation
        return knex.transaction(function(trx) {
            console.log ("KnexStrategy.createSample - creating new sample instance...");

            // fing the greatest (i.e. the last) inserted biobank_code for that biobank
            // TODO test this thing
            let query = knex.first('id', 'biobank_code').from('sample')
            .whereNull('parent_sample').andWhere('biobank', '=', sample.biobank)
            .orderBy('id', 'desc').transacting(trx);

            console.log(query.toString());

            return query

            // store the new Sample entity
            .then(function(lastSample) {
                console.log('PgKnexCrudStrategy.createSample - last primitive sample: ' + lastSample);
                let lastBiobankCode = lastSample ? _.parseInt(lastSample.biobank_code) : START_SAMPLE_CODE;
                let nextCode = _.isNaN(lastBiobankCode) ? START_SAMPLE_CODE : '0' + (lastBiobankCode + 1);
                console.log('PgKnexCrudStrategy.createSample - nextCode: ' + nextCode);
                let sampleCode = sample.biobankCode || nextCode || START_SAMPLE_CODE;
                return knex.returning('*').insert({
                    'biobank_code': sampleCode,
                    'type': sample.type,
                    'biobank': sample.biobank,
                    'parent_subject': sample.donor,
                    'owner':data.owner,
                    'parent_sample': sample.parentSample,
                    'tags': JSON.stringify(sample.tags),
                    'notes': sample.notes,
                    'metadata': sample.metadata,
                    'created_at': new Date(),
                    'updated_at': new Date()
                }).into('sample').transacting(trx);
            })

            // store files on the FileSystem of choice (e.g. iRODS) in their final collection
            .then(function(rows) {
                createdSample = _.mapKeys(rows[0], (value, key) => { return _.camelCase(key); });
                console.log("KnexStrategy.createData - data instance created with ID: " + createdSample.id);
                return that.handleFiles(files, createdSample.id, sampleTypeName, trx, 'sample');

            })
            // add stored files to the created sample entity
            .then(function() {
                createdSample.files = _.map(files, file => {
                    return _.mapKeys(file, (value, key) => {
                        return key === 'parent_subject' ? 'donor' : _.camelCase(key);
                    });
                });
            });

        }) // Knex supports implicit commit/rollback
        .then(function(inserts) {
            console.log("KnexStrategy.createSample: transaction committed for new Sample: " + createdSample.id);
            return createdSample;
        })
        .catch(function(error) {
            console.log("KnexStrategy.createSample - error caught");
            console.log(error);
            throw new TransactionError(error.message);
        });

    }

    /**
     * @method
     * @name updateSample
     * @description transaction-safe Sample update
     * @param {Object} sample - a Sample entity
     * @return idSample the ID of the updated Sample
     */
    updateSample(sample, sampleTypeName) {
        let that = this, knex = this.knex, updatedSample;
        let partitionedFiles = _.partition(sample.files, file => {
            return !file.id;
        });
        console.log(partitionedFiles);
        let existingFiles = partitionedFiles[1], notExistingFiles = partitionedFiles[0];
        /*
        let files = sample.files ? _.cloneDeep(_.filter(sample.files, file => {
            return !file.id;
        })) : []; */

        return knex.transaction(function(trx) {

            return knex('sample').returning('*').where({
                'id': sample.id,
                'type': sample.type // you must match also the correct sampleType
            }).update({
                'biobank': sample.biobank,
                'parent_subject': sample.donor,
                'parent_sample': sample.parentSample,
                'tags': JSON.stringify(sample.tags),
                'notes': sample.notes,
                'metadata': sample.metadata,
                'updated_at': new Date()
            }).transacting(trx)

            // store files on the FileSystem of choice (e.g. iRODS) in their final collection
            .then(function(rows) {
                updatedSample = _.mapKeys(rows[0], (value, key) => { return _.camelCase(key); });
                console.log("KnexStrategy.updateSample - sample instance created with ID: " + updatedSample.id);
                return that.handleFiles(notExistingFiles, updatedSample.id, sampleTypeName, trx, 'sample');

            })
            // add stored files to the created sample entity
            .then(function() {
                notExistingFiles = _.map(notExistingFiles, file => {
                    return _.mapKeys(file, (value, key) => {
                        return key === 'parent_subject' ? 'donor' : _.camelCase(key);
                    });
                });
                updatedSample.files = existingFiles.concat(notExistingFiles);
            });

        })

        .then(function() {
            console.log("KnexStrategy.createSample: transaction committed for new Sample: " + updatedSample.id);
            return updatedSample;
        })
        .catch(function(error){
            console.log("KnexStrategy.updateSample - error caught");
            console.log(error);
            throw new TransactionError(error.message);
        });


    }

    /**
     * @method
     * @name deleteSample
     * @param{id} - sample ID
     */
    deleteSample(id) {
        return this.knex('sample').where('id', id).del()
        .then(function(res) {
            return res;
        })
        .catch(function(error) {
            throw new TransactionError(error.message);
        });
    }

    /**
     *  @method
     *  @name createSubject
     *  @description  transactional Subject creation
     */
    createSubject(subject, subjectTypeName) {
        let knex = this.knex;
        let idProjects = _.cloneDeep(subject.projects) || [];
        delete subject.projects;
        let createdSubject = null, createdPersonalDetails;

        return knex.transaction(function(trx) {
            console.log ("KnexStrategy.createSubject - creating new subject instance...");

            // create the new PersonalDetails instance (if personalDetails are present)
            return BluebirdPromise.try(function() {
                if (!subject.personalInfo) {
                    return;
                }
                else {
                    return knex.returning('*').insert({
                        'given_name': subject.personalInfo.givenName,
                        'surname': subject.personalInfo.surname,
                        'birth_date': subject.personalInfo.birthDate,
                        'created_at': new Date(),
                        'updated_at': new Date()
                    }).into('personal_details').transacting(trx);
                }
            })

            // get the last inserted SUBJECT id
            .then(function(rows) {
                createdPersonalDetails = rows && _.mapKeys(rows[0], (value, key) => { return _.camelCase(key); });
                subject.personalInfo = createdPersonalDetails && createdPersonalDetails.id;
                return knex.raw('SELECT last_value FROM subject_id_seq').transacting(trx);
            })

            // create the new Subject entity
            .then(function(result) {
                let lastId = result.rows && result.rows[0] && _.parseInt(result.rows[0].last_value);
                let subjCode = SUBJ_CODE_PREFIX + (lastId+1);
                console.log("KnexStrategy.createSubject - subject code: " + subjCode);
                return knex.returning('*').insert({
                    'code': subject.code || subjCode, // if a code is provided by the user use that
                    'sex': subject.sex || 'N.D.',
                    'type': subject.type,
                    'tags': JSON.stringify(subject.tags),
                    'notes': subject.notes,
                    'owner':data.owner,
                    'metadata': subject.metadata,
                    'personal_info': subject.personalInfo,
                    'created_at': new Date(),
                    'updated_at': new Date()
                }).into('subject').transacting(trx);
            });

            // create all the Subject-Project associations
            // .then(function(rows) {
            //     createdSubject = _.mapKeys(rows[0], (value, key) => { return _.camelCase(key); });
            //     console.log("KnexStrategy.createSubject - creating associations with projects...");
            //     return BluebirdPromise.map(idProjects, function(idProject) {
            //         return knex.insert({'project_subjects': idProject, 'subject_projects': createdSubject.id})
            //         .into('project_subjects__subject_projects').transacting(trx);
            //     });
            // });

        }) // Knex supports implicit commit/rollback
        .then(function(createdSubject) {
            console.log("KnexStrategy.createSubject: transaction committed for new Subject: " + createdSubject.id);
            return _.assign(createdSubject, {
                'personalInfo': createdPersonalDetails
                // 'projects': idProjects
            });
        })
        .catch(function(error) {
            console.log("KnexStrategy.createSample - error caught");
            console.log(error);
            throw new TransactionError(error.message);
        });

    }

    /**
     * @method
     * @name updateSubject
     * @description transaction-safe Subject update
     * @param {Object} subject - the Subject entity to be updated
     * @return idSubject
     */
    updateSubject(subject) {

        let knex = this.knex, updatedSubject, updatedPersonalDetails;
        let idProjects = _.cloneDeep(subject.projects) || [];
        delete subject.projects;

        return knex.transaction(function(trx) {

            // Update or create personal information
            return BluebirdPromise.try(function() {
                console.log("KnexStrategy.updateSubject - trying to create/edit PersonalInfo: " + subject.personalInfo);

                // if no personalInfo is provided just skip this step (no creation/update)
                if (!_.isObject(subject.personalInfo)) {
                    return;
                }

                // you have to create a new personal_details entity (i.e. row)
                else if(!subject.personalInfo.id) {
                    return knex.returning('*').insert({
                        'surname': subject.personalInfo.surname,
                        'given_name': subject.personalInfo.givenName,
                        'birth_date': subject.personalInfo.birthDate,
                        'created_at': new Date(),
                        'updated_at': new Date()
                    }).into('personal_details').transacting(trx);
                }

                // otherwise update personal_details
                else {
                    return knex('personal_details').returning('*').where('id','=',subject.personalInfo.id).update({
                        'surname': subject.personalInfo.surname,
                        'given_name': subject.personalInfo.givenName,
                        'birth_date': subject.personalInfo.birthDate
                    }).transacting(trx);
                }

            })

            // update Subject entity
            .then(function(rows) {
                console.log("KnexStrategy.updateSubject - updating Subject...");
                console.log(rows);

                if(rows && rows[0]) {
                    updatedPersonalDetails = _.mapKeys(rows[0], (value, key) => { return _.camelCase(key); });
                    subject.personalInfo = updatedPersonalDetails.id;
                    // console.log(updatedPersonalDetails);
                    // console.log(subject.personalInfo);
                }
                return knex('subject').returning('*').where({
                    'id': subject.id,
                    'type': subject.type
                }).update({
                    'code': subject.code || subjCode, // if a code is provided by the user use that
                    'sex': subject.sex || 'N.D.',
                    'personal_info': subject.personalInfo,
                    'tags': JSON.stringify(subject.tags),
                    'notes': subject.notes,
                    'metadata': subject.metadata,
                    'updated_at': new Date()
                }).transacting(trx);

            });

            // update Projects if present
            // first delete all existing Projects
            // .then(function(rows) {
            //     updatedSubject = _.mapKeys(rows[0], (value, key) => { return _.camelCase(key); });
            //     console.log("KnexStrategy.updateSubject - dissociating projects for Subject ID: " + updatedSubject.id);
            //     return knex('project_subjects__subject_projects').where('subject_projects','=',updatedSubject.id).del().transacting(trx);
            // })
            //
            // // then insert all listed Projects
            // .then(function() {
            //     console.log("KnexStrategy.updateSubject - associating projects for Subject ID: " + updatedSubject.id);
            //     return BluebirdPromise.map(idProjects, function(idProject) {
            //         return knex.insert({'project_subjects': idProject, 'subject_projects': updatedSubject.id})
            //         .into('project_subjects__subject_projects').transacting(trx);
            //     });
            // });

        })
        .then(function(updatedSubject) {
            console.log('KnexStrategy.updateSubject - transaction commited for updating subject with ID:' + updatedSubject.id);
            return _.assign(updatedSubject, {
                'personalInfo': updatedPersonalDetails
                // 'projects': idProjects
            });
        })
        .catch(function(error) {
            console.log("KnexStrategy.createSample - error caught");
            console.log(error);
            throw new TransactionError(error.message);
        });

    }

    /**
     * @method
     * @name deleteData
     * @param{id}
     */
    deleteSubject(id) {
        // TODO should personalDetails be deleted as well??
        return this.knex('subject').where('id', id).del().then(function(res) {
            return res;
        })
        .catch(function(error) {
            throw new TransactionError(error.message);
        });
    }


    /**
     *  @method
     *  @name putMetadataFieldsIntoEAV
     *  @description extract the Metadata Fields from the JSON schema and stores each one in a dedicated
     *              ATTRIBUTE table, for use in an EAV catalogue
     *  @param {integer} idDataType - the identifier of the DataType (i.e. ENTITY)
     *  @param {Array} fields - the array containing all the MetadataFields to be inserted (or updated?)
     *  @param {boolean} useFormattedNames - if true use the formatted name
     *  TODO check the use of formatted names
     *
     */
    putMetadataFieldsIntoEAV(idDataType, fields, useFormattedNames) {

        let knex = this.knex;

        return knex.transaction(function(trx) {

            // for each metadata field
            return BluebirdPromise.map(fields, function(field) {

                // insert the new metadata field
                return knex('eav_attribute').where({
                    'data_type': idDataType,
                    'name': field.name
                }).transacting(trx)

                .then(function(found) {
                    console.log("KnexStrategy.putMetadataFieldsIntoEAV - found for field " + field.name + ": "  + found);
                    if (_.isEmpty(found)) {
                        console.log("KnexStrategy.putMetadataFieldsIntoEAV - inserting field " + field.name);
                        return knex.returning('id').insert({
                            'data_type': idDataType,
                            'name': useFormattedNames ? field.formattedName : field.name, // notice: this must be tested - by Massi
                            'field_type': field.fieldType,
                            'has_unit': field.hasUnit,
                            'created_at': new Date(),
                            'updated_at': new Date()
                        }).into('eav_attribute').transacting(trx);
                    }
                });

            });

        })

        .then(function(insertedIds) {
            console.log('KnexStrategy.putMetadataFieldsIntoEAV - transaction commited for DataType:' + idDataType);
            return _.flatten(insertedIds);
        })

        .catch(function(error) {
            console.log("KnexStrategy.putMetadataFieldsIntoEAV - error caught");
            console.log(error);
            throw new TransactionError("Transaction could not be completed. Some error occurred");
        });
    }


    /**
     * @method
     * @name putMetadataValuesIntoEAV
     * @description extract the metadata values from the "metadata" column of the "data" ("subject" or "sample") entity,
     * and store it in the appropriate EAV Value(s) table. Five value tables are provided, one for each fundamental data type (text, integer, float,
     * date and boolean)
     * @param {Object} data -  the Data (Subject, Sample or Generic) that must extracted and loadad in the EAV catalogue
     *
     */
    putMetadataValuesIntoEAV(data, eavValueTableMap) {

        console.log("KnexStrategy.putMetadataValuesIntoEAV - eavValueTableMap: " + eavValueTableMap);
        let knex = this.knex;
        return knex.transaction(function(trx) {

            return knex('data_type').where({id: data.type}).first('model').transacting(trx)

            .then(function(row) {

                // identify the table (e.g. data, subject, sample...)
                let entityTable = row.model.toLowerCase().toUnderscore();
                console.log("KnexStrategy.putMetadataValuesIntoEAV - entity table is: " + entityTable);

                // store each metadata value in the appropriate EAV catalogue
                return BluebirdPromise.map(Object.keys(data.metadata), function(metadataField) {

                    console.log("KnexStrategy.putMetadataValuesIntoEAV - trying to retrieve the metadataField: " + metadataField);
                    // find the attribute
                    return knex('eav_attribute').where({
                        'data_type': data.type,
                        'name': metadataField
                    }).transacting(trx)

                    //
                    .then(function(eavAttribute) {
                        if (eavAttribute.length !== 1) {
                            throw new Error("none or more than one attribute was restrieved!!");
                        }

                        eavAttribute = eavAttribute[0];
                        let eavValueTable;
                        console.log("KnexStrategy.putMetadataValuesIntoEAV - eavAttribute: " + eavAttribute);
                        // if the metadata has a single field value, insert it!
                        if (data.metadata[metadataField].value) {
                            console.log("KnexStrategy.putMetadataValuesIntoEAV - field " + metadataField + " is a single attribute");
                            let eavValue = {
                                // 'entity_table': table,
                                'entity': data.id,
                                'attribute': eavAttribute.id,
                                'value': data.metadata[metadataField].value,
                                'created_at': new Date(),
                                'updated_at': new Date()
                            };

                            if (isUnitAllowed(eavAttribute, data.metadata[metadataField])) {
                                console.log ("KnexStrategy.putMetadataValuesIntoEAV - unit allowed for field: " + metadataField);
                                eavValue.unit = data.metadata[metadataField].unit;
                            }
                            eavValueTable = eavValueTableMap[eavAttribute.field_type] + '_' + entityTable;
                            console.log("KnexStrategy.putMetadataValuesIntoEAV - eavAttribute: " + eavAttribute);
                            console.log("KnexStrategy.putMetadataValuesIntoEAV - inserting new value into table " + eavValueTable);
                            return knex.returning('id').insert(eavValue).into(eavValueTable).transacting(trx);
                        }

                        // otherwise it is a loop!!
                        else if (_.isArray(data.metadata[metadataField].values)) {
                            console.log("KnexStrategy.putMetadataValuesIntoEAV - field " + metadataField + " is a loop");
                            let unitAllowed = isUnitAllowed(data.metadata[metadataField], eavAttribute);
                            let eavValues = data.metadata[metadataField].values.map(function(value, index) {
                                let instance = {
                                    'entity': data.id,
                                    'attribute': eavAttribute.id,
                                    'value': value,
                                    'created_at': new Date(),
                                    'updated_at': new Date()
                                };
                                if (unitAllowed) {
                                    console.log ("KnexStrategy.putMetadataValuesIntoEAV - unit allowed for field: " + metadataField);
                                    instance.unit = data.metadata[metadataField].units[index];
                                }
                                return instance;
                            });
                            eavValueTable = eavValueTableMap[eavAttribute.field_type] + '_' + entityTable;
                            console.log("KnexStrategy.putMetadataValuesIntoEAV - inserting new value into table " + eavValueTable);
                            return knex.returning('id').insert(eavValues).into(eavValueTable).transacting(trx);
                        }

                        // something is wrong, throw new error
                        else {
                            console.log("KnexStrategy.putMetadataValuesIntoEAV - metadata field" + metadataField +
                                        "missing or it does not possess a valid value");
                        }
                    });

                })

                .then(function(ids) {

                    console.log("KnexStrategy.putMetadataValuesIntoEAV - inserted successfully new metadata value: IDS " + ids);
                    return ids;
                });
            })

            .then(function(insertedIds) {
                // console.log('KnexStrategy.putMetadataValuesIntoEAV - inserted values'+ insertedIds);
                return _.flatten(insertedIds);
            })

            .catch(function(error) {
                console.log("KnexStrategy.putMetadataValuesIntoEAV - error caught");
                console.log(error);
                throw new TransactionError("Transaction could not be completed. Some error occurred");
            });

        });

    }


}
module.exports.PgCrudStrategy = PgCrudStrategy;
module.exports.PgKnexCrudStrategy = PgKnexCrudStrategy;
module.exports.PgPromiseCrudStrategy = PgPromiseCrudStrategy;
