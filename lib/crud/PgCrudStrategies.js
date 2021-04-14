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

const PG = 'pg';
const START_SAMPLE_CODE = '0';
const SUBJ_CODE_PREFIX = 'CPN-';

let BluebirdPromise = require('bluebird');
let FileSystemManager = require('xtens-fs').FileSystemManager;
let _ = require("lodash");
let QueryStream = require('pg-query-stream');
let pgp = require('pg-promise')();
let InvalidFormatError = require('xtens-utils').Errors.InvalidFormatError;
let TransactionError = require('xtens-utils').Errors.TransactionError;
const camelcaseKeys = require('camelcase-keys');

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
          alias: "dtdt"
      }]
  ]);

/**
 * @private
 * @description evaluate whether the metadata field has a measure unit. Only numeric values are allowed a unit.
 * @return {boolean} - true if the metadata field has unit
 */
function isUnitAllowed (field, fieldInstance) {
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
String.prototype.toUnderscore = function () {
    return this.replace(/([A-Z])/g, function ($1) {
        return "_" + $1.toLowerCase();
    });
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
    constructor (dbConnection, fsConnection) {
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

    get fileSystemManager () {
        return this._fileSystemManager;
    }

    set fileSystemManager (fileSystemManager) {
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
    constructor (dbConnection, fsConnection) {
        super(dbConnection, fsConnection);

        this.db = pgp({
            host: dbConnection.host,
            port: dbConnection.port,
            user: dbConnection.user,
            password: dbConnection.password,
            database: dbConnection.database
        });
    }

    get pgp () {
        return this._pgp;
    }

    set pgp (pgp) {
        if (pgp) {
            this._pgp = pgp;
        }
    }

    queryStream (statement, parameters, next) {
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
    constructor (dbConnection, fsConnection) {
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

    get knex () {
        return this._knex;
    }

    set knex (knex) {
        if (knex) {
            this._knex = knex;
        }
    }

    /**
   * @method
   * @name createDataType
   * @description transactional DataType creation
   */
    createDataType (dataType) {
        let knex = this.knex;
        let createdDataType;

        return knex.transaction(function (trx) {
        // create the new SuperType instance if superType are present and superType has not id (duplication)
            return BluebirdPromise.try(function () {
                if (!isNaN(dataType.superType) || (dataType.superType && dataType.superType.id)) {

                } else {
                    return knex.returning('*').insert({
                        'name': dataType.superType.name,
                        'uri': dataType.superType.uri,
                        'schema': dataType.superType.schema,
                        'skip_paging': dataType.superType.skipPaging,
                        'created_at': new Date(),
                        'updated_at': new Date()
                    }).into('super_type').transacting(trx);
                }
            })

                .then(function (rows) {
                    let createdSuperType = rows && _.mapKeys(rows[0], (value, key) => {
                        return _.camelCase(key);
                    });
                    dataType.superType = createdSuperType ? createdSuperType.id : !isNaN(dataType.superType) ? dataType.superType : dataType.superType.id;
                    return knex.returning('*').insert({
                        'name': dataType.name,
                        'model': dataType.model,
                        'biobank_prefix': dataType.biobankPrefix,
                        'parent_code': !!dataType.getParentCode,
                        'parent_no_prefix': !!dataType.ifParentNoPrefix,
                        'project': dataType.project,
                        'super_type': dataType.superType,
                        'created_at': new Date(),
                        'updated_at': new Date()
                    }).into('data_type').transacting(trx)

                        .then(function (rows) {
                            createdDataType = _.mapKeys(rows[0], (value, key) => {
                                return _.camelCase(key);
                            });
                            return BluebirdPromise.map(dataType.parents || [], function (idParent) {
                                // NOTE: for some reason the column nomenclature is inverted here (must be preserved for Sails associations to work)
                                return knex.returning('id').insert({
                                    'datatype_parents': createdDataType.id,
                                    'datatype_children': idParent
                                }).into('datatype_children__datatype_parents').transacting(trx);
                            });
                        });
                });
        })

            .then(function () {
                return _.assign(dataType, createdDataType);
            })

            .catch(function (error) {
                throw new TransactionError(error.message);
            });
    }

    /**
     * @method
     * @name updateDataType
     * @description transactional DataType creation
     *
     */
    updateDataType (dataType) {
        let knex = this.knex; let updatedDataType;

        return knex.transaction(function (trx) {
            // Update or create Super Type
            return BluebirdPromise.try(function () {
                console.log("KnexStrategy.updateDataType - trying to create/edit SuperType: " + dataType.superType);

                // if no superType is provided just skip this step (no creation/update)
                if (!_.isObject(dataType.superType)) {

                }

                // you have to create a new super_type entity (i.e. row)
                else if (!dataType.superType.id) {
                    return knex.returning('*').insert({
                        'name': dataType.superType.name,
                        'uri': dataType.superType.uri,
                        'schema': dataType.superType.schema,
                        'skip_paging': dataType.superType.skipPaging,
                        'created_at': new Date(),
                        'updated_at': new Date()
                    }).into('super_type').transacting(trx);
                }
                // otherwise update super_type
                else {
                    return knex('super_type').returning('*').where('id', '=', dataType.superType.id).update({
                        'name': dataType.superType.name,
                        'uri': dataType.superType.uri,
                        'schema': dataType.superType.schema,
                        'skip_paging': dataType.superType.skipPaging,
                        'updated_at': new Date()
                    }).transacting(trx);
                }
            })
            // update DataType entity
                .then(function (rows) {
                    console.log("KnexStrategy.updateDataType - updating DataType...");
                    console.log(rows);

                    if (rows && rows[0]) {
                        let updatedSuperType = _.mapKeys(rows[0], (value, key) => {
                            return _.camelCase(key);
                        });
                        dataType.superType = updatedSuperType.id;
                    }

                    return knex('data_type').returning('*').where('id', '=', dataType.id).update({
                        'name': dataType.name,
                        'super_type': dataType.superType,
                        'parent_no_prefix': !!dataType.ifParentNoPrefix,
                        // 'parent_code': dataType.getParentCode ? true : false
                        // 'biobank_prefix': dataType.biobankPrefix, //not editable
                        // 'project': dataType.project, // can not change project
                        'updated_at': new Date()
                    }).transacting(trx)

                    // find all the existing associations
                        .then(function (rows) {
                            updatedDataType = _.mapKeys(rows[0], (value, key) => {
                                return _.camelCase(key);
                            });
                            return knex('datatype_children__datatype_parents').where('datatype_parents', dataType.id).transacting(trx);
                        })

                        .then(function (foundDTAssociations) {
                            return BluebirdPromise.map(dataType.parents || [], function (idParent) {
                                // if the associations exists, leave it alone
                                if (_.findWhere(foundDTAssociations, {
                                    'datatype_children': idParent
                                })) {
                                    console.log("KnexStrategy.updateDataType - child-parent association found: " + dataType.id + "-" + idParent);
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

            .then(function () {
                return _.assign(dataType, updatedDataType);
            })

            .catch(function (error) {
                throw new TransactionError(error.message);
            });
    }

    /**
   * @method
   * @name deleteDataType
   * @description transactional dataType delete
   * @param{integer} id - dataType ID
   */
    deleteDataType (id) {
        return this.knex('data_type').where('id', id).del()
            .then(function (res) {
                return res;
            })
            .catch(function (error) {
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
    findData (criteria) {
        let knex = this.knex;
        let query = {};
        let model = criteria.model.toLowerCase();
        let modelType = model + '.type';
        switch (criteria.model) {
            case "Data":
                query = knex.select('data.id', 'data.type', 'data.metadata', 'data_type.project', 'data_type.name as data_type_name', 'datatype_privileges.privilege_level', 'data.notes', 'data.tags', 'data.acquisition_date as date', 'data.created_at', 'data.updated_at').from('data');
                if (criteria.parentData) {
                    query.innerJoin('data_childrendata__data_parentdata', function () {
                        this.on('data_childrendata__data_parentdata.data_childrenData', "=", parseInt(criteria.parentData));
                        this.andOn('data_childrendata__data_parentdata.data_parentData', "=", 'data.id');
                    });
                    query.select('data_childrendata__data_parentdata.data_childrenData');
                }
                break;

            case "Subject":
                query = knex.select('subject.id', 'subject.type', 'subject.metadata', 'data_type.project', 'data_type.name', 'datatype_privileges.privilege_level', 'subject.notes', 'subject.tags', 'subject.code', 'subject.sex', 'subject.created_at', 'subject.updated_at').from('subject');

                if (criteria.canAccessPersonalData) {
                    query.select('personal_details.given_name', 'personal_details.surname', 'personal_details.birth_date', 'personal_details.id as personal_details_id');
                    query.leftJoin('personal_details', 'subject.personal_info', 'personal_details.id');
                } else {
                    query.select('subject.personal_info');
                }

                if (criteria.subjectCode) {
                    query.where('subject.code', criteria.subjectCode);
                }

                if (criteria.sex) {
                    query.where('subject.sex', criteria.sex);
                }
                break;

            case "Sample":
                query = knex.select('sample.id', 'sample.type', 'sample.metadata', 'data_type.project', 'data_type.name', 'subject.id as donor', 'subject.code', 'datatype_privileges.privilege_level', 'sample.notes', 'sample.tags', 'sample.biobank', 'biobank.acronym', 'sample.biobank_code', 'sample.created_at', 'sample.updated_at').from('sample');
                let joinInfo = joinPoolingTables.get('sample_subject');

                query.innerJoin('biobank', 'sample.biobank', 'biobank.id');
                query.innerJoin(joinInfo.name, function () {
                    this.on(joinInfo.name + '.' + joinInfo.childColumn, '=', 'sample.id');
                    if (criteria.donor) {
                        this.andOn(joinInfo.name + '.' + joinInfo.parentColumn, '=', parseInt(criteria.donor));
                    }
                });
                query.innerJoin('subject', joinInfo.name + '.' + joinInfo.parentColumn, 'subject.id');
                if (criteria.biobankCode) {
                    query.where('sample.biobank_code', criteria.biobankCode);
                }
                break;

            default:
                query = knex.select().from('data');
        }
        query.innerJoin('data_type', modelType, 'data_type.id');
        query.innerJoin('super_type', 'super_type.id', 'data_type.super_type');
        query.innerJoin('datatype_privileges', 'data_type.id', 'datatype_privileges.data_type');
        query.innerJoin('xtens_group', 'xtens_group.id', 'datatype_privileges.xtens_group');
        query.innerJoin('group_members__operator_groups', 'xtens_group.id', 'group_members__operator_groups.group_members');
        query.innerJoin('operator', 'operator.id', 'group_members__operator_groups.operator_groups');

        query.where('operator.id', criteria.idOperator).andWhere('data_type.model', criteria.model).andWhere('super_type.skip_paging', '<>', true);
        if (criteria.project) {
            query.where('data_type.project', criteria.project);
        }
        if (criteria.privilegeLevel) {
            query.where('datatype_privileges.privilege_level', '>=', criteria.privilegeLevel);
        }

        if (criteria.parentSample) {
            let joinInfo = joinPoolingTables.get(model.toLowerCase() + '_sample');
            query.innerJoin(joinInfo.name, function () {
                this.on(joinInfo.name + '.' + joinInfo.childColumn, '=', model + '.id');
                this.andOn(joinInfo.name + '.' + joinInfo.parentColumn, '=', parseInt(criteria.parentSample));
            });
            query.select(joinInfo.name + '.' + joinInfo.parentColumn);
        }

        if (criteria.parentSubject) {
            let joinInfo = joinPoolingTables.get(model.toLowerCase() + '_subject');
            query.innerJoin(joinInfo.name, function () {
                this.on(joinInfo.name + '.' + joinInfo.childColumn, '=', model + '.id');
                this.andOn(joinInfo.name + '.' + joinInfo.parentColumn, '=', parseInt(criteria.parentSubject));
            });
            query.innerJoin('subject as sb', joinInfo.name + '.' + joinInfo.parentColumn, 'sb.id');
            query.select(joinInfo.name + '.' + joinInfo.parentColumn);
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

        return query.then(function (results) {
            return camelcaseKeys(results, {
                deep: false
            });
        }).catch((err) => {
            console.error(err);
            throw new Error(err);
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
    countData (criteria) {
        let knex = this.knex;
        let model = criteria.model.toLowerCase();
        let modelType = model + '.type';
        let query = knex(model).count();
        query.innerJoin('data_type', modelType, 'data_type.id');
        query.innerJoin('super_type', 'super_type.id', 'data_type.super_type');
        query.innerJoin('datatype_privileges', 'data_type.id', 'datatype_privileges.data_type');
        query.innerJoin('xtens_group', 'xtens_group.id', 'datatype_privileges.xtens_group');
        query.innerJoin('group_members__operator_groups', 'xtens_group.id', 'group_members__operator_groups.group_members');
        query.innerJoin('operator', 'operator.id', 'group_members__operator_groups.operator_groups');

        query.where('operator.id', criteria.idOperator).andWhere('data_type.model', criteria.model).andWhere('super_type.skip_paging', '<>', true);

        if (criteria.project) {
            query.where('data_type.project', criteria.project);
        }
        if (criteria.privilegeLevel) {
            query.where('datatype_privileges.privilege_level', '>=', criteria.privilegeLevel);
        }

        if (criteria.parentData) {
            query.innerJoin('data_childrendata__data_parentdata', function () {
                this.on('data_childrendata__data_parentdata.data_childrenData', "=", parseInt(criteria.parentData));
                this.andOn('data_childrendata__data_parentdata.data_parentData', "=", 'data.id');
            });
        }

        if (criteria.parentSample) {
            let joinInfo = joinPoolingTables.get(model.toLowerCase() + '_sample');
            query.innerJoin(joinInfo.name, function () {
                this.on(joinInfo.name + '.' + joinInfo.childColumn, '=', model + '.id');
                this.andOn(joinInfo.name + '.' + joinInfo.parentColumn, '=', parseInt(criteria.parentSample));
            });
        }

        if (criteria.parentSubject || criteria.donor) {
            let joinInfo = joinPoolingTables.get(model.toLowerCase() + '_subject');
            query.innerJoin(joinInfo.name, function () {
                this.on(joinInfo.name + '.' + joinInfo.childColumn, '=', model + '.id');
                this.andOn(joinInfo.name + '.' + joinInfo.parentColumn, '=', parseInt(criteria.parentSubject ? criteria.parentSubject : criteria.donor));
            });
        }

        if (criteria.type) {
            let idDataTypes = _.isArray(criteria.type) ? criteria.type : criteria.type.split(',').map(val => _.parseInt(val));
            query.whereIn('data_type.id', idDataTypes);
        }

        console.log(query.toString());

        return query.then(function (count) {
            return count[0] && count[0].count ? _.parseInt(count[0].count) : null;
        }).catch((err) => {
            console.error(err);
            throw new Error(err);
        });
    }

    /**
   * @method
   * @name getCountsForDashboard
   * @param{projectId} [integer]: the ID of the project doing the current request
   */
    getCountsForDashboard (projectId) {
        let knex = this.knex;
        let results = {};

        let queryDts = knex.count('data_type.id as value');
        queryDts.select('data_type.model as label');
        queryDts.from('data_type');
        queryDts.groupBy('label');

        let querySamples = knex.count('sample.id as value');
        querySamples.select('data_type.name as label');
        querySamples.from('sample');
        querySamples.innerJoin('data_type', 'data_type.id', 'sample.type');
        querySamples.groupBy('label');

        let queryData = knex.count('data.id as value');
        queryData.select('data_type.name as label');
        queryData.from('data');
        queryData.innerJoin('data_type', 'data_type.id', 'data.type');
        queryData.groupBy('label');

        if (projectId) {
            queryDts.where('data_type.project', projectId);
            querySamples.where('data_type.project', projectId);
            queryData.where('data_type.project', projectId);
        }

        // added for debugging
        console.log(queryDts.toString());
        console.log(querySamples.toString());
        console.log(queryData.toString());

        return queryDts.then(function (DataTypes) {
            results.DataTypes = DataTypes;
            return querySamples.then(function (Samples) {
                results.Samples = Samples;
                return queryData.then(function (Data) {
                    results.Data = Data;
                    return results;
                });
            });
        });
    }

    /**
   * @method
   * @name getInfoForBarChart
   * @param{dataTypeId} [integer]: the ID of the project doing the current request
   * @param{fieldName} [string]: the ID of the project doing the current request
   * @param{model} [integer]: the ID of the project doing the current request
   * @param{period} [string]: the ID of the project doing the current request
   */
    getInfoForBarChart (dataTypeId, fieldName, model, period) {
        let knex = this.knex;
        model = model.toLowerCase();

        let field = '(' + model + '.created_at)::date';
        if (fieldName !== 'created_at') {
            field = "(" + model + ".metadata->'" + fieldName + "'->>'value')::date";
        }

        let query = knex.count(model + '.id as value');
        query.from(model);
        query.innerJoin('data_type', 'data_type.id', model + '.type');
        query.where('data_type.id', dataTypeId);
        query.groupBy('date');
        query.orderBy('date');

        let selector = "";
        switch (period) {
            case 'allyear':
                selector = "date_part('year', " + field + ")";
                break;
            case 'year':
                selector = "CONCAT(date_part('year', " + field + "),'-',date_part('month', " + field + "))";
                query.andWhereRaw(field + " >  CURRENT_DATE - INTERVAL '1 year'");
                break;
            case 'month':
                selector = "CONCAT(date_part('month', " + field + "),'-',date_part('day', " + field + "))";
                query.andWhereRaw(field + " >  CURRENT_DATE - INTERVAL '1 month'");
                break;
            case 'week':
                selector = "CONCAT(date_part('month', " + field + "),'-',date_part('day', " + field + "))";
                query.andWhereRaw(field + " >  CURRENT_DATE - INTERVAL '1 week'");
                break;
            default:
                selector = "date_part('month', " + field + ")";
                query.andWhereRaw("date_part('year', " + field + ") = date_part('year', CURRENT_DATE)");
                break;
        }
        query.select(knex.raw(selector + ' as date'));

        // added for debugging
        console.log(query.toString());

        return query.then(function (results) {
            return results;
        });
    }

    getInfoForBarChartDatediff (
        fromModel, fromDataTypeId, fromFieldName, fromHasSample, fromIsChild,
        toModel, toDataTypeId, toFieldName, toHasSample, toIsChild,
        period) {

        //--------------VARIABLES
        let knex = this.knex;
        fromModel = fromModel.toLowerCase();
        toModel = toModel.toLowerCase();
        let fromMdAlias = "from_" + fromModel;
        let toMdAlias = "to_" + toModel;
        let joinFrom = joinPoolingTables.get(fromModel + "_subject");
        let joinTo = joinPoolingTables.get(toModel + "_subject");
        let joinSample = null;
        if (joinFrom == joinTo && fromDataTypeId == toDataTypeId) {
            toMdAlias = fromMdAlias;
        }
        let fromField = "(" + fromMdAlias + ".metadata->'" + fromFieldName + "'->>'value')::date";
        let toField = "(" + toMdAlias + ".metadata->'" + toFieldName + "'->>'value')::date";

        //--------------QUERY
        let query = knex.select("subject.code as subject_code");
        if ([fromModel, toModel].find(function(md) {
            return md == 'sample';
        })) {
            query.select((fromModel== 'sample' ? fromMdAlias : toMdAlias) + ".biobank_code as biobank_code");
        } else if (fromHasSample == 1 || toHasSample == 1) {
            joinSample = joinPoolingTables.get((fromHasSample == 1 ? fromModel : toModel) + "_sample");
            query.select("sample.biobank_code as biobank_code");
        } else {
            query.select(knex.raw("null as biobank_code"));
        }
        query.select(knex.raw(fromField + " as fromDate"));
        query.select(knex.raw(toField + " as toDate"));
        query.from("subject");

        //--------------DATATYPE FROM
        query.innerJoin(joinFrom.name + " as " + joinFrom.alias, joinFrom.alias + "." + joinFrom.parentColumn, "subject.id");
        query.innerJoin(fromModel + " as " + fromMdAlias, function() {
            this.on(fromMdAlias + ".id", "=", joinFrom.alias + "." + joinFrom.childColumn)
            .andOn(fromMdAlias + ".type", parseInt(fromDataTypeId))
        });

        //--------------DATATYPE TO 
        //ADD JOIN FOR TO-SUBJECT, ONLY IF FROM AND TO HAS DIFFERENT MODEL RELATION
        if (joinFrom != joinTo) {
            query.innerJoin(joinTo.name + " as " + joinTo.alias, joinTo.alias + "." + joinTo.parentColumn, "subject.id");
        }
        //ADD JOIN FOR TO DATATYPE, ONLY IF FROM AND TO ARE DIFFERENT DATA TYPES
        if (joinFrom != joinTo || fromDataTypeId != toDataTypeId) {
            query.innerJoin(toModel + " as " + toMdAlias, function() {
                this.on(toMdAlias + ".id", "=", joinTo.alias + "." + joinTo.childColumn)
                .andOn(toMdAlias + ".type", parseInt(toDataTypeId))
            });
        }

        //--------------SAMPLE JOIN (TO RETRIEVE BIOBANK CODE)
        if (joinSample) {
            if (fromHasSample == 1) {
                query.innerJoin(joinSample.name + " as " + joinSample.alias, joinSample.alias + "." + joinSample.childColumn, fromMdAlias + ".id");
                query.innerJoin("sample", joinSample.alias + "." + joinSample.parentColumn, "sample.id");
            } else {
                query.innerJoin(joinSample.name + " as " + joinSample.alias, joinSample.alias + "." + joinSample.parentColumn, toMdAlias + ".id");
                query.innerJoin("sample", joinSample.alias + "." + joinSample.parentColumn, "sample.id");
            }
        }

        //--------------JOIN: FROM IS CHILD OF TO
        if (fromIsChild) {
            joinFromChild = joinPoolingTables.get(fromModel + "_" + toModel);
            query.innerJoin(joinFromChild.name + " as " + joinFromChild.alias, function() {
                this.on(fromMdAlias + ".id", "=", joinFromChild.alias + "." + joinFromChild.childColumn)
                .andOn(toMdAlias + ".id", "=", joinFromChild.alias + "." + joinFromChild.parentColumn)
            }); 
        }

        //--------------JOIN: TO IS CHILD OF FROM
        if (toIsChild) {
            joinToChild = joinPoolingTables.get(toModel + "_" + fromModel);
            query.innerJoin(joinToChild.name + " as " + joinToChild.alias, function() {
                this.on(toMdAlias + ".id", "=", joinToChild.alias + "." + joinToChild.childColumn)
                .andOn(fromMdAlias + ".id", "=", joinToChild.alias + "." + joinToChild.parentColumn)
            }); 
        }

        //--------------WHERE AND ORDER
        query.whereRaw(fromField + " is not null");
        query.orderBy("date");

        //--------------PERIOD FIELD
        let selector = "";
        switch (period) {
            case 'allyear':
                selector = "date_part('year', " + fromField + ")";
                break;
            case 'year':
                selector = "CONCAT(date_part('year', " + fromField + "),'-',date_part('month', " + fromField + "))";
                query.andWhereRaw(fromField + " >  CURRENT_DATE - INTERVAL '1 year'");
                break;
            case 'month':
                selector = "CONCAT(date_part('month', " + fromField + "),'-',date_part('day', " + fromField + "))";
                query.andWhereRaw(fromField + " >  CURRENT_DATE - INTERVAL '1 month'");
                break;
            case 'week':
                selector = "CONCAT(date_part('month', " + fromField + "),'-',date_part('day', " + fromField + "))";
                query.andWhereRaw(fromField + " >  CURRENT_DATE - INTERVAL '1 week'");
                break;
            default:
                selector = "date_part('month', " + fromField + ")";
                query.andWhereRaw("date_part('year', " + fromField + ") = date_part('year', CURRENT_DATE)");
                break;
        }
        query.select(knex.raw(selector + ' as date'));

        // added for debugging
        console.log(query.toString());

        return query.then(function (results) {
            return results;
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
    getDataTypesByRolePrivileges (criteria) {
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

        return query.then(function (results) {
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
    handleFiles (files, idData, dataTypeName, trx, tableName) {
        let knex = this.knex;
        let fileSystemManager = this.fileSystemManager;
        tableName = tableName || 'data';

        return BluebirdPromise.map(files, function (file) {
            console.log("PgKnexCrudStrategy.handleFiles - handling file: ");
            console.log(file);
            return fileSystemManager.storeFileAsync(file, idData, dataTypeName);
        })

        // insert the DataFile instances on the database
            .then(function (results) {
                if (results.length) { // if there are files store their URIs on the database
                    console.log("PgKnexCrudStrategy.handleFiles - inserting files..");
                    return knex.returning('id').insert(
                        _.each(files, function (file) {
                            _.extend(file, {
                                'created_at': new Date(),
                                'updated_at': new Date()
                            });
                        })
                    ).into('data_file').transacting(trx);
                } else { // else return an empty array
                    return [];
                }
            })

        // create the associations between the Data instance and the DataFile instances
            .then(function (idFiles) {
                console.log(idFiles);
                console.log("PgKnexCrudStrategy.createData - creating associations...");
                return BluebirdPromise.map(idFiles, function (idFile) {
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
    createData (data, dataTypeName) {
        let knex = this.knex;
        let that = this;
        let files = data.files ? _.cloneDeep(data.files) : [];
        delete data.files;
        let createdData = null;

        // transaction-safe data creation
        return knex.transaction(function (trx) {
            console.log("KnexStrategy.createData - creating new data instance...");
            console.log("KnexStrategy.createData - acquisition Date: " + data.date);

            // save the new Data entity
            return knex.returning('*').insert({
                'type': data.type,
                'tags': JSON.stringify(data.tags),
                'notes': data.notes,
                'metadata': data.metadata,
                'owner': data.owner,
                'acquisition_date': data.date,
                'created_at': new Date(),
                'updated_at': new Date()
            }).into('data').transacting(trx)
                .then(function (rows) {
                    createdData = _.mapKeys(rows[0], (value, key) => {
                        return _.camelCase(key);
                    });
                    console.log("KnexStrategy.createData - data instance created with ID: " + createdData.id);
                    return BluebirdPromise.map(data.parentData || [], function (idParent) {
                        return knex.returning('id').insert({
                            'data_parentData': createdData.id,
                            'data_childrenData': idParent
                        }).into('data_childrendata__data_parentdata').transacting(trx);
                    });
                })
                .then(function () {
                    return BluebirdPromise.map(data.parentSample || [], function (idParent) {
                        return knex.returning('id').insert({
                            'data_parentSample': createdData.id,
                            'sample_childrenData': idParent
                        }).into('data_parentsample__sample_childrendata').transacting(trx);
                    });
                })
                .then(function () {
                    return BluebirdPromise.map(data.parentSubject || [], function (idParent) {
                        return knex.returning('id').insert({
                            'data_parentSubject': createdData.id,
                            'subject_childrenData': idParent
                        }).into('data_parentsubject__subject_childrendata').transacting(trx);
                    });
                })
            // store files on the FileSystem of choice (e.g. iRODS) in their final collection
                .then(function () {
                    return that.handleFiles(files, createdData.id, dataTypeName, trx);
                })

                .then(function () {
                    createdData.files = _.map(files, file => {
                        return _.mapKeys(file, (value, key) => {
                            return _.camelCase(key);
                        });
                    });
                });
        }) // Knex supports implicit commit/rollback
            .then(function () {
                console.log("KnexStrategy.createData: transaction committed for new Data: " + createdData.id);
                console.log(createdData);
                // _.each(createdData.files, console.log);
                return createdData;
            })
            .catch(function (error) {
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
    updateData (data, dataTypeName) {
        let that = this;
        let knex = this.knex;
        let updatedData;
        let partitionedFiles = _.partition(data.files, file => {
            return !file.id;
        });
        let existingFiles = partitionedFiles[1];
        let notExistingFiles = partitionedFiles[0];
        /*
    let files = data.files ? _.cloneDeep(_.filter(data.files, file => {
        return !file.id;
    })) : []; */
        console.log("KnexStrategy.updateData - new files to insert...");
        console.log(notExistingFiles);
        delete data.files;

        // transaction-safe Data update
        return knex.transaction(function (trx) {
            return knex('data').returning('*').where({
                'id': data.id,
                'type': data.type // match dataType as well
            }).update({
            // NOTE: should I also update parent_subject, parent_sample and/or parent_data? Should it be proper/safe?
                'tags': JSON.stringify(data.tags),
                'notes': data.notes,
                'acquisition_date': data.date,
                'metadata': data.metadata,
                'updated_at': new Date()
            }).transacting(trx)
                .then(function (rows) {
                    updatedData = _.mapKeys(rows[0], (value, key) => {
                        return _.camelCase(key);
                    });
                    console.log("KnexStrategy.updateData - data instance updated for ID: " + updatedData.id);
                    return knex('data_childrendata__data_parentdata').where('data_parentData', data.id).transacting(trx);
                })
                .then(function (foundDataAssociations) {
                    return BluebirdPromise.map(data.parentData || [], function (idParent) {
                        // if the associations exists, leave it alone
                        if (_.findWhere(foundDataAssociations, {
                            'data_childrenData': idParent
                        })) {
                            console.log("KnexStrategy.updateData - child-parent data association found: " + data.id + "-" + idParent);
                        }
                        // otherwise insert a new association (no support for deleting associations is currently provided for consistency)
                        else {
                            return knex.returning('id').insert({
                                'data_parentData': updatedData.id,
                                'data_childrenData': idParent
                            }).into('data_childrendata__data_parentdata').transacting(trx);
                        }
                    });
                })
                .then(function () {
                    return knex('data_parentsample__sample_childrendata').where('data_parentSample', data.id).transacting(trx);
                })
                .then(function (foundSampleAssociations) {
                    return BluebirdPromise.map(data.parentSample || [], function (idParent) {
                        // if the associations exists, leave it alone
                        if (_.findWhere(foundSampleAssociations, {
                            'sample_childrenData': idParent
                        })) {
                            console.log("KnexStrategy.updateData - child-parent sample association found: " + data.id + "-" + idParent);
                        }
                        // otherwise insert a new association (no support for deleting associations is currently provided for consistency)
                        else {
                            return knex.returning('id').insert({
                                'data_parentSample': updatedData.id,
                                'sample_childrenData': idParent
                            }).into('data_parentsample__sample_childrendata').transacting(trx);
                        }
                    });
                })
                .then(function () {
                    return knex('data_parentsubject__subject_childrendata').where('data_parentSubject', data.id).transacting(trx);
                })
                .then(function (foundSubjectAssociations) {
                    return BluebirdPromise.map(data.parentSubject || [], function (idParent) {
                        // if the associations exists, leave it alone
                        if (_.findWhere(foundSubjectAssociations, {
                            'subject_childrenData': idParent
                        })) {
                            console.log("KnexStrategy.updateData - child-parent subject association found: " + data.id + "-" + idParent);
                        }
                        // otherwise insert a new association (no support for deleting associations is currently provided for consistency)
                        else {
                            return knex.returning('id').insert({
                                'data_parentSubject': updatedData.id,
                                'subject_childrenData': idParent
                            }).into('data_parentsubject__subject_childrendata').transacting(trx);
                        }
                    });
                })
                .then(function () {
                    return that.handleFiles(notExistingFiles, updatedData.id, dataTypeName, trx);
                })

                .then(function () {
                    notExistingFiles = _.map(notExistingFiles, file => {
                        return _.mapKeys(file, (value, key) => {
                            return _.camelCase(key);
                        });
                    });
                    data.files = existingFiles.concat(notExistingFiles);
                });
        })
            .then(function () {
                console.log("KnexStrategy.updateData: transaction committed updating Data with ID: " + updatedData.id);
                return updatedData;
            })
            .catch(function (error) {
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
    deleteData (id) {
        return this.knex('data').where('id', id).del()
            .then(function (res) {
                return res;
            })
            .catch(function (error) {
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
    createSample (sample, sampleTypeName, project) {
        let that = this;
        let knex = this.knex;
        let fileSystemManager = this.fileSystemManager;
        let files = sample.files ? _.cloneDeep(sample.files) : [];
        delete sample.files;
        let createdSample = null;

        // transaction-safe sample creation
        return knex.transaction(function (trx) {
            console.log("KnexStrategy.createSample - creating new sample instance...");

            // fing the greatest (i.e. the last) inserted biobank_code for that biobank
            // TODO test this thing

            return that.getNextBiobankCode(sample, project, trx)
            // store the new Sample entity
                .then(function (sampleCode) {
                    return knex.returning('*').insert({
                        'biobank_code': sampleCode,
                        'type': sample.type,
                        'biobank': sample.biobank,
                        'owner': sample.owner,
                        'tags': JSON.stringify(sample.tags),
                        'notes': sample.notes,
                        'metadata': sample.metadata,
                        'created_at': new Date(),
                        'updated_at': new Date()
                    }).into('sample').transacting(trx);
                })
                .then(function (rows) {
                    createdSample = _.mapKeys(rows[0], (value, key) => {
                        return _.camelCase(key);
                    });
                    console.log("KnexStrategy.createSample - sample instance created with ID: " + createdSample.id);
                    return BluebirdPromise.map(sample.parentSample || [], function (idParent) {
                        return knex.returning('id').insert({
                            'sample_parentSample': createdSample.id,
                            'sample_childrenSample': idParent
                        }).into('sample_parentsample__sample_childrensample').transacting(trx);
                    });
                })
                .then(function () {
                    return BluebirdPromise.map(sample.donor || [], function (idParent) {
                        return knex.returning('id').insert({
                            'sample_donor': createdSample.id,
                            'subject_childrenSample': idParent
                        }).into('sample_donor__subject_childrensample').transacting(trx);
                    });
                })
            // store files on the FileSystem of choice (e.g. iRODS) in their final collection
                .then(function () {
                    return that.handleFiles(files, createdSample.id, sampleTypeName, trx, 'sample');
                })
            // add stored files to the created sample entity
                .then(function () {
                    createdSample.files = _.map(files, file => {
                        return _.mapKeys(file, (value, key) => {
                            return key === 'subject_childrenSample' ? 'donor' : _.camelCase(key);
                        });
                    });
                });
        }) // Knex supports implicit commit/rollback
            .then(function (inserts) {
                console.log("KnexStrategy.createSample: transaction committed for new Sample: " + createdSample.id);
                return createdSample;
            })
            .catch(function (error) {
                console.log("KnexStrategy.createSample - error caught");
                console.log(error);
                throw new TransactionError(error.message);
            });
    }

    getNextBiobankCode (sample, project, trx) {
        if (!project) {
            return BluebirdPromise.resolve(null);
        }
        if (sample.biobankCode) {
            return BluebirdPromise.resolve(sample.biobankCode);
        }
        let nextCode;
        let knex = this.knex;
        if (!sample.parentSample) {
            let queryParentPrefix = knex.first('data_type.parent_no_prefix').from('data_type').where('data_type.id', '=', sample.type);
            return queryParentPrefix.then((res) => {
                let query = knex.first('sample.id', 'biobank_code', 'data_type.biobank_prefix', 'data_type.parent_code', 'data_type.parent_no_prefix').from('data_type').leftOuterJoin(
                    'sample',
                    function () {
                        this.on('sample.type', 'data_type.id');
                        this.on('sample.biobank', '=', parseInt(sample.biobank));
                    }).leftOuterJoin(
                    'sample_parentsample__sample_childrensample',
                    function () {
                        this.on('sample.id', 'sample_parentsample__sample_childrensample.sample_parentSample');
                    }).whereNull('sample_parentsample__sample_childrensample.sample_childrenSample').andWhere('data_type.project', project);
                if (res.parent_no_prefix) {
                    query.whereRaw("biobank_code ~ '^[0-9\.]+$' = true");
                } else {
                    query.where('data_type.id', '=', sample.type);
                }
                query.orderBy('id', 'desc').transacting(trx);
                return query.then((result) => {
                    let lastBiobankCode = result && result.biobank_code ? result.biobank_code : START_SAMPLE_CODE;
                    if (!isNaN(lastBiobankCode)) {
                        if (result && result.biobank_prefix && !result.parent_no_prefix) {
                            return result.biobank_prefix + (_.parseInt(lastBiobankCode) + 1);
                        }
                        return '0' + (_.parseInt(lastBiobankCode) + 1);
                    } else {
                        var splittedCode = lastBiobankCode.split(result.biobank_prefix);
                        nextCode = _.parseInt(splittedCode[1]) + 1;
                        return result.biobank_prefix + nextCode;
                    }
                });
            });
        } else {
            let query = knex.first('sample.id', 'biobank_code', 'data_type.biobank_prefix', 'data_type.parent_code', 'data_type.parent_no_prefix').from('data_type').leftOuterJoin(
                'sample',
                function () {
                    this.on('sample.type', 'data_type.id');
                    this.on('sample.biobank', '=', parseInt(sample.biobank));
                }).where('data_type.id', '=', sample.type).andWhere('data_type.project', project)
                .orderBy('id', 'desc').transacting(trx);

            return query.then((result) => {
                console.log('PgKnexCrudStrategy.createSample - BiobankCodeHandler: ' + result);
                if (result.parent_code && (!result.parent_no_prefix || sample.parentSample)) {
                    nextCode = result.biobank_prefix + sample.parentSample;
                    return nextCode;
                }

                let lastBiobankCode = result && result.biobank_code ? result.biobank_code : START_SAMPLE_CODE;
                if (!isNaN(lastBiobankCode)) {
                    if (result && result.biobank_prefix) {
                        return result.biobank_prefix + (_.parseInt(lastBiobankCode) + 1);
                    }
                    return '0' + (_.parseInt(lastBiobankCode) + 1);
                } else {
                    var splittedCode = lastBiobankCode.split(result.biobank_prefix);
                    nextCode = _.parseInt(splittedCode[1]) + 1;
                    return result.biobank_prefix + nextCode;
                }
            });
        }
    }

    findByBiobankCode (biobankCode, project) {
        if (!biobankCode || !project) {
            return BluebirdPromise.resolve(null);
        }
        let knex = this.knex;
        let queryParentPrefix = knex.first('sample.id')
            .from('sample')
            .innerJoin('data_type', 'data_type.id', 'sample.type')
            .where('biobank_code', '=', biobankCode)
            .andWhere('data_type.project', '=', project);
        return queryParentPrefix.then((sampleId) => {
            return sampleId || {
                id: 0
            };
        }).catch(function (error) {
            console.log("KnexStrategy.findByBiobankCode - error caught");
            console.log(error);
            throw new Error(error.message);
        });
    }

    /**
   * @method
   * @name updateSample
   * @description transaction-safe Sample update
   * @param {Object} sample - a Sample entity
   * @return idSample the ID of the updated Sample
   */
    updateSample (sample, sampleTypeName) {
        let that = this;
        let knex = this.knex;
        let updatedSample;
        let partitionedFiles = _.partition(sample.files, file => {
            return !file.id;
        });
        console.log(partitionedFiles);
        let existingFiles = partitionedFiles[1];
        let notExistingFiles = partitionedFiles[0];
        /*
    let files = sample.files ? _.cloneDeep(_.filter(sample.files, file => {
        return !file.id;
    })) : []; */

        return knex.transaction(function (trx) {
            return knex('sample').returning('*').where({
                'id': sample.id,
                'type': sample.type // you must match also the correct sampleType
            }).update({
                'biobank': sample.biobank,
                'tags': JSON.stringify(sample.tags),
                'notes': sample.notes,
                'metadata': sample.metadata,
                'updated_at': new Date()
            }).transacting(trx)

            // store files on the FileSystem of choice (e.g. iRODS) in their final collection
                .then(function (rows) {
                    updatedSample = _.mapKeys(rows[0], (value, key) => {
                        return _.camelCase(key);
                    });
                    console.log("KnexStrategy.updateSample - sample instance created with ID: " + updatedSample.id);
                    return that.handleFiles(notExistingFiles, updatedSample.id, sampleTypeName, trx, 'sample');
                })
                .then(function () {
                    return knex('sample_parentsample__sample_childrensample').where('sample_parentSample', sample.id).transacting(trx);
                })
                .then(function (foundSampleAssociations) {
                    return BluebirdPromise.map(sample.parentSample || [], function (idParent) {
                        // if the associations exists, leave it alone
                        if (_.findWhere(foundSampleAssociations, {
                            'sample_childrenSample': idParent
                        })) {
                            console.log("KnexStrategy.updateData - child-parent sample association found: " + sample.id + "-" + idParent);
                        }
                        // otherwise insert a new association (no support for deleting associations is currently provided for consistency)
                        else {
                            return knex.returning('id').insert({
                                'sample_parentSample': updatedSample.id,
                                'sample_childrenSample': idParent
                            }).into('sample_parentsample__sample_childrensample').transacting(trx);
                        }
                    });
                })
                .then(function () {
                    return knex('sample_donor__subject_childrensample').where('sample_donor', sample.id).transacting(trx);
                })
                .then(function (foundSubjectAssociations) {
                    return BluebirdPromise.map(sample.donor || [], function (idParent) {
                        // if the associations exists, leave it alone
                        if (_.findWhere(foundSubjectAssociations, {
                            'subject_childrenSample': idParent
                        })) {
                            console.log("KnexStrategy.updateData - child-parent subject association found: " + sample.id + "-" + idParent);
                        }
                        // otherwise insert a new association (no support for deleting associations is currently provided for consistency)
                        else {
                            return knex.returning('id').insert({
                                'sample_donor': updatedSample.id,
                                'subject_childrenSample': idParent
                            }).into('sample_donor__subject_childrensample').transacting(trx);
                        }
                    });
                })
            // add stored files to the created sample entity
                .then(function () {
                    notExistingFiles = _.map(notExistingFiles, file => {
                        return _.mapKeys(file, (value, key) => {
                            return key === 'subject_childrenSample' ? 'donor' : _.camelCase(key);
                        });
                    });
                    updatedSample.files = existingFiles.concat(notExistingFiles);
                });
        })

            .then(function () {
                console.log("KnexStrategy.createSample: transaction committed for new Sample: " + updatedSample.id);
                return updatedSample;
            })
            .catch(function (error) {
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
    deleteSample (id) {
        return this.knex('sample').where('id', id).del()
            .then(function (res) {
                return res;
            })
            .catch(function (error) {
                throw new TransactionError(error.message);
            });
    }

    /**
   * @method
   * @name deleteSample
   * @param{id} - sample ID
   */

    getNextSubjectCode (subject, trx) {
        if (subject.code) { // if a code is provided by the user use that
            return BluebirdPromise.resolve(subject.code);
        }
        if (!subject.type) {
            return BluebirdPromise.resolve(null);
        }
        let knex = this.knex;
        return knex.raw('SELECT code FROM subject where type = ' + subject.type + 'order by id desc limit 1').transacting(trx)
            .then(function (result) {
                // NOTE: : //CODE STRUCTURE: PREFIX + NUMBER
                let subjCode = SUBJ_CODE_PREFIX + 1;
                if (result.rows && result.rows.length > 0) {
                    let splitted = result.rows && result.rows[0] && result.rows[0].code.split(/(\d+)/).filter(Boolean);
                    let prefix = splitted.slice(0, -1).join('');
                    let nextId = _.parseInt(splitted.slice(-1)) ? (_.parseInt(splitted.slice(-1)) + 1) : 1;
                    subjCode = prefix || SUBJ_CODE_PREFIX;
                    subjCode = subjCode + nextId;
                }
                return subjCode;
            });
    }

    /**
   *  @method
   *  @name createSubject
   *  @description  transactional Subject creation
   */
    createSubject (subject, subjectTypeName) {
        let knex = this.knex;
        let that = this;
        let idProjects = _.cloneDeep(subject.projects) || [];
        delete subject.projects;
        let createdSubject = null;
        let createdPersonalDetails;

        return knex.transaction(function (trx) {
            console.log("KnexStrategy.createSubject - creating new subject instance...");

            // create the new PersonalDetails instance (if personalDetails are present)
            return BluebirdPromise.try(function () {
                if (!subject.personalInfo) {

                } else {
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
                .then(function (rows) {
                    createdPersonalDetails = rows && _.mapKeys(rows[0], (value, key) => {
                        return _.camelCase(key);
                    });
                    subject.personalInfo = createdPersonalDetails && createdPersonalDetails.id;
                    return that.getNextSubjectCode(subject, trx);
                })

            // create the new Subject entity
                .then(function (subjCode) {
                    // NOTE: : //CODE STRUCTURE: PREFIX + NUMBER

                    console.log("KnexStrategy.createSubject - subject code: " + subjCode);
                    return knex.returning('*').insert({
                        'code': subjCode,
                        'sex': subject.sex || 'N.D.',
                        'type': subject.type,
                        'tags': JSON.stringify(subject.tags),
                        'notes': subject.notes,
                        'owner': subject.owner,
                        'metadata': subject.metadata,
                        'personal_info': subject.personalInfo,
                        'created_at': new Date(),
                        'updated_at': new Date()
                    }).into('subject').transacting(trx);
                })

            // create all the Subject-Project associations
            // .then(function(rows) {
            //     createdSubject = _.mapKeys(rows[0], (value, key) => { return _.camelCase(key); });
            //     console.log("KnexStrategy.createSubject - creating associations with projects...");
            //     return BluebirdPromise.map(idProjects, function(idProject) {
            //         return knex.insert({'project_subjects': idProject, 'subject_projects': createdSubject.id})
            //         .into('project_subjects__subject_projects').transacting(trx);
            //     });
            // });

                .then(function (res) {
                    createdSubject = res[0];
                    return BluebirdPromise.map(subject.parentSubject || [], function (idParent) {
                        return knex.returning('id').insert({
                            'subject_parentSubject': createdSubject.id,
                            'subject_childrenSubject': idParent
                        }).into('subject_parentsubject__subject_childrensubject').transacting(trx);
                    });
                });
        }) // Knex supports implicit commit/rollback
            .then(function (res) {
                console.log("KnexStrategy.createSubject: transaction committed for new Subject: " + createdSubject.id);
                return _.assign(createdSubject, {
                    'personalInfo': createdPersonalDetails
                    // 'projects': idProjects
                });
            })
            .catch(function (error) {
                console.log("KnexStrategy.createSubject - error caught");
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
    updateSubject (subject) {
        let knex = this.knex;
        let updatedSubject;
        let updatedPersonalDetails;
        let idProjects = _.cloneDeep(subject.projects) || [];
        delete subject.projects;
        let resSubject = {};
        return knex.transaction(function (trx) {
        // Update or create personal information
            return BluebirdPromise.try(function () {
                console.log("KnexStrategy.updateSubject - trying to create/edit PersonalInfo: " + subject.personalInfo);

                // if no personalInfo is provided just skip this step (no creation/update)
                if (!_.isObject(subject.personalInfo)) {

                }

                // you have to create a new personal_details entity (i.e. row)
                else if (!subject.personalInfo.id) {
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
                    return knex('personal_details').returning('*').where('id', '=', subject.personalInfo.id).update({
                        'surname': subject.personalInfo.surname,
                        'given_name': subject.personalInfo.givenName,
                        'birth_date': subject.personalInfo.birthDate
                    }).transacting(trx);
                }
            })

            // update Subject entity
                .then(function (rows) {
                    console.log("KnexStrategy.updateSubject - updating Subject...");
                    console.log(rows);

                    if (rows && rows[0]) {
                        updatedPersonalDetails = _.mapKeys(rows[0], (value, key) => {
                            return _.camelCase(key);
                        });
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
                })

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

                .then(function (res) {
                    updatedSubject = res;
                    return knex('subject_parentsubject__subject_childrensubject').where('subject_parentSubject', subject.id).transacting(trx);
                })
                .then(function (foundSubjectAssociations) {
                    return BluebirdPromise.map(subject.parentSubject || [], function (idParent) {
                        // if the associations exists, leave it alone
                        if (_.findWhere(foundSubjectAssociations, {
                            'subject_childrenSubject': idParent
                        })) {
                            console.log("KnexStrategy.updateData - child-parent subject association found: " + subject.id + "-" + idParent);
                        }
                        // otherwise insert a new association (no support for deleting associations is currently provided for consistency)
                        else {
                            return knex.returning('id').insert({
                                'subject_parentSubject': resSubject.id,
                                'subject_childrenSubject': idParent
                            }).into('subject_parentsubject__subject_childrensubject').transacting(trx);
                        }
                    });
                });
        })
            .then(function (res) {
                console.log('KnexStrategy.updateSubject - transaction commited for updating subject with ID:' + updatedSubject.id);
                return _.assign(updatedSubject, {
                    'personalInfo': updatedPersonalDetails
                    // 'projects': idProjects
                });
            })
            .catch(function (error) {
                console.log("KnexStrategy.createSubject - error caught");
                console.log(error);
                throw new TransactionError(error.message);
            });
    }

    /**
   * @method
   * @name deleteData
   * @param{id}
   */
    deleteSubject (id) {
        let knex = this.knex;
        let personalInfoToDelete = 0;
        let deletedSubject;
        // check if exist personal_info associated
        return knex.transaction(function (trx) {
            return knex.select('personal_info').from('subject').where('id', id).then(function (personalInfoId) {
                if (personalInfoId[0].personal_info) {
                    personalInfoToDelete = personalInfoId[0].personal_info;
                }
                return knex('subject').where('id', id).del().transacting(trx);
            }).then(function (resSubject) {
                deletedSubject = resSubject;
                if (personalInfoToDelete !== 0) {
                    return knex('personal_details').where('id', personalInfoToDelete).del().transacting(trx);
                }
                return BluebirdPromise.resolve(0);
            });
        }).then(function () {
            console.log('KnexStrategy.deleteSubject - transaction commited for deleting subject with ID:' + id);
            return deletedSubject;
        }).catch(function (error) {
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
    putMetadataFieldsIntoEAV (idDataType, fields, useFormattedNames) {
        let knex = this.knex;

        return knex.transaction(function (trx) {
        // for each metadata field
            return BluebirdPromise.map(fields, function (field) {
                // insert the new metadata field
                return knex('eav_attribute').where({
                    'data_type': idDataType,
                    'name': field.name
                }).transacting(trx)

                    .then(function (found) {
                        console.log("KnexStrategy.putMetadataFieldsIntoEAV - found for field " + field.name + ": " + found);
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

            .then(function (insertedIds) {
                console.log('KnexStrategy.putMetadataFieldsIntoEAV - transaction commited for DataType:' + idDataType);
                return _.flatten(insertedIds);
            })

            .catch(function (error) {
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
    putMetadataValuesIntoEAV (data, eavValueTableMap) {
        console.log("KnexStrategy.putMetadataValuesIntoEAV - eavValueTableMap: " + eavValueTableMap);
        let knex = this.knex;
        return knex.transaction(function (trx) {
            return knex('data_type').where({
                id: data.type
            }).first('model').transacting(trx)

                .then(function (row) {
                    // identify the table (e.g. data, subject, sample...)
                    let entityTable = row.model.toLowerCase().toUnderscore();
                    console.log("KnexStrategy.putMetadataValuesIntoEAV - entity table is: " + entityTable);

                    // store each metadata value in the appropriate EAV catalogue
                    return BluebirdPromise.map(Object.keys(data.metadata), function (metadataField) {
                        console.log("KnexStrategy.putMetadataValuesIntoEAV - trying to retrieve the metadataField: " + metadataField);
                        // find the attribute
                        return knex('eav_attribute').where({
                            'data_type': data.type,
                            'name': metadataField
                        }).transacting(trx)

                        //
                            .then(function (eavAttribute) {
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
                                        console.log("KnexStrategy.putMetadataValuesIntoEAV - unit allowed for field: " + metadataField);
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
                                    let eavValues = data.metadata[metadataField].values.map(function (value, index) {
                                        let instance = {
                                            'entity': data.id,
                                            'attribute': eavAttribute.id,
                                            'value': value,
                                            'created_at': new Date(),
                                            'updated_at': new Date()
                                        };
                                        if (unitAllowed) {
                                            console.log("KnexStrategy.putMetadataValuesIntoEAV - unit allowed for field: " + metadataField);
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

                        .then(function (ids) {
                            console.log("KnexStrategy.putMetadataValuesIntoEAV - inserted successfully new metadata value: IDS " + ids);
                            return ids;
                        });
                })

                .then(function (insertedIds) {
                    // console.log('KnexStrategy.putMetadataValuesIntoEAV - inserted values'+ insertedIds);
                    return _.flatten(insertedIds);
                })

                .catch(function (error) {
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
