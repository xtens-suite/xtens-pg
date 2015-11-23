/**
 * @module
 * @name recursiveQueries
 * @author Massimiliano Izzo
 */
/* jshint esnext: true */
/* jshint node: true */
"use strict";


/**
 * @method
 * @name fetchDataTypeTree
 * @param{integer} root - ID (?) of the root Data Type
 */
function fetchDataTypeTree(root, next) {
    
    let statement = ['WITH RECURSIVE nodes (parentId, parentName, parentTemplate, childId, childName, childTemplate, path, depth)',
                    ' AS (SELECT r.datatype_children, p1.name, p1.model, r.datatype_parents, p2.name, p2.model, ARRAY[r.datatype_parents], 1',
                    ' FROM datatype_children__datatype_parents AS r, data_type AS p1, data_type AS p2 WHERE r.datatype_children = $1',
                    ' AND p1.id = r.datatype_children AND p2.id = r.datatype_parents UNION ALL',
                    ' SELECT r.datatype_children, p1.name, p1.model, r.datatype_parents, p2.name, p2.model,path || r.datatype_children, nd.depth + 1',
                    ' FROM datatype_children__datatype_parents AS r, data_type AS p1, data_type AS p2, nodes AS nd',
                    ' WHERE r.datatype_children = nd.childId AND p1.id = r.datatype_children AND p2.id = r.datatype_parents)',
                    ' SELECT * FROM nodes;'].join('');

    global.sails.models.dataType.query(statement, [root], next);

}

/**
 * @method
 * @name fetchSubjectDataTree
 * @description fetch a nested (recursive) data structure for the given subject
 * @param{integer} subjectId
 */
function fetchSubjectDataTree(subjectId, next) {
    
    let statement = ['SELECT concat(\'s_\',s.id) AS id , dt.name as type , s.metadata,',
        ' CASE WHEN s.parent_sample > 0 THEN concat(\'s_\',s.parent_sample) ELSE NULL END AS parent_sample, NULL AS parent_data FROM sample s',
        ' INNER JOIN data_type dt ON s.type = dt.id WHERE s.parent_subject = $1 UNION ALL',
        ' SELECT concat(\'d_\',d.id) AS id, dt.name AS type, d.metadata,',
        ' CASE WHEN d.parent_sample > 0 THEN concat(\'s_\',d.parent_sample) ELSE NULL END AS parent_sample,',
        ' CASE WHEN d.parent_data > 0 THEN concat(\'d_\',d.parent_data) ELSE NULL END AS parent_data',
        ' FROM data d INNER JOIN data_type dt ON d.type = dt.id WHERE d.parent_subject = $1  LIMIT 100;'].join('');

    global.sails.models.subject.query(statement, [subjectId], next);

}

/**
 * @method
 * @name fetchSubjectDataTreeSimple
 * @description fetch a single level data structure with all the children for the given subject
 * @param{integer} subjectId
 */
function fetchSubjectDataTreeSimple(subjectId, next) {
    
    let statement = ['SELECT data_type.id FROM data_type',
        ' INNER JOIN sample ON data_type.id = sample.type WHERE parent_subject = $1 UNION',
        ' SELECT data_type.id FROM data_type INNER JOIN data ON data_type.id = data.type WHERE parent_subject = $1;'].join('');

    global.sails.models.subject.query(statement, [subjectId], next);

}

module.exports.fetchDataTypeTree = fetchDataTypeTree;
module.exports.fetchSubjectDataTree = fetchSubjectDataTree;
module.exports.fetchSubjectDataTreeSimple = fetchSubjectDataTreeSimple;
