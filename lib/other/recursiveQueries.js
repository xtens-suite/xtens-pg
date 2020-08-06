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

  let statement = ['WITH RECURSIVE nodes (parentId, parentName, parentTemplate, childId, childName, childTemplate, path, cycle, depth)',
    ' AS (SELECT r.datatype_children, p1.name, p1.model, r.datatype_parents, p2.name, p2.model, ARRAY[r.datatype_parents,r.datatype_children], (r.datatype_parents = r.datatype_children), 1',
    ' FROM datatype_children__datatype_parents AS r, data_type AS p1, data_type AS p2',
    ' WHERE r.datatype_children = $1 AND p1.id = r.datatype_children AND p2.id = r.datatype_parents',
    ' UNION ALL',
    ' SELECT r.datatype_children, p1.name, p1.model, r.datatype_parents, p2.name, p2.model, nd.path || r.datatype_parents, r.datatype_parents = ANY(nd.path) , nd.depth + 1',
    ' FROM datatype_children__datatype_parents AS r, data_type AS p1, data_type AS p2, nodes AS nd',
    ' WHERE r.datatype_children = nd.childId AND p1.id = r.datatype_children AND p2.id = r.datatype_parents AND NOT cycle)',
    ' SELECT * FROM nodes;'
  ].join('');

  global.sails.models.datatype.query(statement, [root], next);

}

/**
 * @method
 * @name fetchSubjectDataTree
 * @description fetch a nested (recursive) data structure for the given subject
 * @param{integer} subjectId
 */
function fetchSubjectDataTree(subjectId, next) {

  let statement = ['SELECT concat(\'sb_\',sb.id) AS id, dt.name as type, dt.id AS typeId, sb.metadata,',
    'CASE WHEN sbsb."subject_childrenSubject" > 0 THEN (\'sb_\', sbsb."subject_childrenSubject") ELSE NULL END AS parent_subject,',
    'NULL AS parent_sample,',
    'NULL AS parent_data,',
    'NULL AS biobankCode',
    'FROM subject sb',
    'INNER JOIN data_type dt ON sb.type = dt.id',
    'INNER JOIN subject_parentsubject__subject_childrensubject sbsb ON sbsb."subject_parentSubject" = sb.id',
    'WHERE sbsb."subject_childrenSubject" = $1',
    'UNION ALL',
    'SELECT concat(\'s_\',s.id) AS id , dt.name as type , dt.id AS typeId, s.metadata,',
    'NULL AS parent_subject,',
    'CASE WHEN ss."sample_childrenSample" > 0 THEN concat(\'s_\',ss."sample_childrenSample") ELSE NULL END AS parent_sample,',
    'NULL AS parent_data,',
    's.biobank_code AS biobankCode',
    'FROM sample s',
    'INNER JOIN data_type dt ON s.type = dt.id',
    'INNER JOIN sample_donor__subject_childrensample ssb ON ssb.sample_donor = s.id',
    'LEFT JOIN sample_parentsample__sample_childrensample ss ON ss."sample_parentSample" = s.id',
    'WHERE ssb."subject_childrenSample" = $1',
    'UNION ALL',
    'SELECT concat(\'d_\',d.id) AS id, dt.name AS type, dt.id AS typeId, d.metadata,',
    'NULL AS parent_subject,',
    'CASE WHEN sd."sample_childrenData" > 0 THEN concat(\'s_\',sd."sample_childrenData") ELSE NULL END AS parent_sample,',
    'CASE WHEN dd."data_childrenData" > 0 THEN concat(\'d_\',dd."data_childrenData") ELSE NULL END AS parent_data,',
    'NULL AS biobankCode',
    'FROM data d',
    'INNER JOIN data_type dt ON d.type = dt.id',
    'INNER JOIN super_type st ON dt.super_type = st.id',
    'INNER JOIN data_parentsubject__subject_childrendata dsb ON dsb."data_parentSubject" = d.id',
    'LEFT JOIN data_parentsample__sample_childrendata sd ON sd."data_parentSample" = d.id',
    'LEFT JOIN data_childrendata__data_parentdata dd ON dd."data_parentData" = d.id',
    'WHERE st.skip_paging <> true AND dsb."subject_childrenData" = $1;'
  ].join(' ');

  global.sails.models.subject.query(statement, [subjectId], next);

}

/**
 * @method
 * @name fetchSubjectDataTreeSimple
 * @description fetch a single level data structure with all the children for the given subject
 * @param{integer} subjectId
 */
function fetchSubjectDataTreeSimple(subjectId, next) {

  let statement = ['SELECT data_type.id',
    'FROM data_type',
    'INNER JOIN sample ON data_type.id = sample.type',
    'INNER JOIN sample_donor__subject_childrensample ssb ON ssb.sample_donor = sample.id',
    'WHERE ssb."subject_childrenSample" = $1',
    'UNION',
    'SELECT data_type.id',
    'FROM data_type',
    'INNER JOIN data ON data_type.id = data.type',
    'INNER JOIN data_parentsubject__subject_childrendata dsb ON dsb."data_parentSubject" = data.id',
    'WHERE dsb."subject_childrenData" = $1;'
  ].join(' ');

  global.sails.models.subject.query(statement, [subjectId], next);

}

module.exports.fetchDataTypeTree = fetchDataTypeTree;
module.exports.fetchSubjectDataTree = fetchSubjectDataTree;
module.exports.fetchSubjectDataTreeSimple = fetchSubjectDataTreeSimple;