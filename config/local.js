var connection = {
    adapter: 'sails-postgresql',
    host: 'localhost',
    port: 9432,
    user: 'xtenspg',
    password: 'xtenspg',
    database: 'xtensknextest',
    pool: true,
    ssl: false,
    schema: true
};

var fsConnection = {
    type: 'irods-rest',
    restURL: {
        hostname: '130.251.10.60',
        port: 8080,
        path: '/irods-rest-4.0.2.1-SNAPSHOT/rest'
    },
    irodsHome: '/biolabZone/home/superbiorods',
    repoCollection: 'xtens-repo',
    landingCollection: 'landing',
    username: 'superbiorods',
    password: 'superbio05!'
};


module.exports.connection = connection;
module.exports.fsConnection = fsConnection;
