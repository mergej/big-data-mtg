const db = require('./database');

const StatusEnum = {
    SUCCESS: 'Success',
    SERVERERROR: 'Internal Server Error'
};

async function sqlQuery(sqlStatment) {
    let result = {};

    try {
        const query = await db.DataBasePool.query(sqlStatment);
        result = { status: StatusEnum.SUCCESS, rows: query[0] };
    }
    catch(err) {
        result = { status: StatusEnum.SERVERERROR, rows: err };
    }
    
    return result;
}

module.exports = {
    sqlQuery: sqlQuery,
    StatusEnum,
};