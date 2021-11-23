const mysql = require('mysql2/promise');

DB_HOST="uldz7bsq6x3frltd.myfritz.net"
DB_USER="root"
DB_PASSWORD="tinf19d2021"
DB_DATABASE="big_data"
DB_PORT=3308

exports.DataBasePool = mysql.createPool({
    host: DB_HOST,
    user: DB_USER,
    password: DB_PASSWORD,
    database: DB_DATABASE,
    port: DB_PORT,
    connectionLimit: 5
});