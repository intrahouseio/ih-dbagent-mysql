/**
 * mysql client
 */

let mysql;
let poolDb;

module.exports = {
  init() {
    try {
      mysql = require('mysql');
    } catch (err) {
      return err; // Не установлен npm модуль - больше не перезагружать
    }
  },

  connect(dbopt) {
    poolDb = mysql.createPool(dbopt);
  },

  createDatabase(dbopt) {
    const database = dbopt.database;
    dbopt.database = null;
    const connection = mysql.createConnection(dbopt);

    return new Promise((resolve, reject) => {
      connection.query('CREATE DATABASE IF NOT EXISTS ' + database, err => {
        // connection.end(); ??
        if (!err) {
          dbopt.database = database;
          resolve();
        } else reject(err);
      });
    });
  },

  createTable(tableName, fname) {
    if (!fname) fname = tableName;
    const query = getCreateTableStr(tableName, fname);
    return new Promise((resolve, reject) => {
      poolDb.query(query, err => {
        if (!err) {
          resolve();
        } else reject(err);
      });
    });
  },

  writePoints(tableName, columns, values) {
    const query = 'INSERT INTO ' + tableName + ' (' + columns.join(',') + ') VALUES ? ';
    return new Promise((resolve, reject) => {
      poolDb.query(query, [values], err => {
        if (!err) {
          resolve();
        } else reject(err);
      });
    });
  },

  query(query) {
    poolDb.query('SHOW TABLES', (err, records) => 
      // 
       new Promise((resolve, reject) => {
        if (!err) {
          resolve(records);
        } else reject(err);
      }));
  }
};

// Частные функции
function getCreateTableStr(tableName, fname) {
  let result;
  switch (tableName) {
    case 'timeline':
      result =
        'id int auto_increment NOT NULL,' +
        'dn char(64) NOT NULL,' +
        'start BIGINT NOT NULL,' +
        'end BIGINT NOT NULL,' +
        'state char(8),' +
        'PRIMARY KEY(id)';
      break;

    case 'userlog':
      result =
        'id int NOT NULL,' +
        'ts BIGINT NOT NULL,' +
        'endts BIGINT NOT NULL,' +
        'ackts BIGINT NOT NULL,' +
        'txt char(80),' +
        'level int,' +
        'PRIMARY KEY(id)';
      break;
    default:
      result =
        'id int auto_increment NOT NULL,' +
        'ts BIGINT NOT NULL,' +
        'dn char(64) NOT NULL,' +
        'val char(16),' +
        'KEY dn_ts(dn,ts),' +
        'PRIMARY KEY(id)';
  }
  return 'CREATE TABLE IF NOT EXISTS ' + fname + ' (' + result + ') ENGINE=MYISAM';
}
