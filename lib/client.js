/**
 * mysql client
 */

let mysql;


module.exports = {
  pool:null,
  init() {
    try {
      mysql = require('mysql');
    } catch (err) {
      return err; // Не установлен npm модуль - больше не перезагружать
    }
  },

  async createPoolToDatabase(dbopt) {
    const database = dbopt.database;
    dbopt.database = null;
   
    await this.connectAndQuery('CREATE DATABASE IF NOT EXISTS ' + database, dbopt);
   
    dbopt.database = database;
    dbopt.connectionLimit = 10;
    this.pool = mysql.createPool(dbopt);
  },

  connectAndQuery(query, dbopt) {
    const connection = mysql.createConnection(dbopt);
    return new Promise((resolve, reject) => {
      connection.query(query, err => {
        connection.end();
        if (!err) {
          resolve();
        } else reject(err);
      });
    });
  },


  writePoints(tableName, columns, values) {
    const query = 'INSERT INTO ' + tableName + ' (' + columns.join(',') + ') VALUES ?' ;

    return new Promise((resolve, reject) => {
      this.pool.query(query, [values], err => {
      //  this.pool.query(query, err => {
        if (!err) {
          resolve();
        } else reject(err);
      });
    });
  },


  query(query) {
    return new Promise((resolve, reject) => {
      this.pool.query(query, (err, records) => {
        if (!err) {
          resolve(records);
        } else reject(err);
      });
    });
  }
};

// Частные функции
