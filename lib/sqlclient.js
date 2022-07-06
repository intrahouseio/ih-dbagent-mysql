/**
 * sqlclient.js
 *
 * Объект вызывается из другого дочернего процесса для чтения из БД
 *   (данные для отчетов)
 *  - Выполняет подключение к БД
 *  - Формирует sql запрос (при необходимости)
 *  - Выполняет запрос, возвращает массив данных
 */

const util = require('util');

const mysql = require('mysql');

const utils = require('./utils');

// Входной параметр
//   dbPath: <полный путь к БД, включая имя файла>
class Sqlclient {
  constructor(opt) {
    this.opt = opt;
    this.pool = null;
  }

  connect() {
    let options = this.opt;
    options.database = options.database+'_hist';
    return new Promise((resolve, reject) => {
      this.pool = mysql.createPool(options);
      this.pool ? resolve() : reject();
    });
  }
  
  prepareQuery(queryObj) {
    let queryStr;
    if (typeof queryObj == 'string') {
      queryStr = queryObj;
    } else if (queryObj.sql) {
      queryStr = queryObj.sql;
    } else {
      if (!queryObj.ids) return ''; // Нет запроса - просто пустая строка

      const idarr = queryObj.ids.split(',');
      queryStr = utils.getQueryStrId(queryObj, idarr);
    }

    console.log('SQLClient queryStr='+queryStr);
    return queryStr;
  }


  query(queryStr) {
   
    if (!queryStr) return Promise.reject('Empty queryStr! ');
    if (typeof queryStr != 'string') return Promise.reject('Expected query as SQL string! ');

    return new Promise((resolve, reject) => {
      this.pool.query(queryStr, (err, records) => {
        if (!err) {
          resolve(records);
        } else reject(err);
      });
    });
  }

  close() {
    if (this.pool) {
      this.pool.end();
      this.pool = null;
    }
  }
}

module.exports = Sqlclient;
