/**
 * logagent.js
 *
 * Точка входа при запуске дочернего процесса для работы с логами
 * Входной параметр - конфигурация как строка JSON: {
 *   dbPath: <полный путь к БД, включая имя файла>
 *   logfile: <полный путь к файлу лога процесса>
 *   loglevel: <уровень логирования>
 * }
 */

const util = require('util');
const path = require('path');
//const { promises: fs } = require('fs');

const logger = require('./logger');
const client = require('./lib/client');
const utils = require('./lib/utils');

const tableNames = ['mainlog', 'pluginlog', 'devicelog'];

let opt;
try {
  opt = JSON.parse(process.argv[2]);
} catch (e) {
  opt = {};
}
opt.database = opt.database+"_log"
const logfile = opt.logfile || path.join(__dirname, 'ih_mysql_logagent.log');
const loglevel = opt.loglevel || 0;
const maxlogrecords = opt.maxlogrecords || 100000;
logger.start(logfile, loglevel);
logger.log('Start logagent mysql. Options: ' + JSON.stringify(opt));

sendProcessInfo();
setInterval(sendProcessInfo, 10000); // 10 сек

sendSettingsRequest();
setInterval(sendSettingsRequest, 10800000); // 3 часа = 10800 сек

main(process);

async function main(channel) {
  const initErr = client.init();
  if (initErr) processExit(0, initErr); // Модуль mysql не установлен

  try {
    //if (!opt.dbPath) throw { message: 'Missing dbPath for logs!' };

    await client.createPoolToDatabase(opt, logger);
    if (!client.pool) throw { message: 'Client creation Failed!' };

    for (const name of tableNames) {
      await client.query(getCreateTableStr(name));
      await client.query('CREATE INDEX IF NOT EXISTS ' + name + '_ts ON ' + name + ' (tsid);');
    }

    sendDBSize(); // Отправить статистику первый раз
    setInterval(async () => sendDBSize(), 300000); // 300 сек = 5 мин

    channel.on('message', ({ id, type, query, payload }) => {
      if (type == 'write') return write(id, query, payload);
      if (type == 'read') return read(id, query);
      if (type == 'run') return run(id, query);
      if (type == 'settings') return del(payload);
    });

    process.on('SIGTERM', () => {
      logger.log('Received SIGTERM');
      processExit(0);
    });

    process.on('exit', () => {
      if (client && client.pool) client.pool.end();
    });
  } catch (err) {
    processExit(1, err);
  }

  async function showGroups(name) {
    const result = await client.query(getGroupQuery(name));
    logger.log(name + ' group: ' + util.inspect(result));
  }

  /**
   *
   * @param {String} id - request uuid
   * @param {Objects} queryObj - {table}
   * @param {Array of Objects} payload - [{ dn, prop, ts, val }]
   */
  async function write(id, queryObj, payload) {
    const table = queryObj && queryObj.table ? queryObj.table : 'mainlog';
    const columns = getColumns(table);
    const values = utils.formValues(payload, columns);
    if (!values || !values.length) return;

    const values1 = values.map(i => `(${i})`).join(', ');
    const sql = 'INSERT INTO ' + table + ' (' + columns.join(',') + ') VALUES ' + values1;

    try {
      const changes = await client.query(sql);
      logger.log('Write query id=' + id + ', changes=' + changes, 2);
    } catch (err) {
      sendError(id, err);
    }
  }

  async function read(id, queryObj) {
    try {
      const sql = queryObj.sql ? queryObj.sql : '';
      if (!sql) throw { message: 'Missing query.sql in read query: ' + util.inspect(queryObj) };

      const result = await client.query(sql);
      logger.log(sql + ' Result length = ' + result.length, 1);

      send({ id, query: queryObj, payload: result });
    } catch (err) {
      sendError(id, err);
    }
  }

  async function run(id, queryObj) {
    try {
      const sql = queryObj.sql ? queryObj.sql : '';
      if (!sql) throw { message: 'Missing query.sql in run query: ' + util.inspect(queryObj) };

      const changes = await client.query(sql);
      logger.log(`${sql}  Row(s) affected: ${changes}`, 2);
    } catch (err) {
      sendError(id, err);
    }
  }

  // {devicelog:[{level, days},..], pluginlog:{level,days}}
  async function del(payload) {
    if (!payload || !payload.rp || typeof payload.rp != 'object') return;
    for (const name of Object.keys(payload.rp)) {
      // Если есть такая таблица - обработать
      if (tableNames.includes(name)) {
        const arr = payload.rp[name];
        for (const item of arr) {
          await deleteRecordsByLevel(name, item.days, item.level);
        }
      }
    }
  }

  async function deleteRecordsByLevel(tableName, archDay, level) {
    const archDepth = archDay * 86400000;
    const delTime = Date.now() - archDepth;

    const sql = `DELETE FROM ${tableName} WHERE level = ${level} AND ts<${delTime}`;

    try {
      const changes = await client.query(sql);
      logger.log(`${tableName}  Level=${level} Archday=${archDay}  Row(s) deleted ${changes}`, 1);
    } catch (err) {
      sendError('delete', err);
    }
  }

  async function deleteRecordsMax(tableName) {
    // Оставляем только данные за 1 день
    const archDepth = 1 * 86400000;
    const delTime = Date.now() - archDepth;

    const sql = `DELETE FROM ${tableName} WHERE ts<${delTime}`;
    const mes = 'Number of records exceeded ' + maxlogrecords + '!! All except the last day data was deleted!!';

    try {
      const changes = await client.run(sql);
      logger.log(`${tableName}  ${mes} Row(s) deleted ${changes}`, 1);
    } catch (err) {
      sendError('delete', err);
    }
  }

  function send(message) {
    channel.send(message);
  }

  function sendError(id, err) {
    logger.log(err);
    send({ id, error: utils.getShortErrStr(err) });
  }

  function processExit(code, err) {
    let msg = '';
    if (err) msg = 'ERROR: ' + utils.getShortErrStr(err) + ' ';

    if (client && client.pool) {
      client.pool.end();
      client.pool = null;
      msg += 'Close connection pool.';
    }

    logger.log(msg + ' Exit with code: ' + code);
    setTimeout(() => {
      channel.exit(code);
    }, 500);
  }

  async function sendDBSize() {
    if (!process.connected) return;
    const data = {
      size: 0
    };
    const sqlQuery = "SELECT table_name AS 'Table', "+ 
    "ROUND(((data_length + index_length) / 1024 / 1024), 2) AS 'Size (MB)' "+
    "FROM information_schema.TABLES "+
    "WHERE table_schema = '"+opt.database+"' ORDER BY (data_length + index_length) DESC;"
    //logger.log("Sql" + sqlQuery);
    const dbSizeArr = await client.query(sqlQuery);
    
    dbSizeArr.forEach(element => {
      data.size += element["Size (MB)"];
    });
    logger.log("DbSize " + data.size, 2);

    const needDelete = [];
    for (const name of tableNames) {
      // const result = await client.query('SELECT Count (*) count From ' + name);
      // const count = result ? result[0].count : 0;
      const count = await getTableRecordsCount(name);
      if (count > 1000) {
        await showGroups(name);
      }
      data[name] = count;
      if (maxlogrecords > 0 && count > maxlogrecords && name != 'mainlog') needDelete.push(name);
    }

    // Отправить фактическое состояние
    if (process.connected) process.send({ type: 'procinfo', data });

    if (!needDelete.length) return;

    for (const name of needDelete) {
      await deleteRecordsMax(name);
    }
  }

  async function getTableRecordsCount(name) {
    const result = await client.query('SELECT Count (*) count From ' + name);
    return result ? result[0].count : 0;
  }
}

// Частные функции
// Строка для создания таблиц в БД
function getCreateTableStr(tableName) {
  let result;
  switch (tableName) {
    case 'devicelog':
      result = 'did TEXT,prop TEXT,val TEXT,txt TEXT, ts BIGINT NOT NULL,tsid TEXT,cmd TEXT,sender TEXT';
      break;
    case 'pluginlog':
      result = 'unit TEXT, txt TEXT,level INTEGER, ts BIGINT NOT NULL, tsid TEXT, sender TEXT';
      break;
    default:
      result = 'tags TEXT, did TEXT, location TEXT, txt TEXT, level INTEGER, ts BIGINT NOT NULL,tsid TEXT,sender TEXT';
  }
  return 'CREATE TABLE IF NOT EXISTS ' + tableName + ' (' + result + ')';
}

function getColumns(tableName) {
  switch (tableName) {
    case 'devicelog':
      return ['did', 'prop', 'val', 'txt', 'ts', 'tsid', 'cmd', 'sender'];

    case 'pluginlog':
      return ['unit', 'txt', 'level', 'ts', 'tsid', 'sender'];

    default:
      return ['tags', 'did', 'location', 'txt', 'level', 'ts', 'tsid', 'sender'];
  }
}

function getGroupQuery(tableName) {
  switch (tableName) {
    case 'devicelog':
      return 'SELECT did, Count (tsid) count from devicelog group by did';

    case 'pluginlog':
      return 'SELECT unit, Count (tsid) count from pluginlog group by unit';

    default:
      return 'SELECT level, Count (tsid) count from ' + tableName + ' group by level';
  }
}

function sendProcessInfo() {
  const mu = process.memoryUsage();
  const memrss = Math.floor(mu.rss / 1024);
  const memheap = Math.floor(mu.heapTotal / 1024);
  const memhuse = Math.floor(mu.heapUsed / 1024);
  if (process.connected) process.send({ type: 'procinfo', data: { state: 1, memrss, memheap, memhuse } });
}

function sendSettingsRequest() {
  if (process.connected) process.send({ id: 'settings', type: 'settings' });
}
