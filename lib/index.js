/**
 * dbagent - client for Mysql
 */
const util = require('util');
const schedule = require('node-schedule');
const client = require('./client');
const utils = require('./utils');

module.exports = async function(channel, opt, logger) {
  const initErr = client.init();
  if (initErr) processExit(0, initErr); 

  let options = getOptions(opt);
  options.database = options.database+"_hist"
  let overflow = 0;
  let lastOverflow = 0;
  let maxTimeRead = 0;
  let maxTimeWrite = 0;

  let hoursRule = new schedule.RecurrenceRule();
  // hoursRule.rule = '*/30 * * * * *';
  hoursRule.rule = '0 0 * * * *';

  let j = schedule.scheduleJob(hoursRule, () => {
    send({ id: 'settings', type: 'settings' }); //Get settings for retention policy
  });

  logger.log('Options: ' + JSON.stringify(options), 2);
  setInterval(async () => getDBSize(), 60000); //Get db size

  async function getDBSize() {
    const sqlQuery = "SELECT table_name AS 'Table', "+ 
    "ROUND(((data_length + index_length) / 1024 / 1024), 2) AS 'Size (MB)' "+
    "FROM information_schema.TABLES "+
    "WHERE table_schema = '"+options.database+"' ORDER BY (data_length + index_length) DESC;"
    //logger.log("Sql" + sqlQuery);
    const dbSizeArr = await client.query(sqlQuery);
    
    let fileSize = 0;
    dbSizeArr.forEach(element => {
      fileSize += element["Size (MB)"];
    });
    logger.log("DbSize " + fileSize, 2);
    if (process.connected) process.send({ type: 'procinfo', data: { size: fileSize } });
    overflow = fileSize > opt.dbLimit ? 1 : 0;

    if (process.connected) process.send({ type: 'procinfo', data: { overflow: overflow } });
  }

  try {
    await client.createPoolToDatabase(options, logger);
    if (!client.pool) throw { message: 'Client creation Failed!' };

    client.pool.on('connection', () => {      
      logger.log('Add new connection to pool.', 2);
    });

    getDBSize();
    await createTable('records');
    await client.run('CREATE INDEX IF NOT EXISTS idx_records_dnts ON records (id, ts);');
    
    await createTable('timeline');
    await client.run('CREATE INDEX IF NOT EXISTS idx_records_start ON timeline (start);');
    
    await createTable('customtable');
    await client.run('CREATE INDEX IF NOT EXISTS idx_name ON customtable (name);');

    channel.on('message', ({ id, type, query, payload, table }) => {
      if (type == 'write') {
        if (overflow == 0) return write(id, payload, table);
        if (overflow == 1 && lastOverflow == 0) {
          lastOverflow = overflow;
          return sendError(id, 'The allocated space for the database has run out, increase the limit');
        }
      }
      if (type == 'read') return read(id, query);
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

    /**
   *
   * @param {String} tableName
   * @param {String} fname - optional
   */
     async function createTable(tableName, fname) {
      if (!fname) fname = tableName;
      return client.query(getCreateTableStr(tableName, fname));
    }

  /**
   *
   * @param {String} id - request uuid
   * @param {Array of Objects} payload - [{ id, ts, val }]
   */
  async function write(id, payload, table) {
    const beginTime = Date.now();

    const tableName = table || 'records';
    const columns = getColumns(tableName);

    const values = utils.formValues(payload, columns);
    if (!values || !values.length) return;

    const values1 = values.map(i => `(${i})`).join(', ');
    const query = 'INSERT INTO ' + tableName + ' (' + columns.join(',') + ') VALUES '+values1;

    logger.log('Row(s) writed ' + query, 2);
    try {
      await client.query(query);
      const endTime = Date.now();
      if (maxTimeWrite < endTime - beginTime) {
        maxTimeWrite = endTime - beginTime;
        if (process.connected)
          process.send({
            type: 'procinfo',
            data: { lastMaxTimeWrite: maxTimeWrite, lastMaxCountWrite: payload.length }
          });
      }
      logger.log('Write query id=' + id + util.inspect(payload), 2);
    } catch (err) {
      sendError(id, err);
    }
  }

  

  async function del(options) {
    let archDays = [1, 7, 15, 30, 90, 180, 360, 500];

    let tableName = 'records';
    for (const archDay of archDays) {
      let arrDnProp = options.rp.filter(object => object.days == archDay);
      await deletePoints(tableName, archDay, arrDnProp);
    }

  }

  async function deletePoints(tableName, archDay, arrDnProp) {
    logger.log('Archday=' + archDay + ' ArrayofProps=' + JSON.stringify(arrDnProp), 1);
    let archDepth = archDay * 86400000;
    //archDepth = 600000;
    let delTime = Date.now() - archDepth;
    if (!arrDnProp.length) return;
    while (arrDnProp.length>0) {
      let chunk = arrDnProp.splice(0,500);
      let values = chunk.map(i => `(id=${i.id})`).join(' OR ');
      logger.log('Map=' + values, 1);
      let sql = `DELETE FROM ${tableName} WHERE (${values}) AND ts<${delTime}`;
      try {
        const changes = await client.query(sql);
        logger.log("Row(s) deleted" + util.inspect(changes), 1);
      } catch (err) {
        sendError('delete', err);
      }
    }
  }

  async function read(id, queryObj) {
    const beginTime  = Date.now();
    let idarr, dnarr;
    try {
      logger.log('queryObj: ' + util.inspect(queryObj), 2);
      let queryStr;
      if (queryObj.sql) {
        queryStr = queryObj.sql;
      } else {
        if (!queryObj.dn_prop) throw { message: 'Expected dn_prop in query ' };
        if (queryObj.table == 'timeline') {
          dnarr = queryObj.dn_prop.split(',');
          queryStr = utils.getQueryStrDn(queryObj, dnarr);
        } else {
          logger.log('idarr: ' + util.inspect(queryObj.ids), 2);
          idarr = queryObj.ids.split(',');
          logger.log('idarr: ' + util.inspect(idarr), 2);
          queryStr = utils.getQueryStrId(queryObj, idarr);
        }

      }
      logger.log('SQL: ' + queryStr, 2);
      firstTime = Date.now();

      const result = await client.query(queryStr);
      //logger.log('Result: ' + util.inspect(result), 1);
       const endTime = Date.now();
      if (maxTimeRead < endTime - beginTime) {
        maxTimeRead = endTime - beginTime;
        if (process.connected) process.send({type:'procinfo', data:{lastMaxTimeRead: maxTimeRead, lastMaxCountRead:result.length}});
      }
      logger.log('Get result ' + id, 2);
      let payload = [];
      if (queryObj.sql || queryObj.table == 'timeline') {
        payload = result;
      } else {  
        payload = queryObj.target == 'trend' ? formForTrend(result) : utils.recordsFor(result, queryObj);
      }
      logger.log('payload ' + util.inspect(payload), 2);
      send({ id, query: queryObj, payload });
    } catch (err) {
      sendError(id, err);
    }

    function formForTrend(res) {
      return idarr.length == 1 ? res.map(item => [item.ts, Number(item.val)]) : utils.recordsForTrend(res, idarr);
    }
  }

  function settings(id, query, payload) {
    logger.log('Recieve settings' + JSON.stringify(payload), 1);
    // if (query.loglevel) logger.setLoglevel(query.loglevel);
  }

  function send(message) {
    if (channel.connected) channel.send(message);
  }

  function sendError(id, err) {
    logger.log(err);
    send({ id, error: utils.getShortErrStr(err) });
  }

  function getOptions(argOpt) {
    //
    const res = {};

    return Object.assign(res, argOpt);
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
};

// Частные функции
// Строка для создания таблиц в БД

function getCreateTableStr(tableName, fname) {
  let result;
  switch (tableName) {
    case 'timeline':
      result =
      'id INTEGER PRIMARY KEY AUTO_INCREMENT,' +
      'dn TEXT NOT NULL, prop TEXT,' +
      'start BIGINT  NOT NULL,' +
      'end BIGINT NOT NULL,' +
      'state char(8)';
      break;
    case 'customtable':
      result = 
      'id INTEGER PRIMARY KEY AUTO_INCREMENT, ' +
      'name TEXT NOT NULL, ' +
      'ts BIGINT NOT NULL, ' +
      'payload TEXT'
    break;
    default:
      result = 'uid INTEGER PRIMARY KEY AUTO_INCREMENT, ts BIGINT NOT NULL, id integer, val real';
  }
  return 'CREATE TABLE IF NOT EXISTS ' + fname + ' (' + result + ') ENGINE=Aria';
}


function getColumns(tableName) {
  switch (tableName) {
    case 'timeline':
      return ['dn', 'prop', 'start', 'end', 'state'];
    default:
      return ['ts', 'id', 'val'];
  }
}