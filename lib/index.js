/**
 * dbagent - client for MySQL
 */
const util = require('util');

const client = require('./client');
const utils = require('./utils');

module.exports = async function(channel, opt, logger) {
  const initErr = client.init();
  if (initErr) processExit(0, initErr); // Модуль mysql не установлен

  const options = getOptions(opt);
  logger.log('Options: ' + JSON.stringify(options), 2);

  try {
    await client.createPoolToDatabase(options);
    if (!client.pool) throw { message: 'Pool creation Failed!' };

    // client.pool.on('connection', connection => {
    client.pool.on('connection', () => {
      logger.log('Add new connection to pool.', 2);
    });

    await createTable('records');

    channel.on('message', ({ id, type, query, payload }) => {
      if (type == 'write') return write(id, payload);
      if (type == 'read') return read(id, query);
      if (type == 'settings') return settings(id, query);
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
   * @param {Array of Objects} payload - [{ dn, prop, ts, val }]
   */
  async function write(id, payload) {
    const columns = ['dn', 'prop', 'ts', 'val'];
    const values = utils.formValues(payload, columns);
    if (!values || !values.length) return;

    const tableName = 'records';
    try {
      await client.writePoints(tableName, columns, values);
    } catch (err) {
      sendError(id, err);
    }
    logger.log('Write query id=' + id + util.inspect(payload), 2);
  }

  async function read(id, queryObj) {
    logger.log('Read query id=' + id + util.inspect(queryObj), 2);
    const dnarr = queryObj.dn_prop.split(',');
    const queryStr = utils.getQueryStr(queryObj, dnarr);
    logger.log('SQL: ' + queryStr, 2);

    try {
      const result = await client.query(queryStr);
      logger.log('Get result ' + id, 2);
      logger.log('result ' + util.inspect(result), 2);
      const payload = queryObj.target == 'trend' ? formForTrend(result) : result;
      send({ id, query: queryObj, payload });
    } catch (err) {
      sendError(id, err);
    }

    function formForTrend(res) {
      return dnarr.length == 1 ? res.map(item => [item.ts, Number(item.val)]) : utils.recordsForTrend(res, dnarr);
    }
  }

  function settings(id, query) {
    if (query.loglevel) logger.setLoglevel(query.loglevel);
  }

  function send(message) {
    channel.send(message);
  }

  function sendError(id, err) {
    logger.log(err);
    send({ id, error: utils.getShortErrStr(err) });
  }

  function getOptions(argOpt) {
    const res = {
      host: 'localhost',
      port: 3306,
      user: 'root',
      password: 'intrahousemysql',
      database: 'ihdb'
    };
    return Object.assign(res, argOpt);
  }

  function processExit(code, err) {
    let msg = '';
    if (err) msg = 'ERROR: ' + utils.getShortErrStr(err) + ' ';

    if (client && client.pool) {
      client.pool.end();
      msg += 'Close connection pool.';
    }

    logger.log(msg + ' Exit with code: ' + code);
    setTimeout(() => {
      channel.exit(code);
    }, 500);
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

    default:
      result =
        'id int auto_increment NOT NULL,' +
        'ts BIGINT NOT NULL,' +
        'dn char(64) NOT NULL,' +
        'prop char(32),' +
        'val char(16),' +
        'PRIMARY KEY(id)';
  }
  return 'CREATE TABLE IF NOT EXISTS ' + fname + ' (' + result + ') ENGINE=MYISAM';
}
