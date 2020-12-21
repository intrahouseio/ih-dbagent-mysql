/**
 * dbagent - client for MySQL
 */
const util = require('util');

const client = require('./client');
const utils = require('./utils');

module.exports = async function(channel, opt, logger) {
  const initErr = client.init();
  if (initErr) processExit(0, initErr);

  const options = getOptions(opt);
  try {
    const databaseName = options.database;
    logger.log('Options: ' + JSON.stringify(options), 2);

    // await client.createDatabase(options);
    logger.log('Database: ' + databaseName, 1);

    client.connect(options);

    // await client.createTable('records');

    channel.on('message', ({ id, type, query, payload }) => {
      if (type == 'write') return write(id, payload);
      if (type == 'read') return read(id, query);
      if (type == 'settings') return settings(id, query);
    });
  } catch (err) {
    processExit(1, err);
  }

  async function write(id, payload) {
    /*
    let values = formValues(payload, columns, tdate);
    if (!values || !values.length) return;
 */
    const tableName = 'records';
    const columns = ['dn','ts','val']
    try {
      await client.writePoints(tableName, columns, ['DT109', Date.now(), 22]);
    } catch (err) {
      sendError(id, err);
    }
    
   logger.log('Write query id=' + id + util.inspect(payload), 2);
  }

  async function read(id, queryObj) {
    logger.log('Get read query id=' + id + util.inspect(queryObj), 2);
    const dnarr = queryObj.dn_prop.split(',');
    const queryStr = utils.getQueryStr(queryObj, dnarr);
    logger.log('Send: ' + queryStr, 2);

    try {
      const result = await client.query(queryStr);

      logger.log('Get result ' + id , 2);
      logger.log('result ' + util.inspect(result), 2);
      // const payload = queryObj.target == 'trend' ? formForTrend(result) : result;
      send({ id, query: queryObj, payload: [] });
    } catch (err) {
      sendError(id, err);
    }

    function formForTrend(res) {
      return dnarr.length == 1 ? res.map(item => [item.time, item.val]) : utils.recordsForTrend(res, dnarr);
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
      connectionLimit: 10,
      host: 'localhost',
      port: 3306,
      user: 'root',
      password: 'intrahousemysql',
      database: 'ihdb'
    };
    return Object.assign(res, argOpt);
  }

  function processExit(code, err) {
    if (err) logger.log(err);

    setTimeout(() => {
      channel.exit(code);
    }, 500);
  }
};
