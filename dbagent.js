/**
 * dbagent.js
 * Точка входа при запуске дочернего процесса
 * Входной параметр - путь к файлу конфигурации или сама конфигурация как строка JSON
 * В данном случае ожидается JSON
 * 
 */

// const util = require('util');
const path = require('path');

const dbagent = require('./lib/index');
const logger = require('./logger');


// Извлечь имя log или писать в /var/log
let opt;
try {
  opt = JSON.parse(process.argv[2]);
} catch (e) {
  opt = {};
}

const logfile = opt.logfile || path.join(__dirname,'ih_mysql.log');
const loglevel = opt.loglevel || 0;

logger.start(logfile,loglevel);
logger.log('Start dbagent mysql. Loglevel: '+loglevel);

delete opt.logfile;
delete opt.loglevel;

dbagent(process, opt, logger);



