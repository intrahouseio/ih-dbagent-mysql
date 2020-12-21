/**
 *
 */
// const util = require('util');

function recordsForTrend(records, dnarr) {
  if (!dnarr || !dnarr.length || !records || !records.length) return [];

  // Свойства пока откинуть??
  const dArr = dnarr.map(item => item.split('.')[0]);
  const rarr = [];
  const len = dArr.length;
  let last_ts;

  for (let i = 0; i < records.length; i++) {
    const rec = records[i];

    if (!rec || !rec.dn || !rec.time) continue;
    const dn_indx = dArr.findIndex(el => el == rec.dn);
    if (dn_indx < 0) continue;

    let data;
    // const ts = Date.parse(rec.time);
    const ts = rec.time;
    // multiple series data combine
    if (ts != last_ts) {
      data = new Array(len + 1).fill(null);
      data[0] = ts;
      last_ts = ts;
    } else {
      data = rarr.pop();
    }
    data[1 + dn_indx] = Number(rec.val);
    rarr.push(data);
  }
  return rarr;
}

function getQueryStr(query, dnarr) {
  // Время и значения должны быть в одинарных скобках, имена полей в двойных!!
  const from = query.start;
  const to = query.end;

  return `select * from records ${formWhereQuery(dnarr, from, to)} order by ts`;
}

function formWhereQuery(dnarr, from, to) {
  let query = '';
  let first = true;

  if (dnarr && dnarr.length > 0) {
    if (dnarr.length == 1) {
      query += dnAndProp(dnarr[0]);
      first = false;
    } else {
      query += ' ( ';
      for (let i = 0; i < dnarr.length; i++) {
        if (dnarr[i]) {
          query += isFirst(' OR ') + ' (' + dnAndProp(dnarr[i]) + ')';
        }
      }
      query += ' ) ';
    }
  }

  if (from) {
    query += isFirst(' AND ') + ' ts > ' + from;
  }

  if (to) {
    query += isFirst(' AND ') + ' ts < ' + to;
  }

  return query ? ' WHERE ' + query : '';

  function isFirst(op) {
    return first ? ((first = false), '') : op;
  }

  function dnAndProp(dn_prop) {
    if (dn_prop.indexOf('.') > 0) {
      const splited = dn_prop.split('.');
      return " dn = '" + splited[0] + "' AND prop = '" + splited[1] + "' ";
    }
    // Иначе это просто dn
    return " dn = '" + dn_prop + "'";
  }
}

function getShortErrStr(e) {
  if (typeof e == 'object') return e.message ? getErrTail(e.message) : JSON.stringify(e);
  if (typeof e == 'string') return e.indexOf('\n') ? e.split('\n').shift() : e;
  return String(e);

  function getErrTail(str) {
    let idx = str.lastIndexOf('error:');
    return idx > 0 ? str.substr(idx + 6) : str;
  }
}

function getDateStr() {
  const dt = new Date();
  return (
    pad(dt.getDate()) +
    '.' +
    pad(dt.getMonth() + 1) +
    ' ' +
    pad(dt.getHours()) +
    ':' +
    pad(dt.getMinutes()) +
    ':' +
    pad(dt.getSeconds()) +
    '.' +
    pad(dt.getMilliseconds(), 3)
  );
}

function pad(str, len = 2) {
  return String(str).padStart(len, '0');
}

module.exports = {
  recordsForTrend,
  getQueryStr,
  formWhereQuery,
  getShortErrStr,
  getDateStr
};
