'use strict';

// TO-DO
// 1. minute_ix -- bit-wise marking, or minute_ix, minute_ix1, minute_ix2
// 2. lm_a -- requires model parameters - to be stored on the DB
// 3. remove obsolete HTTP API parameters such as :filter in get_transaction, :fun/:met/:mon/:tra/:wat in get_raw_pieces
// 4. server-side sampling  -- simply limit chart_time to 1 day or even shorer?

module.exports = MinkeLite;

var CONFIG = require('./minkelite_config.json');
var ago = require("ago");
var async = require("async");
var fs = require("fs");
var md5 = require('MD5');
var sqlite3 = require("sqlite3");
var util = require('util');
var xtend = require('xtend');
var zlib = require('zlib');

var express = require('express');
var formidable = require('formidable');

var DEV_MODE = true;
var EXTRA_WRITE_COUNT_IN_DEV_MODE = 0;
var $$$ = '|';
var traceNotFoundGzipped = null;
zlib.gzip("The trace file not found.",function(err, buf){traceNotFoundGzipped = buf});

function MinkeLite(config) {
  if (!(this instanceof MinkeLite)) return new MinkeLite(config)
  this.config = config ? xtend(CONFIG, config) : CONFIG;
  // set production default config parameters
  this.config.dir_path = this.config.dir_path || "./";
  this.config.db_name = this.config.db_name || ":memory:";
  this.config.sqlite3_verbose = this.config.sqlite3_verbose || false;
  if ( this.config.sqlite3_verbose ) sqlite3 = sqlite3.verbose();
  this.config.stale_minutes = this.config.stale_minutes || 1*24*60;
  this.config.chart_minutes = this.config.chart_minutes || 1*24*60;
  this.config.pruning_interval_seconds = this.config.pruning_interval_seconds || 600;
  this.config.start_server = this.config.start_server || false;
  this.config.server_port = this.config.server_port || 8103;

  this._init_db();
  this._init_server();
  this.pruner = setInterval(deleteAllStaleRecords.bind([this,this.config.stale_minutes,"minute"]), this.config.pruning_interval_seconds*1000);
  if ( DEV_MODE ) console.log(this);
}

MinkeLite.prototype.shutdown = function (cb) {
  exitIfNotReady(this, "shutdown");
  if ( this.db.pruner ){
    clearInterval(this.db.pruner);
    this.db.pruner = null;
  };
  if ( this.db ){
    his.db.close();
    this.db = null;
  };
  if ( this.express_server ){
    this.express_server.close();
    this.express_server = null;
  };
};

MinkeLite.prototype.get_express_app = function () {
  return this.express_app;
}

MinkeLite.prototype.start_server = function () {
  this.express_server = this.express_app.listen(this.config.server_port);
}

MinkeLite.prototype._init_db = function () {
  this.db_being_initialized = false;
  this.db_path = this.config.in_memory ? this.config.db_name : this.config.dir_path+this.config.db_name;
  this.db_exists = this.config.in_memory ? false : fs.db_existsSync(this.db_path);
  this.db = new sqlite3.Database(this.db_path);
  if ( this.db_exists ) return;
  this.db_being_initialized = true;
  for ( var i in this.config.tables ){
    var tbl = this.config.tables[i];
    var query = util.format("CREATE TABLE %s (%s) WITHOUT ROWID", tbl.name, tbl.columns);
    this.db.run(query, printRow);
  }
  this.db_being_initialized = false;
  this.db_exists = true;
}

MinkeLite.prototype._init_server = function () {
  this.express_app = express()
  .post("/post_raw_pieces",
    function(req,res){postRawPieces(this,req,res)}.bind(this))
  .get("/get_raw_pieces/:pfkey/:fun/:met/:mon/:tra/:wat",
    function(req,res){getRawPieces(this,req,res)}.bind(this))
  .get("/get_raw_memory_pieces/:act/:filter/:host/:pid",
    function(req,res){getRawMemoryPieces(this,req,res)}.bind(this))
  .get("/get_meta_transactions/:act",
    function(req,res){getMetaTransactions(this,req,res)}.bind(this))
  .get("/get_transaction/:act/:transaction/:filter/:host/:pid",
    function(req,res){getTransaction(this,req,res)}.bind(this));
  this.express_server = this.config.start_server ? this.express_app.listen(this.config.server_port) : null;
};

function getRawPieces(self,req,res){
  // "/get_raw_pieces/:pfkey/:fun/:met/:mon/:tra/:wat"
  var pfkey = decodeURIComponent(req.params.pfkey);
  var db = self.db;
  db.serialize(function() {
    // var chartTime = ago(self.config.chart_minutes, "minutes").toString();
    var query = util.format("SELECT trace FROM raw_trace WHERE pfkey='%s'", pfkey);
    db.get(query, function(err,row){
      var trace = traceNotFoundGzipped;
      if ( row && row.trace ) trace = row.trace;
      writeHeaderJSON(res);
      res.write(trace);
      res.end();
    });
  });
};

function getRawMemoryPieces(self,req,res){
  // "/get_raw_memory_pieces/:act/:filter/:host/:pid"
  var act = decodeURIComponent(req.params.act);
  var host = decodeURIComponent(req.params.host);
  var pid = parseInteger(decodeURIComponent(req.params.pid));
  var db = self.db;

  db.serialize(function() {
    var DATA = {};
    DATA["act"] = act;
    DATA["points"] = [];
    var HOST = null;
    var PID = null;

    function getRowsRawMemoryPieces(err, rows){
      if (err){console.log("ERROR:",err)}
      else if (rows!=null){
        for (var i in rows){
          var row = rows[i];
          var data = {};
          data["pfkey"] = row.pfkey;
          data["ts"] = getDateTimeStr(row.ts);
          data["p_mr"] = row.p_mr;
          data["p_mt"] = row.p_mt;
          data["p_mu"] = row.p_mu;
          data["p_ut"] = row.p_ut;
          data["s_la"] = row.s_la;
          data["lm_a"] = row.lm_a;
          DATA["points"].push(data); 
        };
        DATA["host"] = HOST;
        DATA["pid"] = PID;
        zipAndRespond(DATA,res);
     };
    };

    var chartTime = ago(self.config.chart_minutes, "minutes").toString();
    var query = util.format("SELECT host,pid FROM raw_memory_pieces WHERE act='%s' AND ts > %s ORDER BY ts DESC", act, chartTime);
    async.series([
      function(cb){db.get(query,function(err,row){HOST=row.host;PID=row.pid;cb()});}
    ],
    function(err,result){
      query = util.format("SELECT pfkey,ts,host,pid,p_mr,p_mt,p_mu,p_ut,s_la,lm_a FROM raw_memory_pieces WHERE act='%s' AND host='%s' AND pid=%s AND ts > %s ORDER BY ts", act, HOST, PID.toString(), chartTime);
      db.all(query,getRowsRawMemoryPieces);
    });
  });
};

function getMetaTransactions(self,req,res){
  // "/get_meta_transactions/:act"
  var act = decodeURIComponent(req.params.act);
  var db = self.db;
  db.serialize(function() {
    var DATA = {};
    DATA["act"] = act;
    DATA["transactions"] = [];
    var data = {};
    data["hour"] = 0;
    data["transactions"] = [];
    DATA["transactions"].push(data);

    function getRowsMetaTransactions(err, rows){
      if (err){console.log("ERROR:",err)}
      else if (rows!=null){
        for (var i in rows){
          var row = rows[i];
          data["hour"] = row.hour;
          data["transactions"] = [];
          var trans = row.trans.split($$$);
          for (var k in trans){
            DATA["transactions"][0]["transactions"].push(trans[k]);
          };
        };
        zipAndRespond(DATA,res);
     };
    };

    var chartTime = ago(self.config.chart_minutes, "minutes").toString();
    var query = util.format("SELECT hour,trans FROM meta_transactions WHERE act='%s' AND ts > %s", act, chartTime);
    db.all(query,getRowsMetaTransactions);
  });
};

function getTransaction(self,req,res){
  // "/get_transaction/:act/:transaction/:filter/:host/:pid"
  var act = decodeURIComponent(req.params.act);
  var tran = req.params.transaction;
  var filter = parseInteger(decodeURIComponent(req.params.filter));
  var host = decodeURIComponent(req.params.host);
  var pid = parseInteger(decodeURIComponent(req.params.pid));
  var db = self.db;

  db.serialize(function() {
    var DATA = {};
    DATA["act"] = act;
    DATA["points"] = [];

    function getRowsTransactions(err, rows){
      if (err){console.log("ERROR:",err)}
      else if (rows!=null){
        for (var i in rows){
          var row = rows[i];
          var data = {}
          data["host"] = row.host;
          data["pid"] = row.pid;
          data["pfkey"] = row.pfkey;
          data["transaction"] = row.tran;
          data["ts"] = getDateTimeStr(row.ts);
          data["max"] = row.max;
          data["mean"] = row.mean;
          data["min"] = row.min;
          data["n"] = row.n;
          data["sd"] = row.sd;
          data["lm_a"] = row.lm_a;
          DATA["points"].push(data)
        };
        DATA["points"].sort(function(x,y){
          if( x.transaction<y.transaction ) return -1;
          if( x.transaction>y.transaction ) return 1;
          if( x.ts<y.ts ) return -1;
          if( x.ts>y.ts ) return 1;
          return 0;          
        });
        zipAndRespond(DATA,res);
      };
    };

    var query = null;
    var chartTime = ago(self.config.chart_minutes, "minutes").toString();
    if ( host && pid && host.length>0 && host!="0" && pid>0 ) {
      var baseQueryWithHostPid = "SELECT tran,pfkey,ts,host,pid,max,mean,min,n,sd,lm_a FROM raw_transactions WHERE act='%s' AND tran='%s' AND host='%s' AND pid=%s AND ts > %s ORDER BY max DESC LIMIT 20";
      query = util.format(baseQueryWithHostPid, act, tran, host, pid, chartTime);
    } else {
      var baseQueryWithoutHostPid = "SELECT tran,pfkey,ts,host,pid,max,mean,min,n,sd,lm_a FROM raw_transactions WHERE act='%s' AND tran='%s' AND ts > %s ORDER BY max DESC LIMIT 20";
      query = util.format(baseQueryWithoutHostPid, act, tran, chartTime);
    }
    db.all(query,getRowsTransactions);
  });
};

function postRawPieces(self,req,res){
  var form = new formidable.IncomingForm();
  form.parse(req, function(err, fields, files) {
    var traceGz = new Buffer(fields.file,'base64');
    // console.log("postRawPieces :", traceGz.constructor.name, traceGz.toString().length, traceGz.toString('hex').substring(0,200),'...');
    self._write_raw_trace(fields.act, traceGz);
    res.end();
  });
};

MinkeLite.prototype._write_raw_trace = function (act, traceGz) {
  exitIfNotReady(this, "_write_raw_trace");
  try {
    populateMinkeTables(this, act, traceGz);
  } catch (e) {
    console.log("*** Ignoring a duplicate insert for act :", act, e);
  }
};

function populateMinkeTables(self, act, traceGz){
  zlib.unzip(traceGz, function(zlibErr, buf){
    if( zlibErr && DEV_MODE ) console.log(zlibErr);
    if(! zlibErr && buf ) {
      var trace = JSON.parse(buf.toString('utf-8'));
      var pfkey = compilePfkey(act,trace);
      var ts = trace.metadata.timestamp;
      if ( DEV_MODE ) ts = Date.now();
      self.db.serialize(function() {
        populateRawTraceTable(self, act, traceGz, pfkey, ts);
        populateRawMemoryPieces(self, act, trace, pfkey, ts);
        populateRawTransactions(self, act, trace, pfkey, ts);
        populateMetaTransactions(self, act, trace, pfkey, ts);
      });
    };
  });
};

function populateRawTraceTable(self, act, trace, pfkey, ts){
  var db = self.db;
  ts = ts.toString();
  var stmt = db.prepare("INSERT INTO raw_trace(pfkey,ts,trace) VALUES ($pfkey,$ts,$trace)");
  var params = {};
  params.$pfkey = pfkey;
  params.$ts = ts;
  params.$trace = trace;
  stmt.run(params);

  if ( DEV_MODE )
    for (var i = 0; i < EXTRA_WRITE_COUNT_IN_DEV_MODE; i++) {
      var parts = pfkey.split($$$);
      params.$pfkey = parts[0]+$$$+md5((new Date()).toString()+Math.random().toString()+parts[1]);
    stmt.run(params);
    };

  stmt.finalize();
};

function populateRawMemoryPieces(self, act, trace, pfkey, ts){
  var db = self.db;
  ts = ts.toString();
  var stmt = db.prepare("INSERT INTO raw_memory_pieces \
    ( pfkey, ts, act, host, pid, lm_a, minute_ix, p_mr, p_mt, p_mu, p_ut, s_la) VALUES \
    ($pfkey,$ts,$act,$host,$pid,$lm_a,$minute_ix,$p_mr,$p_mt,$p_mu,$p_ut,$s_la)");
  var params = {};
  params.$pfkey = pfkey;
  params.$ts = ts;
  params.$act = act;
  params.$host = trace.monitoring.system_info.hostname;
  params.$pid = trace.metadata.pid;
  params.$lm_a = 0;
  params.$minute_ix = 1;
  params.$p_mr = trace.monitoring.process_info.memory.rss;
  params.$p_mt = trace.monitoring.process_info.memory.heapTotal;
  params.$p_mu = trace.monitoring.process_info.memory.heapUsed;
  params.$p_ut = trace.monitoring.process_info.uptime;
  params.$s_la = trace.monitoring.system_info.loadavg["1m"];
  stmt.run(params);
  stmt.finalize();
};

function populateRawTransactions(self, act, trace, pfkey, ts){
  if ( !trace.transactions || !trace.transactions.transactions || Object.keys(trace.transactions.transactions).length==0 ){
    return;
  };
  var db = self.db;
  ts = ts.toString();
  var stmt = db.prepare("INSERT INTO raw_transactions \
    ( act_tran_ts_host_pid, pfkey, ts, act, host, pid, tran, lm_a, minute_ix, max, mean, min, n, sd) VALUES \
    ($act_tran_ts_host_pid,$pfkey,$ts,$act,$host,$pid,$tran,$lm_a,$minute_ix,$max,$mean,$min,$n,$sd)");
  var params = {};
  params.$pfkey = pfkey;
  params.$ts = ts;
  params.$act = act;
  params.$host = trace.monitoring.system_info.hostname;
  params.$pid = trace.metadata.pid;
  params.$lm_a = 0;
  params.$minute_ix = 1;
  var act_ts_host_pid = act+$$$+ts.toString()+$$$+params.$host+$$$+params.$pid.toString();
  if ( DEV_MODE ) act_ts_host_pid += $$$+md5((new Date()).toString()+Math.random().toString());
  for (var tran in trace.transactions.transactions){
    var stats = trace.transactions.transactions[tran].subset_stats;
    params.$act_tran_ts_host_pid = act_ts_host_pid+$$$+tran;
    params.$tran = tran;
    params.$max = stats.max;
    params.$mean = stats.mean;
    params.$min = stats.min;
    params.$n = stats.n;
    params.$sd = stats.standard_deviation;
    stmt.run(params);
  };
  stmt.finalize();
};

function populateMetaTransactions(self, act, trace, pfkey, ts){
  if ( !trace.transactions || !trace.transactions.transactions || Object.keys(trace.transactions.transactions).length==0 ){
    if ( DEV_MODE ) console.log("meta_transactions - trans in trace empty ... skipped.")
    return;
  };
  var db = self.db;
  var hour = getHourInt(ts);
  var host = trace.monitoring.system_info.hostname;
  var pid = trace.metadata.pid;
  var act_hour_host_pid = act+$$$+hour.toString()+$$$+host+$$$+pid.toString();
  async.waterfall([
    function(async_cb){
      var query = util.format("SELECT trans FROM meta_transactions WHERE act_hour_host_pid='%s'", act_hour_host_pid);
      db.get(query, function(err,row){async_cb(null,row)});
    }
  ],function(err,row){
    var query = null;
    var trans = [];
    var params = {};
    params.$act_hour_host_pid = act_hour_host_pid;
    if ( row ){
      trans = row.trans.split($$$);
      query = "UPDATE meta_transactions SET trans=$trans WHERE act_hour_host_pid=$act_hour_host_pid";
      if ( DEV_MODE ) process.stdout.write("UPDATE meta_transactions ...");
    }
    else{
      query = "INSERT INTO meta_transactions \
        ( act_hour_host_pid, act, ts, hour, host, pid, trans) VALUES \
        ($act_hour_host_pid,$act,$ts,$hour,$host,$pid,$trans)";
      if ( DEV_MODE ) process.stdout.write("INSERT meta_transactions ...");
      params.$act = act;
      params.$ts = ts;
      params.$hour = hour;
      params.$host = host;
      params.$pid = pid;
    };
    var newTransAdded = false;
    for (var tran in trace.transactions.transactions){
      if ( trans.indexOf(tran)<0 ){trans.push(tran); newTransAdded=true};
    };
    if ( newTransAdded ){
      var stmt = db.prepare(query);
      trans = trans.join($$$);
      params.$trans = trans;
      stmt.run(params);
      stmt.finalize();
      if ( DEV_MODE ) console.log(" done.");
    }
    else{
      if ( DEV_MODE ) console.log(" skipped.");
    };
  });
};

MinkeLite.prototype._read_all_records = function (table, showContents) {
  exitIfNotReady(this, "_read_all_records");
  var db = this.db;
  var query = util.format("SELECT %s FROM %s", showContents ? "*" : "count(*)", table);
  db.serialize(function(){
    db.each(query, function(err,row){process.stdout.write(table+" :\t");printRow(err,row)});
  });
}

MinkeLite.prototype._list_tables = function (callback) {
  exitIfNotReady(this, "_list_tables");
  var db = this.db;
  var query = "SELECT name FROM sqlite_master WHERE type='table'";
  db.all(query, function(err,rows){
    var tableNames = []
    for (var i in rows){tableNames.push(rows[i].name)};
    callback(err,tableNames);
  });
}

MinkeLite.prototype._delete_stale_records = function (tableName, value, unitStr) {
  exitIfNotReady(this, "_delete_stale_records");
  var db = this.db;
  var tsThereshold = ago(value, unitStr);
  var query = util.format("DELETE FROM %s WHERE ts < %s", tableName, tsThereshold.toString());
  db.run(query);
}

function deleteAllStaleRecords () {
  var self = this[0];
  var value = this[1];
  var unitStr = this[2];
  self.db.serialize(function(){
    process.stdout.write("pruning ");
    for (var i in self.config.tables){
      var tableName = self.config.tables[i].name;
      if ( i>0 ) process.stdout.write(', ')
      process.stdout.write(tableName);
      self._delete_stale_records(tableName,value,unitStr);
    };
    console.log(" ... done.");
  });
}


// Utilities

// var trace = {monitoring:{system_info:{hostname:"hostname"}},metadata:{pid:12345,timestamp:12345467890123}};
// compilePfkey("cx-dataserver",trace)
// --> 'cx-dataserver#0/hostname#0/12345/12345467890.json'
function compilePfkey(act, trace){
  var pfkey = act+"#0/";
  pfkey += trace.monitoring.system_info.hostname;
  if ( DEV_MODE ) pfkey += $$$+md5((new Date()).toString()+Math.random().toString());
  pfkey += "#0/";
  pfkey += trace.metadata.pid.toString()+"/";
  pfkey += Math.floor(trace.metadata.timestamp/1000).toString()+".json";
  pfkey = md5(act)+$$$+md5(pfkey);
  return pfkey;
}

function parseInteger(str){
  var intValue = null;
  try{intValue = parseInt(str)} catch (e) {intValue = 0};
  return intValue;
}

function writeHeaderJSON(res){
  res.writeHead(200, {'Content-Type': 'application/json', 'Content-Encoding': 'gzip'});
};

function zipAndRespond(data,res){
  zlib.gzip(JSON.stringify(data),function(err, gzipped_buf){
    writeHeaderJSON(res);
    res.write(gzipped_buf);
    res.end();
  });
};

function getDateTimeStr(ts){

  function zeroFill(s){
    if ( s.length==1 ) s = '0'+s;
    return s;
  }

  var dt = new Date(ts);
  var dtStr = dt.getUTCFullYear().toString();
  dtStr += '-'+zeroFill((dt.getUTCMonth()+1).toString());
  dtStr += '-'+zeroFill(dt.getUTCDate().toString());
  dtStr += ' '+zeroFill(dt.getUTCHours().toString());
  dtStr += ':'+zeroFill(dt.getUTCMinutes().toString());
  dtStr += ':'+zeroFill(dt.getUTCSeconds().toString());
  return dtStr;
}

function getHourInt(epochTs){
  var dt = new Date(epochTs);
  return 1000000*dt.getUTCFullYear()+10000*(dt.getUTCMonth()+1)+100*dt.getUTCDate()+dt.getUTCHours()
};

function printRow(err, row){
  if (err){console.log("ERROR:",err)}
  else if (row!=null){
    console.log(JSON.stringify(row).substring(0,1000));
  };
}

function exitIfNotReady(inst, myName){
  if ( inst.db_being_initialized ){ console.log(myName, " db being initialized."); process.exit(1) };
  if ( ! inst.db_exists ){ console.log(myName, " db does not exist."); process.exit(1) };  
}
