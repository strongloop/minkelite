'use strict'

module.exports = MinkeLite

var CONFIG_JSON = require('./minkelite_config.json')
var ago = require("ago")
var async = require("async")
var bodyParser = require('body-parser')
var express = require('express')
var fs = require("fs")
var md5 = require('MD5')
var sqlite3 = require("sqlite3")
var statslite = require("stats-lite")
var util = require('util')
var xtend = require('xtend')
var zlib = require('zlib')

var DISABLE_VERBOSE_MODE = true
var EXTRA_WRITE_COUNT_IN_DEVMODE = 0
var $$$ = '|'
var $$_ = '?'
var TRACE_NOT_FOUND_GZIPPED = null
zlib.gzip("The trace file not found.",function(err, buf){TRACE_NOT_FOUND_GZIPPED = buf})
var SUPPORTED_TRACER_VERSIONS = ["1.0.1"]
var MIN_DATA_POINTS_REQUIRED_FOR_MODELING = 20
var SUPRESS_NOISY_WATERFALL_SEGMENTS = false
var MINIMUM_SEGMENT_DURATION = 2

var SYSTEM_TABLES = {
  // "raw_trace", "raw_memory_pieces", "meta_transactions", "raw_transactions", "model_mean_sd"
  "system_tables":[
    {
      "name": "raw_trace",
      "columns": "pfkey TEXT PRIMARY KEY, ts INTEGER, trace BLOB"
    }
    ,{
      "name": "raw_memory_pieces",
      "columns": "pfkey TEXT PRIMARY KEY, ts INTEGER, act TEXT, host TEXT, pid INTEGER, lm_a INTEGER, p_mr INTEGER, p_mt INTEGER, p_mu INTEGER, p_ut REAL, s_la REAL"
    }
    ,{
      "name": "meta_transactions",
      "columns": "act_hour_host_pid TEXT PRIMARY KEY, ts INTEGER, act TEXT, hour INTEGER, host TEXT, pid INTEGER, trans BLOB"
    }
    ,{
      "name": "raw_transactions",
      "columns": "act_tran_ts_host_pid TEXT PRIMARY KEY, pfkey TEXT, tran TEXT, ts INTEGER, act TEXT, host TEXT, pid INTEGER, lm_a INTEGER, max INTEGER, mean REAL, min INTEGER, n INTEGER, sd REAL"
    }
    ,{
      "name": "model_mean_sd",
      "columns": "act_host_pid TEXT PRIMARY KEY, ts INTEGER, p_mu_mean REAL, p_mu_sd REAL, s_la_mean REAL, s_la_sd REAL"
    }
  ] 
}

function MinkeLite(config) {
  if (!(this instanceof MinkeLite)) return new MinkeLite(config)
  var MY_CONFIG = config ? xtend(SYSTEM_TABLES, config) : SYSTEM_TABLES
  this.config = xtend(MY_CONFIG, CONFIG_JSON)
  this.config.dev_mode = (this.config.dev_mode!=null) ? this.config.dev_mode : false
  this.config.verbose = (this.config.verbose!=null) ?  this.config.verbose : false
  if( DISABLE_VERBOSE_MODE ) this.config.verbose = false
  this.config.in_memory = (this.config.in_memory!=null) ? this.config.in_memory : true
  this.config.dir_path = this.config.dir_path || "./"
  this.config.db_name = this.config.in_memory ? ":memory:" : ( this.config.db_name || "minkelite.db" )
  this.config.sqlite3_verbose = (this.config.sqlite3_verbose!=null) ? this.config.sqlite3_verbose : false
  if ( this.config.sqlite3_verbose ) sqlite3 = sqlite3.verbose();
  this.config.stale_minutes = this.config.stale_minutes || 1*24*60
  this.config.chart_minutes = this.config.chart_minutes || 1*24*60
  this.config.pruning_interval_seconds = this.config.pruning_interval_seconds || 10*60
  this.config.start_server = (this.config.start_server!=null) ? this.config.start_server : false
  this.config.server_port = this.config.server_port || 8103
  this.config.max_transaction_count = this.config.max_transaction_count || 20
  this.config.stats_interval_seconds = this.config.stats_interval_seconds || 10*60
  this.config.compress_trace_file = this.config.compress_trace_file || true

  this._init_db()
  this._init_server()
  this.pruner = this.config.pruning_interval_seconds==0 ? null : setInterval(deleteAllStaleRecords.bind([this,this.config.stale_minutes,"minute"]), this.config.pruning_interval_seconds*1000)
  this.model_builder = setInterval(buildStats.bind([this,this.config.stale_minutes,"minute"]), this.config.stats_interval_seconds*1000)
  if ( this.config.verbose ) console.log(this)
}

MinkeLite.prototype.shutdown = function (cb) {
  // exitIfNotReady(this, "shutdown")
  if ( this.pruner ){
    clearInterval(this.pruner)
    this.pruner = null
  }
  if ( this.model_builder ){
    clearInterval(this.model_builder)
    this.model_builder = null
  }
  if ( this.db ){
    his.db.close()
    this.db = null
  }
  if ( this.express_server ){
    this.express_server.close()
    this.express_server = null
  }
}

MinkeLite.prototype.get_express_app = function () {
  return this.express_app
}

MinkeLite.prototype.start_server = function () {
  this.express_server = this.express_app.listen(this.config.server_port)
}

MinkeLite.prototype._init_db = function () {
  this.db_being_initialized = false
  this.db_path = this.config.in_memory ? this.config.db_name : this.config.dir_path+this.config.db_name
  this.db_exists = this.config.in_memory ? false : fs.existsSync(this.db_path)
  this.db = new sqlite3.Database(this.db_path)
  if ( this.db_exists ) return
  this.db_being_initialized = true
  for ( var i in this.config.system_tables ){
    var tbl = this.config.system_tables[i]
    var query = util.format("CREATE TABLE %s (%s) WITHOUT ROWID", tbl.name, tbl.columns)
    this.db.run(query, printRow)
  }
  this.db_being_initialized = false
  this.db_exists = true
}

MinkeLite.prototype._init_server = function () {
  var jsonParser = bodyParser.json({limit:100000000})
  this.express_app = express()
    .post("/post_raw_pieces/:version", jsonParser,
      function(req,res){postRawPieces(this,req,res)}.bind(this))
    .post("/results/:version", jsonParser,
      function(req,res){postRawPieces(this,req,res)}.bind(this))
    .get("/get_raw_pieces/:pfkey",
      function(req,res){getRawPieces(this,req,res)}.bind(this))
    .get("/get_raw_memory_pieces/:act/:host/:pid",
      function(req,res){getRawMemoryPieces(this,req,res)}.bind(this))
    .get("/get_meta_transactions/:act/:host/:pid",
      function(req,res){getMetaTransactions(this,req,res)}.bind(this))
    .get("/get_transaction/:act/:transaction/:host/:pid",
      function(req,res){getTransaction(this,req,res)}.bind(this))
    .get("/get_host_pid_list/:act",
      function(req,res){getHostPidList(this,req,res)}.bind(this));
  this.express_server = this.config.start_server ? this.express_app.listen(this.config.server_port,
    function(){if(this.config.verbose) console.log("MinkeLite is listening on " + this.config.server_port)}.bind(this)) : null
}

function sendCompressedTrace(err,row){
  var self = this[0]
  var res = this[1]
  var trace = TRACE_NOT_FOUND_GZIPPED
  if ( row && row.trace ){
    trace = row.trace
    if( self.config.verbose ) console.log("___ SELECT trace(compressed) for pfkey :", pfkey, '... done.')
  }
  writeHeaderJSON(res)
  res.write(trace)
  res.end()  
}

function sendUncompressedTrace(err,row){
  var self = this[0]
  var res = this[1]
  if ( row && row.trace ){
    zlib.unzip(rowData.trace, function(zlibErr, buf){
      var traceStr = buf.toString('utf-8');
      writeHeaderJSON(res,false)
      res.write(traceStr)
      res.end()    
    })
    if( self.config.verbose ) console.log("___ SELECT trace(uncompressed) for pfkey :", pfkey, '... done.')
  } else {
    writeHeaderJSON(res,false)
    res.write("The trace file not found.")
    res.end()    
  }
}

function getRawPieces(self,req,res){
  // "/get_raw_pieces/:pfkey"
  var pfkey = decodeURIComponent(req.params.pfkey)
  var db = self.db
  db.serialize(function() {
    var query = util.format("SELECT trace FROM raw_trace WHERE pfkey='%s'", pfkey)
    var callback = self.config.compress_trace_file ? sendCompressedTrace : sendUncompressedTrace
    db.get(query, callback.bind([self,res]))
  })
}

function getHostPidList(self,req,res){
  // "/get_host_pid_list/:act
  var act = decodeURIComponent(req.params.act)
  var db = self.db

  db.serialize(function() {
    var DATA = {}
    DATA["act"] = act
    DATA["hosts"] = []

    function getRowsHostPidList(err, rows){
      if (err){console.log("ERROR:",err)}
      else if ( rows ){
        var hosts = DATA["hosts"]
        for (var i in rows){
          var row = rows[i]
          var unknownHost = true
          for(var k in hosts){
            if( hosts[k]["host"]==row.host ){
              hosts[k]["pids"].push(row.pid)
              unknownHost = false
              break
            }
          }
          if( unknownHost ){
            hosts.push({"host":row.host,"pids":[row.pid]})
          }
        }
        zipAndRespond(DATA,res)
        if( self.config.verbose ) console.log("___ SELECT host,pid FROM raw_memory_pieces for act :", act, "... done.")
      }
    }

    var chartTime = ago(self.config.chart_minutes, "minutes").toString()
    var query = util.format("SELECT DISTINCT host,pid FROM (SELECT host,pid FROM raw_memory_pieces WHERE act='%s' AND ts > %s ORDER BY ts DESC)", act, chartTime)

    db.all(query,getRowsHostPidList)
  })
}

function getRawMemoryPieces(self,req,res){
  // "/get_raw_memory_pieces/:act/:host/:pid"
  var act = decodeURIComponent(req.params.act)
  var HOST = decodeURIComponent(req.params.host)
  var PID = parseInteger(decodeURIComponent(req.params.pid))
  var db = self.db

  db.serialize(function() {
    var DATA = {}
    DATA["act"] = act
    DATA["hosts"] = {}

    function getRowsRawMemoryPieces(err, rows){
      if (err){console.log("ERROR:",err)}
      else if ( rows ){
        for (var i in rows){
          var row = rows[i]
          var data = {}
          data["pfkey"] = row.pfkey
          data["ts"] = getDateTimeStr(row.ts)
          data["p_mr"] = row.p_mr
          data["p_mt"] = row.p_mt
          data["p_mu"] = row.p_mu
          data["p_ut"] = row.p_ut
          data["s_la"] = row.s_la
          data["lm_a"] = row.lm_a
          if(!(row.host in DATA["hosts"])) DATA["hosts"][row.host] = {}
          if(!(row.pid in DATA["hosts"][row.host])) DATA["hosts"][row.host][row.pid] = []
          DATA["hosts"][row.host][row.pid].push(data)
        }
        for( var host in DATA["hosts"] ){
          for( var pid in DATA["hosts"][host] ){
            DATA["hosts"][host][pid].sort(function(x,y){
              if( x.ts<y.ts ) return -1
              if( x.ts>y.ts ) return 1
              return 0
            })
          }
        }
        zipAndRespond(DATA,res)
        if( self.config.verbose ) console.log("___ SELECT FROM raw_memory_pieces for act :", act, "... done.")
      }
    }

    var chartTime = ago(self.config.chart_minutes, "minutes").toString()
    var query = null
    var baseQuery1 = util.format("SELECT pfkey,ts,host,pid,p_mr,p_mt,p_mu,p_ut,s_la,lm_a FROM raw_memory_pieces WHERE act='%s' AND ts > %s ", act, chartTime)
    var baseQuery2 = "ORDER BY ts"
    if ( HOST.length>0 && HOST!="0" && PID>0 ) {
      var querySpecificHostPid = baseQuery1+"AND host='%s' AND pid=%s "+baseQuery2
      query = util.format(querySpecificHostPid, HOST, PID.toString())
    } else if ( HOST.length>0 && HOST!="0" && PID==0 ) {
      var querySpecificHost = baseQuery1+"AND host='%s' "+baseQuery2
      query = util.format(querySpecificHost, HOST)
    } else if ( (HOST.length==0 || HOST=="0") && PID>0 ) {
      var querySpecificPid = baseQuery1+"AND pid=%s "+baseQuery2
      query = util.format(querySpecificPid, PID.toString())
    } else {
      query = baseQuery1+baseQuery2
    }
    db.all(query,getRowsRawMemoryPieces)
  })
}

function getMetaTransactions(self,req,res){
  // "/get_meta_transactions/:act/:host/:pid"
  var act = decodeURIComponent(req.params.act)
  var HOST = decodeURIComponent(req.params.host)
  var PID = parseInteger(decodeURIComponent(req.params.pid))
  var db = self.db

  db.serialize(function() {
    var DATA = {}
    DATA["act"] = act
    DATA["hosts"] = {}

    function getRowsMetaTransactions(err, rows){
      if (err){console.log("ERROR:",err)}
      else if ( rows ){
        for (var i in rows){
          var row = rows[i]
          if(!(row.host in DATA["hosts"])) DATA["hosts"][row.host] = {}
          if(!(row.pid in DATA["hosts"][row.host])) DATA["hosts"][row.host][row.pid] = []
          var transArray = decomposeTransBlob(row.trans)
          reverseSortTransArray(transArray)
          var maxTransCount = Math.min(transArray.length,self.config.max_transaction_count)
          for (var k=0; k<maxTransCount; k++){
            var tran = transArray[k][0]
            if( DATA["hosts"][row.host][row.pid].indexOf(tran) < 0 )
              DATA["hosts"][row.host][row.pid].push(tran)
          }
        }
        zipAndRespond(DATA,res)
        if( self.config.verbose ) console.log("___ SELECT FROM meta_transactions for act :", act, "... done.")
      }
    }

    var chartTime = ago(self.config.chart_minutes, "minutes").toString()
    var query = null
    var baseQuery = util.format("SELECT host,pid,trans FROM meta_transactions WHERE act='%s' AND ts > %s", act, chartTime)
    if ( HOST.length>0 && HOST!="0" && PID>0 ) {
      var querySpecificHostPid = baseQuery+" AND host='%s' AND pid=%s"
      query = util.format(querySpecificHostPid, HOST, PID.toString())
    } else if ( HOST.length>0 && HOST!="0" && PID==0 ) {
      var querySpecificHost = baseQuery+" AND host='%s'"
      query = util.format(querySpecificHost, HOST)
    } else if ( (HOST.length==0 || HOST=="0") && PID>0 ) {
      var querySpecificPid = baseQuery+" AND pid=%s"
      query = util.format(querySpecificPid, PID.toString())
    } else {
      query = baseQuery
    }
    db.all(query,getRowsMetaTransactions)
  })
}

function getTransaction(self,req,res){
  // "/get_transaction/:act/:transaction/:host/:pid"
  var act = decodeURIComponent(req.params.act)
  var tran = req.params.transaction
  var HOST = decodeURIComponent(req.params.host)
  var PID = parseInteger(decodeURIComponent(req.params.pid))
  var db = self.db

  db.serialize(function() {
    var DATA = {}
    DATA["act"] = act
    DATA["hosts"] = {}
    
    function getRowsTransactions(err, rows){
      if (err){console.log("ERROR:",err)}
      else if ( rows ){
        for (var i in rows){
          var row = rows[i]
          var data = {}
          data["pfkey"] = row.pfkey
          data["transaction"] = row.tran
          data["ts"] = getDateTimeStr(row.ts)
          data["max"] = row.max
          data["mean"] = row.mean
          data["min"] = row.min
          data["n"] = row.n
          data["sd"] = row.sd
          data["lm_a"] = row.lm_a

          if(!(row.host in DATA["hosts"])) DATA["hosts"][row.host] = {}
          if(!(row.pid in DATA["hosts"][row.host])) DATA["hosts"][row.host][row.pid] = []
          DATA["hosts"][row.host][row.pid].push(data)
        }
        for( var host in DATA["hosts"] ){
          for( var pid in DATA["hosts"][host] ){
            DATA["hosts"][host][pid].sort(function(x,y){
              if( x.ts<y.ts ) return -1
              if( x.ts>y.ts ) return 1
              return 0
            })
          }
        }
        zipAndRespond(DATA,res)
        if( self.config.verbose ) console.log("___ SELECT FROM raw_transactions for act :", act, "... done.")
      }
    }

    var chartTime = ago(self.config.chart_minutes, "minutes").toString()
    var query = null
    var baseQuery1 = util.format("SELECT tran,pfkey,ts,host,pid,max,mean,min,n,sd,lm_a FROM raw_transactions WHERE act='%s' AND tran='%s' AND ts > %s ", act, tran, chartTime)
    var baseQuery2 = util.format("ORDER BY max DESC LIMIT %s", self.config.max_transaction_count.toString())
    if ( HOST.length>0 && HOST!="0" && PID>0 ) {
      var querySpecificHostPid = baseQuery1+"AND host='%s' AND pid=%s "+baseQuery2
      query = util.format(querySpecificHostPid, HOST, PID.toString())
    } else if ( HOST.length>0 && HOST!="0" && PID==0 ) {
      var querySpecificHost = baseQuery1+"AND host='%s' "+baseQuery2
      query = util.format(querySpecificHost, HOST)
    } else if ( (HOST.length==0 || HOST=="0") && PID>0 ) {
      var querySpecificPid = baseQuery1+"AND pid=%s "+baseQuery2
      query = util.format(querySpecificPid, PID.toString())
    } else {
      query = baseQuery1+baseQuery2
    }
    db.all(query,getRowsTransactions)
  })
}

function postRawPieces(self,req,res){
  var version = decodeURIComponent(req.params.version)
  if( SUPPORTED_TRACER_VERSIONS.indexOf(version)<0 ){
    res.writeHead(400)
  } else {
    var act = req.headers['concurix-api-key']
    self._write_raw_trace(act, req.body)
    res.writeHead(202)
  }
  res.end()
}

MinkeLite.prototype._write_raw_trace = function (act, trace) {
  // exitIfNotReady(this, "_write_raw_trace")
  try {
    populateMinkeTables(this, act, trace)
  } catch (e) {
    if ( this.config.verbose ) console.log("*** Ignoring a duplicate insert for act :", act, e)
  }
}

  // "raw_trace", "raw_memory_pieces", "meta_transactions", "raw_transactions", "model_mean_sd"
function populateMinkeTables(self, act, trace){
  var pfkeys = compilePfkey(self, act,trace)
  var pfkey = pfkeys[0]
  if( self.config.verbose ) console.log(pfkeys[1],"____________________________________________________")
  var ts = trace.metadata.timestamp
  if ( self.config.dev_mode ) ts = Date.now()
  async.series([
    function(async_cb){
      self.db.serialize(function() {
        populateRawTraceTable(self, act, trace, pfkey, ts, async_cb)
        if ( self.config.verbose ) self._read_all_records("raw_trace", false)
      })
    }
    ,function(async_cb){
      self.db.serialize(function() {
        populateRawMemoryPieces(self, act, trace, pfkey, ts)
        if ( self.config.verbose ) self._read_all_records("raw_memory_pieces", false)
        populateRawTransactions(self, act, trace, pfkey, ts)
        if ( self.config.verbose ) self._read_all_records("raw_transactions", false)
        populateMetaTransactions(self, act, trace, pfkey, ts)
        if ( self.config.verbose ) self._read_all_records("meta_transactions", false)
        async_cb(null)
      })
    }
  ],function(err,result){
    if( err && self.config.verbose ){
      var tsStr = (new Date(ts)).toString()
      console.log("Trace insertion failure at", tsStr,"for",act,":",err)
    }
  })
}

function populateRawTraceTable(self, act, trace, pfkey, ts, cb){
  async.waterfall([
    function(async_cb){
      if( SUPRESS_NOISY_WATERFALL_SEGMENTS ){
        for( var k in trace.waterfalls ){
          var waterfall = trace.waterfalls[k]
          for( var i=waterfall.segments.length-1; i>=0; i-- ){
            var segment = waterfall.segments[i]
            var segment_duration = segment.end - segment.start
            if( segment_duration < MINIMUM_SEGMENT_DURATION ) waterfall.segments.splice(i,1)
          }
        }
      }
      async_cb(null)
    }
    ,function(async_cb){
      zlib.gzip(JSON.stringify(trace), function(err,buf){
        async_cb(err,buf)
      })
    }
  ],function(err,buf){
    if( err ){cb(err);return}
    var db = self.db
    ts = ts.toString()
    var stmt = db.prepare("INSERT INTO raw_trace(pfkey,ts,trace) VALUES ($pfkey,$ts,$trace)")
    var params = {}
    params.$pfkey = pfkey
    params.$ts = ts
    params.$trace = buf
    try {
      stmt.run(params)} catch (e){err = true}
    if ( !err && self.config.dev_mode ){
      for (var i = 0; i < EXTRA_WRITE_COUNT_IN_DEVMODE; i++) {
        var parts = pfkey.split($$$)
        params.$pfkey = parts[0]+$$$+md5((new Date()).toString()+Math.random().toString()+parts[1])
      try {stmt.run(params)} catch (e){err = true;break}
      }
    }
    stmt.finalize()
    cb(err)
  })
}

function getLMa(stats, trace){
  if ( stats==null ) return 0
  var p_mu = trace.monitoring.process_info.memory.heapUsed
  var s_la = trace.monitoring.system_info.loadavg["1m"]
  var p_mu_threshold = stats["p_mu_mean"] + stats["p_mu_sd"]*3
  var s_la_threshold = stats["s_la_mean"] + stats["s_la_sd"]*3
  var anomaly = ( (p_mu > p_mu_threshold) || (s_la > s_la_threshold) )
  return  anomaly ? 2 : 0
}

function populateRawMemoryPieces(self, act, trace, pfkey, ts){
  var db = self.db
  var host = trace.monitoring.system_info.hostname
  var pid = trace.metadata.pid
  var act_host_pid = act+$$$+host+$$$+pid.toString()
  async.waterfall([
    function(async_cb){
      var query = util.format("SELECT act_host_pid,p_mu_mean,p_mu_sd,s_la_mean,s_la_sd FROM model_mean_sd WHERE act_host_pid='%s'", act_host_pid)
      db.get(query, function(err,row){async_cb(null,row)})
    }
  ],function(err,stats_mean_sd){
    ts = ts.toString()
    var stmt = db.prepare("INSERT INTO raw_memory_pieces \
      ( pfkey, ts, act, host, pid, lm_a, p_mr, p_mt, p_mu, p_ut, s_la) VALUES \
      ($pfkey,$ts,$act,$host,$pid,$lm_a,$p_mr,$p_mt,$p_mu,$p_ut,$s_la)")
    var params = {}
    params.$pfkey = pfkey
    params.$ts = ts
    params.$act = act
    params.$host = host
    params.$pid = pid
    params.$p_mr = trace.monitoring.process_info.memory.rss
    params.$p_mt = trace.monitoring.process_info.memory.heapTotal
    params.$p_mu = trace.monitoring.process_info.memory.heapUsed
    params.$p_ut = trace.monitoring.process_info.uptime
    params.$s_la = trace.monitoring.system_info.loadavg["1m"]
    params.$lm_a = getLMa(stats_mean_sd, trace)
    stmt.run(params)
    stmt.finalize()
  })
}

function populateRawTransactions(self, act, trace, pfkey, ts){
  if ( !trace.transactions || !trace.transactions.transactions || Object.keys(trace.transactions.transactions).length==0 ){
    return
  }
  var db = self.db
  var host = trace.monitoring.system_info.hostname
  var pid = trace.metadata.pid
  var act_host_pid = act+$$$+host+$$$+pid.toString()
  async.waterfall([
    function(async_cb){
      var query = util.format("SELECT act_host_pid,p_mu_mean,p_mu_sd,s_la_mean,s_la_sd FROM model_mean_sd WHERE act_host_pid='%s'", act_host_pid)
      db.get(query, function(err,row){async_cb(null,row)})
    }
  ],function(err,stats_mean_sd){
    ts = ts.toString()
    var stmt = db.prepare("INSERT INTO raw_transactions \
      ( act_tran_ts_host_pid, pfkey, ts, act, host, pid, tran, lm_a, max, mean, min, n, sd) VALUES \
      ($act_tran_ts_host_pid,$pfkey,$ts,$act,$host,$pid,$tran,$lm_a,$max,$mean,$min,$n,$sd)")
    var params = {}
    params.$pfkey = pfkey
    params.$ts = ts
    params.$act = act
    params.$host = trace.monitoring.system_info.hostname
    params.$pid = trace.metadata.pid
    params.$lm_a = getLMa(stats_mean_sd, trace)
    var act_ts_host_pid = act+$$$+ts.toString()+$$$+params.$host+$$$+params.$pid.toString()
    if ( self.config.dev_mode ) act_ts_host_pid += $$$+md5((new Date()).toString()+Math.random().toString())
    for (var tran in trace.transactions.transactions){
      var stats = trace.transactions.transactions[tran].subset_stats
      params.$act_tran_ts_host_pid = act_ts_host_pid+$$$+tran
      params.$tran = tran
      params.$max = stats.max
      params.$mean = stats.mean
      params.$min = stats.min
      params.$n = stats.n
      params.$sd = stats.standard_deviation
      stmt.run(params)
    }
    stmt.finalize()
  })
}

function decomposeTransBlob(trans){
  var transArray =  []
  var trans = trans.split($$$)
  for(var i in trans){
    var parts = trans[i].split($$_)
    if( parts.length==1 ) parts.push(1)
    transArray.push([parts[0],parseInt(parts[1])])
  }
  return transArray
}

function assembleTransBlob(transArray){
  for(var i in transArray){
    var parts = transArray[i]
    transArray[i] = parts[0]+$$_+parts[1].toString()
  }
  return transArray.join($$$)
}

function isInTransArray(tran,value,transArray){
  for(var i in transArray){
    if( transArray[i][0]==tran ){
      if( transArray[i][1] > value ){ return true }
      else { transArray.splice(i,1); return false }
    }
  }
  return false
}

function reverseSortTransArray(transArray){
  transArray.sort(function(x,y){
    if( x[1]<y[1] ) return 1
    if( x[1]>y[1] ) return -1
    return 0
  })
}

function populateMetaTransactions(self, act, trace, pfkey, ts){
  if ( !trace.transactions || !trace.transactions.transactions || Object.keys(trace.transactions.transactions).length==0 ){
    if ( self.config.dev_mode ) console.log("meta_transactions - trans in trace empty ... skipped.")
    return
  }
  var db = self.db
  var hour = getHourInt(ts)
  var host = trace.monitoring.system_info.hostname
  var pid = trace.metadata.pid
  var act_hour_host_pid = act+$$$+hour.toString()+$$$+host+$$$+pid.toString()
  async.waterfall([
    function(async_cb){
      var query = util.format("SELECT trans FROM meta_transactions WHERE act_hour_host_pid='%s'", act_hour_host_pid)
      db.get(query, function(err,row){async_cb(null,row)})
    }
  ],function(err,row){
    var queryA,queryB = null
    var stmt = null
    var transArray = []
    var params = {}
    params.$act_hour_host_pid = act_hour_host_pid
    params.$ts = ts
    if ( row ){
      transArray = decomposeTransBlob(row.trans)
      queryA = "UPDATE meta_transactions SET ts=$ts,trans=$trans WHERE act_hour_host_pid=$act_hour_host_pid"
      queryB = "UPDATE meta_transactions SET ts=$ts WHERE act_hour_host_pid=$act_hour_host_pid"
      if ( self.config.verbose ) process.stdout.write("___ UPDATE meta_transactions")
    }
    else{
      queryA ="INSERT INTO meta_transactions \
        ( act_hour_host_pid, act, ts, hour, host, pid, trans) VALUES \
        ($act_hour_host_pid,$act,$ts,$hour,$host,$pid,$trans)"
      queryB = queryA
      if ( self.config.verbose ) process.stdout.write("___ INSERT meta_transactions")
      params.$act = act
      params.$hour = hour
      params.$host = host
      params.$pid = pid
    }
    var newTransAdded = false
    for (var tran in trace.transactions.transactions){
      var value = trace.transactions.transactions[tran].subset_stats.max
      if ( ! isInTransArray(tran,value,transArray) ){
        transArray.push([tran,value]);
        newTransAdded=true;
      }
    }
    if ( newTransAdded ){
      stmt = db.prepare(queryA)
      params.$trans = assembleTransBlob(transArray)
      if ( self.config.verbose ) console.log(" for",act_hour_host_pid,"... done.")
    }
    else{
      stmt = db.prepare(queryB)
      if ( self.config.verbose ) console.log(" for",act_hour_host_pid,"... skipped.")
    }
    stmt.run(params)
    stmt.finalize()
  })
}

MinkeLite.prototype._read_all_records = function (table, showContents) {
  // exitIfNotReady(this, "_read_all_records")
  var db = this.db
  var query = util.format("SELECT %s FROM %s", showContents ? "*" : "count(*)", table)
  db.serialize(function(){
    db.each(query, function(err,row){process.stdout.write(table+" :\t");printRow(err,row)})
  })
}

MinkeLite.prototype._list_tables = function (callback) {
  // exitIfNotReady(this, "_list_tables")
  var db = this.db
  var query = "SELECT name FROM sqlite_master WHERE type='table'"
  db.all(query, function(err,rows){
    var tableNames = []
    for (var i in rows){tableNames.push(rows[i].name)}
    callback(err,tableNames)
  })
}

MinkeLite.prototype._delete_stale_records = function (tableName, value, unitStr) {
  // exitIfNotReady(this, "_delete_stale_records")
  var db = this.db
  var tsThereshold = ago(value, unitStr)
  var query = util.format("DELETE FROM %s WHERE ts < %s", tableName, tsThereshold.toString())
  if ( this.config.verbose ) console.log("___ DELETE FROM",tableName,'... done.')
  db.run(query)
}

function deleteAllStaleRecords () {
  var self = this[0]
  var value = this[1]
  var unitStr = this[2]
  self.db.serialize(function(){
    for (var i in self.config.system_tables){
      var tableName = self.config.system_tables[i].name
      self._delete_stale_records(tableName,value,unitStr)
    }
  })
}

function populateStatsMeanSd(self, value, unitStr){
  var db = self.db
  var DATA = {}

  function meanAndSdOfArray(array){
    var mean = statslite.mean(array)
    var sd = statslite.stdev(array)
    return [mean, sd]
  }

  function readRowsRawMemoryPieces(err, rows){
    if (err){console.log("ERROR:",err)}
    else if (rows!=null){
      DATA["ts"] = Date.now()
      DATA["points"] = {}
      for (var i in rows){
        var row = rows[i]
        var act_host_pid = row.act+$$$+row.host+$$$+row.pid.toString()
        if( act_host_pid in DATA["points"] ){
          var dp = DATA["points"][act_host_pid]
          dp["p_mu"].push(row.p_mu)       
          dp["s_la"].push(row.s_la)        
        } else {
          var dp = {}
          dp["p_mu"] = [row.p_mu]
          dp["s_la"] = [row.s_la]
          DATA["points"][act_host_pid] = dp
        }
      }
      for (var act_host_pid in DATA["points"]){
        var dp = DATA["points"][act_host_pid]
        if( dp["p_mu"].length < MIN_DATA_POINTS_REQUIRED_FOR_MODELING ){
          delete DATA["points"][act_host_pid]
          continue
        }
        var meanSd = meanAndSdOfArray(dp["p_mu"])
        dp["p_mu_mean"] = meanSd[0]
        dp["p_mu_sd"] = meanSd[1]
        var meanSd = meanAndSdOfArray(dp["s_la"])
        dp["s_la_mean"] = meanSd[0]
        dp["s_la_sd"] = meanSd[1]
        delete dp["p_mu"]
        delete dp["s_la"]
      }
    }
    this()
  }

  var tsThereshold = ago(value, unitStr)
  var querySelect = util.format("SELECT act_host_pid FROM model_mean_sd WHERE act_host_pid = $act_host_pid AND ts > %s", tsThereshold)
  var queryUpdate = "UPDATE model_mean_sd SET ts=$ts, \
    p_mu_mean=$p_mu_mean,p_mu_sd=$p_mu_sd,s_la_mean=$s_la_mean,s_la_sd=$s_la_sd \
    WHERE act_host_pid=$act_host_pid"
  var queryInsert = "INSERT INTO model_mean_sd \
    ( act_host_pid, ts, p_mu_mean, p_mu_sd, s_la_mean, s_la_sd ) VALUES \
    ($act_host_pid,$ts,$p_mu_mean,$p_mu_sd,$s_la_mean,$s_la_sd )"
  async.series([
    function(cb){
      db.serialize(function() {
        var query = util.format("SELECT act,host,pid,p_mu,s_la FROM raw_memory_pieces WHERE ts > %s", tsThereshold)
        db.all(query,readRowsRawMemoryPieces.bind(cb))
      })
    }
  ],
  function(err,result){
    var nowStr = getDateTimeStr(DATA["ts"])
    db.serialize(function() {
      var stmtSelect = db.prepare(querySelect)
      var stmtUpdate = db.prepare(queryUpdate)
      var stmtInsert = db.prepare(queryInsert)
      var keys = Object.keys(DATA["points"])
      for (var i in keys){
        var act_host_pid = keys[i]
        var lastActHostPid = (i==keys.length-1)
        var dp = DATA["points"][act_host_pid]
        var asyncP = {"ahp":act_host_pid, "lastOne":lastActHostPid, "dPoint":dp, "stmtS":stmtSelect, "stmtU": stmtUpdate, "stmtI": stmtInsert, "nowStr": nowStr}
        async.waterfall([
          function(cb){cb(null,this)}.bind(asyncP)
          ,function(AP,cb){
            var params = {}
            params.$act_host_pid = AP.ahp
            stmtSelect.get(params,function(err,row){
              var found = false
              if (err){console.log("ERROR:",err)}
              else{found = (row!=null)}
              AP.found = found
              cb(null,AP)
            })
          }]
          ,function(err,AP){
            var stmt = null
            if ( AP.found ){
              stmt = AP.stmtU
              if ( self.config.verbose ) process.stdout.write("___ UPDATE model_mean_sd")
            } else {
              stmt = AP.stmtI
              if ( self.config.verbose ) process.stdout.write("___ INSERT model_mean_sd")              
            }
            var params = {}
            params.$act_host_pid = AP.ahp
            params.$ts = AP.nowStr
            params.$p_mu_mean = AP.dPoint["p_mu_mean"]
            params.$p_mu_sd = AP.dPoint["p_mu_sd"]
            params.$s_la_mean = AP.dPoint["s_la_mean"]
            params.$s_la_sd = AP.dPoint["s_la_sd"]
            stmt.run(params,function(err){
              if ( self.config.verbose ) console.log(" for",this.ahp,"... done.")
              if ( this.lastOne ){
                this.stmtS.finalize()
                this.stmtU.finalize()
                this.stmtI.finalize()
              }
            }.bind(AP))
          }
        )
      }
    })
  })
}

// async.waterfall([function(cb){cb(null,123)}],function(err,result){console.log(result)})

function buildStats () {
  var self = this[0]
  var value = this[1]
  var unitStr = this[2]
  self.db.serialize(function(){
    populateStatsMeanSd(self,value,unitStr)
    if ( self.config.verbose ) self._read_all_records("model_mean_sd", false)
  })
}
// Utilities

// var trace = {monitoring:{system_info:{hostname:"hostname"}},metadata:{pid:12345,timestamp:12345467890123}};
// compilePfkey("cx-dataserver",trace)
// --> 'cx-dataserver#0/hostname#0/12345/12345467890.json'
function compilePfkey(self, act, trace){
  var pfkey = act+"/"
  pfkey += trace.monitoring.system_info.hostname
  if ( self.config.dev_mode ) pfkey += $$$+md5((new Date()).toString()+Math.random().toString())
  pfkey += "#0/"
  pfkey += trace.metadata.pid.toString()+"/"
  pfkey += Math.floor(trace.metadata.timestamp/1000).toString()+".json"
  return [md5(act)+$$$+md5(pfkey), pfkey]
}

function parseInteger(str){
  if( str==null ) return null
  var intValue = null
  try{intValue = parseInt(str)} catch (e) {intValue = 0}
  return intValue
}

function writeHeaderJSON(res,compress){
  compress = ( compress==null ) ? true : compress
  var option = {'Content-Type': 'application/json'}
  if( compress ) option['Content-Encoding'] = 'gzip'
  res.writeHead(200, option)
}

function zipAndRespond(data,res){
  data["timestamp"] = getDateTimeStr(Date.now())
  zlib.gzip(JSON.stringify(data),function(err, gzipped_buf){
    writeHeaderJSON(res)
    res.write(gzipped_buf)
    res.end()
  })
}

var getDateTimeStr = getISODateTimeStr

function getGMTDateTimeStr(ts){

  function zeroFill(s){
    if ( s.length==1 ) s = '0'+s
    return s
  }

  var dt = new Date(ts)
  var dtStr = dt.getUTCFullYear().toString()
  dtStr += '-'+zeroFill((dt.getUTCMonth()+1).toString())
  dtStr += '-'+zeroFill(dt.getUTCDate().toString())
  dtStr += ' '+zeroFill(dt.getUTCHours().toString())
  dtStr += ':'+zeroFill(dt.getUTCMinutes().toString())
  dtStr += ':'+zeroFill(dt.getUTCSeconds().toString())
  return dtStr+" GMT"
}

function getISODateTimeStr(ts){
  var dt = new Date(ts)
  return dt.toISOString()
}

function getHourInt(epochTs){
  var dt = new Date(epochTs)
  return 1000000*dt.getUTCFullYear()+10000*(dt.getUTCMonth()+1)+100*dt.getUTCDate()+dt.getUTCHours()
}

function printRow(err, row){
  if (err){console.log("ERROR:",err)}
  else if (row!=null){
    console.log(JSON.stringify(row).substring(0,1000))
  }
}

function exitIfNotReady(inst, myName){
  if ( inst.db_being_initialized ){ console.log(myName, " db being initialized."); process.exit(1) }
  if ( ! inst.db_exists ){ console.log(myName, " db does not exist."); process.exit(1) }
}
