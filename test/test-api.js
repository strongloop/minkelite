'use strict';

var MinkeLite = require('../index');
var trace = require('./helloworld.json');
// adjust timestaps in the trace file to current for MinkeLite not to filter out
var TIME_STAMP = Date.now();
trace.metadata.timestamp = TIME_STAMP;
trace.monitoring.statware.checktime = trace.metadata.timestamp;
trace.transactions.end = trace.metadata.timestamp;
trace.transactions.start = trace.transactions.end - 22130;

var tap = require('tap');

var ML = new MinkeLite();;
var VERSION = "1.1.2";
var ACT = "wfp:helloworld";
var PFKEY = null // acquired in getRawMemoryPieces Test
var HASHED_ACT = "4307e61e9cfc31889b371b0bae9d047d";
var HOST = "ip-10-99-191-85";
var PID = 17854;
var TRANSACTION = "request GET http://localhost:8123/";

// Run this manually as follows:
// cd minkelite
// node ./node_modules/tap/bin/tap.js test/test-api.js

tap.test('emits "ready" when ready', function(t) {
  ML.on('ready', function() {
    t.ok(true, 'sqlite3 DB is ready for writing');
    t.end();
  });
});

tap.test('postRawPieces Test', function(t) {
  ML.postRawPieces(VERSION,ACT,trace, function(err){
    t.ok( ! err,'postRawPieces writes the trace to the MinkeLite DB');
    t.end();
  });
});

tap.test('getHostPidList Test', function(t) {
  ML.getHostPidList(ACT,function(data){
    t.ok(data!=null,"getHostPidList returns some data.");
    t.equal(data.act,ACT,"getHostPidList returns the correct act.");
    t.equal(data.hosts.length,1,"getHostPidList returns one host.");
    t.equal(data.hosts[0].host,HOST,"getHostPidList returns the correct host name.");
    t.equal(data.hosts[0].pids.length,1,"getHostPidList returns one pid for the host.");
    t.equal(data.hosts[0].pids[0],PID,"getHostPidList returns the corrent pid for the host.");
    t.end();
  });
});

tap.test('getRawMemoryPieces Test', function(t) {
  ML.getRawMemoryPieces(ACT,HOST,PID,function(data){
    t.ok(data!=null,"getRawMemoryPieces returns some data.");
    t.equal(data.act,ACT,"getRawMemoryPieces returns the correct act.");
    PFKEY = data.hosts[HOST][PID][0].pfkey;
    var hashedAct = PFKEY.split('|')[0];
    t.equal(hashedAct,HASHED_ACT,"getRawMemoryPieces returns data with the correct hashed act key.");
    t.end();
  });
});

tap.test('getRawPieces Test 1', function(t) {
  ML.getRawPieces("fake_pfkey",true,function(data){
    t.ok(data==null,"getRawPieces returns null for undefined pfkey.");    
    t.end();
  });
});

tap.test('getRawPieces Test 2', function(t) {
  ML.getRawPieces(PFKEY,true,function(data){
    t.ok(data!=null,"getRawPieces returns some data.");
    var trace = null;
    try { trace = JSON.parse(data) } catch (e) { trace = null }
    t.ok(trace!=null,"getRawPieces returns a valid trace file.");
    t.equal(trace.metadata.timestamp,TIME_STAMP,"getRawPieces returns a trace object with the correct timestamp.");
    t.end();
  });
});

tap.test('getMetaTransactions Test', function(t) {
  ML.getMetaTransactions(ACT,HOST,PID,function(data,callback){
    t.ok(data!=null,"getMetaTransactions returns some data.");
    t.equal(data.act,ACT,"getMetaTransactions returns the correct act.");
    t.equal(data.hosts[HOST][PID].length,2,"getMetaTransactions returns date with the correct number of transactions.");
    t.equal(data.hosts[HOST][PID][0],TRANSACTION,"getMetaTransactions returns data with the correct first transaction.");
    callback(data);
    t.end();
  });
});

tap.test('getTransaction Test', function(t) {
  ML.getTransaction(ACT,TRANSACTION,HOST,PID,function(data){
    t.ok(data!=null,"getTransaction returns some data.");
    t.equal(data.act,ACT,"getTransaction returns the correct act.");
    t.equal(data.hosts[HOST][PID].length,1,"getTransaction returns date with the correct number of transaction data.");
    t.equal(data.hosts[HOST][PID][0].transaction,TRANSACTION,"getTransaction returns data with the correct first transaction.");
    t.equal(data.hosts[HOST][PID][0].max,10,"getTransaction returns data with the correct max value of the first transaction.");
    t.equal(data.hosts[HOST][PID][0].min,1,"getTransaction returns data with the correct min value of the first transaction.");
    t.equal(data.hosts[HOST][PID][0].n,22,"getTransaction returns data with the correct n value of the first transaction.");
    t.end();
  });
});

tap.test('_sort_db_transactions Test', function(t) {
  var input = [
    ['Redis query', 1],
    ['PostgreSQL query', 1],
    ['MySQL query', 1],
    ['MongoDB query', 1],
    ['Memcached query', 1],
    ['Redis query', 10],
    ['PostgreSQL query', 10],
    ['MySQL query', 10],
    ['MongoDB query', 10],
    ['Memcached query', 10],
    ['serve query', 1],
    ['request query', 10]
  ];
  var output = [
    ['request query', 10],
    ['serve query', 1],
    ['Memcached query', 10],
    ['Memcached query', 1],
    ['MongoDB query', 10],
    ['MongoDB query', 1],
    ['MySQL query', 10],
    ['MySQL query', 1],
    ['PostgreSQL query', 10],
    ['PostgreSQL query', 1],
    ['Redis query', 10],
    ['Redis query', 1]
  ];
  ML._sort_db_transactions(input);
  t.equal(input.length,output.length,"array size does not change after db transaciton sort");
  for(var i in input){
    t.equal(input[i][0],output[i][0],"string: db transactions are grouped and sorted in each group: item "+i.toString());
    t.equal(input[i][1],output[i][1]," value: db transactions are grouped and sorted in each group: item "+i.toString());
  }
  t.end();
});

tap.test('shutdown', function(t) {
  ML.shutdown(function(err) {
    t.ifError(err, 'should shutdown cleanly');
    t.end();
  });
});
