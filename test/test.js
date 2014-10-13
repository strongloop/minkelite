var MINKELITE_PORT = 8103
var HOST = "cxlite.concurix.com" // "localhost"
var ACT_KEY = "wfp:mf3d4p"

var cx = require('concurix')({
	accountKey: ACT_KEY,
	archiveInterval: 60000,
	api_host: HOST,
	api_port: MINKELITE_PORT
})

var MinkeLite = require("../index")
var fs = require("fs")
var http = require("http")
var zlib = require('zlib')

var DEBUG = false

var MINKELITE = MinkeLite({"verbose":true,"host_pid_aware":true,"server_port":MINKELITE_PORT})
var LOAD_TRACE_INTERVAL_SECONDS = 1

// localhost:8103/get_meta_transactions/wfp:mf3d4p
// localhost:8103/get_transaction/wfp:mf3d4p/serve%20POST%20%2Fresults%2F1.0.1/0/0
// localhost:8103/get_raw_pieces/d89f29a0bb4094ff1d78d9df89cfc132|9beefcf937ba1c320af0aba9f11eacc3
// localhost:8103/get_raw_memory_pieces/wfp:mf3d4p/0/0

// localhost:8103/get_transaction/wfp:mf3d4p/serve%20POST%20%2Fresults%2F1.0.1/setoair.home/12975
// localhost:8103/get_raw_memory_pieces/wfp:mf3d4p/setoair.home/12975
// localhost:8103/get_meta_transactions/wfp:mf3d4p/setoair.home/12975

// Get pfkey from the log of active server and replace pfkey in the following HTTP request.
// localhost:8103/get_meta_transactions/wfp:mf3d4p
// localhost:8103/get_transaction/wfp:mf3d4p/serve%20POST%20%2Fresults%2F1.0.1/0/0
// localhost:8103/get_raw_memory_pieces/wfp:mf3d4p/0/0
// localhost:8103/get_raw_pieces/d89f29a0bb4094ff1d78d9df89cfc132|36fc135614aa2c40ae73ad59a3f192df

// localhost:8103/get_meta_transactions/wfp:mf3d4p/SetoAir.local/12975

// cxlite.concurix.com:8103/get_meta_transactions/wfp:mf3d4p


function loadTrace(){
	var trace = fs.readFileSync("./cx-websever_1412467216_formatted.json");
	zlib.gzip(trace,function(err, gzipped_buf){
		MINKELITE.db.serialize(function(){
			// postGzTrace(gzipped_buf)
			_upload(gzipped_buf)
			// MINKELITE._write_raw_trace(trace, "wfp:mf3d4p")
			MINKELITE._read_all_records("raw_trace", false)
			MINKELITE._read_all_records("raw_memory_pieces", false)
			MINKELITE._read_all_records("raw_transactions", false)
			MINKELITE._read_all_records("meta_transactions", false)
			MINKELITE._read_all_records("stats_mean_sd", false)
		})
		// MINKELITE.shutdown()
	})
}

function postGzTrace(buf){
	var url = 'http://'+HOST+':'+MINKELITE_PORT.toString()+'/results/1.0.1'
	var compressed = buf
	request.post({
		url: url,
		body: compressed,
		headers: {
			"content-type": "application/json",
			"content-length": compressed.length,
			"content-encoding": "gzip",
			"Concurix-API-Key": ACT_KEY,
			"Concurix-Host": HOST,
			"Concurix-Pid": process.pid
	    }
	}, function (err, res, body) {
		console.log("Status code of test.js/postGzTrace POST: %s", res.statusCode)
	 	if (err) {console.log("err:", err)}
	})
}

function _upload(compressed) {
  var options = {
    agent: false,
    host: HOST,
    port: MINKELITE_PORT,
    path: "/results/1.0.1",
    method: "POST",
    headers: {
		"content-type": "application/json",
		"content-length": compressed.length,
		"content-encoding": "gzip",
		"Concurix-API-Key": ACT_KEY,
		"Concurix-Host": HOST,
		"Concurix-Pid": process.pid
    }
  }
  var req = http.request(options, function (res) {
	console.log("Status code of test.js/_upload POST: %s", res.statusCode)
    if (res.statusCode != 202) {
      console.log("Failed to upload: %s", res.statusCode)
    }
  })
  req.end(compressed)
  req.on("error", function (err) {
    console.log("Error attempting to upload trace file: %s", err)
  })
}


// TRACE_LOADER = setInterval(loadTrace, LOAD_TRACE_INTERVAL_SECONDS*1000)
