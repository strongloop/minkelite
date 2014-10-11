var MINKELITE_PORT = 8103;
var HOST = "cxlite.concurix.com"; // "localhost"

var cx = require('concurix')({
	accountKey: "wfp:mf3d4p",
	archiveInterval: 60000,
	api_host: HOST,
	api_port: MINKELITE_PORT
});

var MinkeLite = require("../index")
var fs = require("fs");
var request = require("request");
var http = require("http");
var zlib = require('zlib');

var MINKELITE = MinkeLite();
MINKELITE.get_express_app().listen(MINKELITE_PORT, function(){
	console.log(MINKELITE);
	console.log("MinkeLite listening on",MINKELITE_PORT);
});
var LOAD_TRACE_INTERVAL_SECONDS = 1;
var GC_INTERVAL_COUNTS = 1000;

// node --nouse_idle_notification --expose_gc --expose-debug-as=v8debug test.js

// Get pfkey from the log of active server and replace pfkey in the following HTTP request.
// localhost:8103/get_raw_pieces/84004897964b8aafe155badafc510f32|fc9702eed2bb4b60309f5129d1a8e1fb/1/1/1/1/1
// localhost:8103/get_raw_memory_pieces/wfp:mf3d4p/1/0/0
// localhost:8103/get_meta_transactions/wfp:mf3d4p
// localhost:8103/get_transaction/wfp:mf3d4p/serve%20GET%20%2F/1/0/0

// cxlite.concurix.com:8103/get_meta_transactions/wfp:mf3d4p


var GC_COUNTER = 0;

function loadTrace(){
	var trace = fs.readFileSync("./cx-websever_1412467216_formatted.json");
	zlib.gzip(trace,function(err, gzipped_buf){
		MINKELITE.db.serialize(function(){
			// postGzTrace(gzipped_buf);
			_upload(gzipped_buf);
			// MINKELITE._write_raw_trace(trace, "wfp:mf3d4p");
			MINKELITE._read_all_records("raw_trace", false);
			MINKELITE._read_all_records("raw_memory_pieces", false);
			MINKELITE._read_all_records("raw_transactions", false);
			MINKELITE._read_all_records("meta_transactions", false);
		});
		// MINKELITE.shutdown();
	});
  	console.log("_____________________ GC_COUNTER :",GC_COUNTER, "___________________");
	if ( GC_COUNTER++ == GC_INTERVAL_COUNTS ){global.gc();GC_COUNTER=0};
}

function postGzTrace(buf){
	var url = 'http://'+HOST+':'+MINKELITE_PORT.toString()+'/results/1.0.1';
	var compressed = buf;
	request.post({
		url: url,
		body: compressed,
		headers: {
			"content-type": "application/json",
			"content-length": compressed.length,
			"content-encoding": "gzip",
			"Concurix-API-Key": 'MinkeLite_Test',
			"Concurix-Host": "MinkeLite_Host",
			"Concurix-Pid": process.pid
	    }
	}, function (err, res, body) {
		console.log("Status code of test.js/postGzTrace POST: %s", res.statusCode)
	 	if (err) {console.log("err:", err)};
	});
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
		"Concurix-API-Key": 'wfp:mf3d4p',
		"Concurix-Host": "cxlite:concurix.com",
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


// TRACE_LOADER = setInterval(loadTrace, LOAD_TRACE_INTERVAL_SECONDS*1000);
