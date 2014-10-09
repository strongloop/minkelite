var MinkeLite = require("../index")
var fs = require("fs");
var request = require("request");
var zlib = require('zlib');

var MINKELITE = MinkeLite();
MINKELITE.get_express_app().listen(8103);
var LOAD_TRACE_INTERVAL_SECONDS = 1;
var GC_INTERVAL_COUNTS = 1000;

// node --nouse_idle_notification --expose_gc --expose-debug-as=v8debug test.js

// Get pfkey from the log of active server and replace pfkey in the following HTTP request.
// localhost:8103/get_raw_pieces/84004897964b8aafe155badafc510f32|4544711c14a78d761642b3ab7755dc17/1/1/1/1/1
// localhost:8103/get_raw_memory_pieces/MinkeLite_Test/1/0/0
// localhost:8103/get_meta_transactions/MinkeLite_Test
// localhost:8103/get_transaction/MinkeLite_Test/serve%20GET%20%2F/1/0/0

var GC_COUNTER = 0;

function loadTrace(){
	var trace = fs.readFileSync("./cx-websever_1412467216_formatted.json");
	zlib.gzip(trace,function(err, gzipped_buf){
		MINKELITE.db.serialize(function(){
			postGzTrace(gzipped_buf);
			// MINKELITE._write_raw_trace(gzipped_buf, "MinkeLite_Test");
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
	var url = 'http://localhost:8103/post_raw_pieces';
	var req = request.post(url, function (err, resp, body) {
	  if (err) {console.log("err:", err)}});
	var form = req.form();
	form.append('file',  buf.toString('base64'));
	form.append('act', 'MinkeLite_Test');
	// console.log("postGzTrace :", buf.constructor.name, buf.toString().length, buf.toString('hex').substring(0,200),'...');
}

TRACE_LOADER = setInterval(loadTrace, LOAD_TRACE_INTERVAL_SECONDS*1000);
