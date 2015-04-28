/*
 *  JCOUCH MODULE
 */


// DEPENDENCIES
var couchbase = require("couchbase");
var async = require("async");
var util = require("util");


// CONSTANTS
var BUCKET_TIMEOUT = 10 * 1000;//10 * 60 * 1000;
var BUCKET_INTERVAL = 10 * 1000;
var DEFAULT_HOST = "52.4.120.251";
var DEFAULT_PORT = 8091;


// MEMBERS
var host;
var port;
var connection;
var buckets = {};
var bucketIntervalId;


// METHODS

// Connection
function getConnection() {
	if( typeof(connection) === "undefined" ) {
		setConnection();
	}
	return connection;
}

function setConnection( _host, _port ) {

	// Set mongo options
	host = (typeof(_host) !== "string") ? DEFAULT_HOST : _host;
	port = (typeof(_port) !== "number") ? DEFAULT_PORT : _port;

	// Set connection to database
	connection = new couchbase.Cluster(host+":"+port);
}

function getBucket( bucketName, callback ) {

	// Do we need to open a new bucket?
	var newBucket;
	if( typeof(newBucket = buckets[bucketName]) === "undefined" ) {
		newBucket = getConnection().openBucket(bucketName, function(err) {
			if(err) {
				callback(err);
			} else {		
				// Set timeout and add bucket
				newBucket.timeout = new Date().getTime();
				buckets[bucketName] = newBucket;

				// Start the bucket interval if haven't already
				if( typeof(bucketIntervalId) === "undefined" ) {
					bucketIntervalId = setInterval(bucketIntervalFunc, BUCKET_INTERVAL)
				}

				callback(undefined, newBucket);
			}
		});
		
	} else {
		callback(undefined, newBucket);
	}
}

function bucketIntervalFunc() {
	for(var key in buckets) {
		if(buckets.hasOwnProperty(key)) {
			var bucket = buckets[key];
			
			// Check if the bucket has been inactive
			if((new Date().getTime() - bucket.timeout) > BUCKET_TIMEOUT) {
				bucket.disconnect();
				delete buckets[key];
				
				// Clear the interval function if there are no more buckets
				if(Object.getOwnPropertyNames(buckets).length === 0) {
					clearInterval(bucketIntervalId);
					bucketIntervalId = undefined;
				}
			}
		}
	}
}


// Helpers
function couchResultToJSON( couchResult ) {
	if(typeof(couchResult.value) !== "undefined") {
		return couchResult.value;
	} else if( typeof(couchResult) === "object") {
		var newObject = {};
		for(var key in couchResult) {
			if(couchResult.hasOwnProperty(key)) {
				newObject[key] = couchResultToJSON(couchResult[key])
			}
		}
		return newObject;

	} else if( Array.isArray(couchResult) ) {
		var newArray = [];
		for(var result in couchResult) {
			newArray.push( couchResultToJSON(couchResult[result]) );
		}
		return newArray;
	}
}

// Crud
function insert( bucketName, docName, doc, finalCallback ) {
	async.waterfall([
		function(callback) {
			getBucket(bucketName, callback);
		},
		function(bucket, callback) {
			bucket.insert(docName, doc, callback);
		}	
	], function(err, results) {
		finalCallback(err, results);
	});
}

function get( bucketName, docName, finalCallback ) {
	async.waterfall([
		function(callback) {
			getBucket(bucketName, callback);
		},
		function(bucket, callback) {
			bucket.get(docName, callback);
		}	
	], function(err, results) {
		finalCallback(err, couchResultToJSON(results));
	});
}

function getMulti( bucketName, docNames, finalCallback ) {
	async.waterfall([
		function(callback) {
			getBucket(bucketName, callback);
		},
		function(bucket, callback) {
			bucket.getMulti(docNames, callback);
		}	
	], function(err, results) {
		finalCallback(err, couchResultToJSON(results));
	});
}

function remove( docName, finalCallback ) {
	async.waterfall([
		function(callback) {
			getBucket(bucketName, callback);
		},
		function(bucket, callback) {
			bucket.remove(docName, callback);
		}	
	], function(err, results) {
		finalCallback(err, results);
	});
}

function upsert( bucketName, docName, doc, finalCallback ) {
	async.waterfall([
		function(callback) {
			getBucket(bucketName, callback);
		},
		function(bucket, callback) {
			bucket.upsert(docName, doc, callback);
		}	
	], function(err, results) {
		finalCallback(err, results);
	});
}

function replace( bucketName, docName, doc, finalCallback ) {
	async.waterfall([
		function(callback) {
			getBucket(bucketName, callback);
		},
		function(bucket, callback) {
			bucket.replace(docName, doc, callback);
		}	
	], function(err, results) {
		finalCallback(err, results);
	});
}


// MAIN EXPORTS
module.exports = {
	setConnection : setConnection,
	insert : insert,
	get : get,
	getMulti : getMulti,
	remove : remove,
	upsert : upsert,
	replace : replace
}



