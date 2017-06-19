require('dotenv').load();

var Cloudant = require('cloudant');
var mysql = require("mysql");
var Processor = require('./dividend-processor');

var username =    process.env.cloudant_username;
var password =    process.env.cloudant_password;

var dbHost =    process.env.dbHost;
var dbUser =    process.env.dbUser;
var dbPass =    process.env.dbPass;
var dbDb =      process.env.dbDb;

exports.handler = function (event, context, callback) {

    console.log('config', dbHost, dbUser, dbDb);

    var cloudant = Cloudant({account:username, password:password, plugin:'promises'});

    var con = mysql.createConnection({
        host: dbHost,
        user: dbUser,
        password: dbPass,
        database: dbDb
    });

    var ticker = 'CBA';

    var query = "select ticker, LEFT(ex,10) as ex , LEFT(pay , 10) as pay, rate, frankingPercent as franking " +
        "from CompanyDetails,Dividend " +
        "where Dividend.company_fk = CompanyDetails.id and rate is not null and ticker = '" + ticker + "' order by ex asc;";


    con.query(query,function(err,result){
        if(err) throw err;
        Processor.processBulk({ticker:ticker, dataArray:result}, cloudant);
    });

    con.end(function(err) {
        if (err) {
            console.log('Connection end error', err);
        }
    });

    //

    if (callback) {
        callback(null, event);
    }
    // callback( 'some error type' );
};

exports.handler(); // just for local testing