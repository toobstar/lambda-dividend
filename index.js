require('dotenv').load();

var Cloudant = require('cloudant');
var PriceService = require('price-service');
var Processor = require('./dividend-processor');

var username =    process.env.cloudant_username;
var password =    process.env.cloudant_password;
var priceUrl =    process.env.priceUrl;

exports.handler = function (event, context, callback) {

    console.log('config', username, password, priceUrl);

    var cloudant = Cloudant({account:username, password:password, plugin:'promises'});
    var dataService = PriceService();
    dataService.fetch(priceUrl, function(result) {
        Processor.process(result, cloudant);
    })

    if (callback) {
        callback(null, event);
    }
};

//exports.handler(); // just for local testing