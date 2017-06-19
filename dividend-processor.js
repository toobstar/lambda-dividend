require('dotenv').load();

module.exports = {
    process: function (resJson, cloudant) {
        processResult(resJson, cloudant);
    },
    processBulk: function (resJson, cloudant) {
        processBulk(resJson, cloudant);
    },
};

// resJson>> [{ticker: "CBA", dataArray: [{"ticker":"CBA","date":"2017-06-14","close":"82.30"}, {"ticker":"CBA","date":"2017-06-15","close":"82.30"}]}
function processBulk(resJson, cloudant) {
    cloudant.db.list().then(function(existingDbs) {

        var ticker = resJson.ticker;
        var dataArray = resJson.dataArray;

        //if (ticker != 'BHP' && ticker != 'CBA' && ticker != 'COH' && ticker != 'ANZ' && ticker != 'TCL') {
        //if (ticker != 'CBA') {
        //    return; // limit tickers for now
        //}

        if (!ticker) {
            console.log('missing ticker in input for ', resJson);
            return;
        }
        processEntry(cloudant, ticker, existingDbs, dataArray);

    }).catch(function(err) {
        console.log('db list error ', err);
    });
}

// resJson>> [{"ticker":"CBA","date":"2017-06-14","close":"82.30","high":"82.30","low":"81.30","split":"","volume":5736170},{"ticker":"BHP","date":"2017-06-14","close":"82.30","high":"82.30","low":"81.30","split":"","volume":5736170}]
function processResult(resJson, cloudant) {
    var count = 0;
    cloudant.db.list().then(function(existingDbs) {
        resJson.forEach(function(stockEntry) {

            var dataForSingleDate = stockEntry;
            var ticker = stockEntry.ticker;

            //if (ticker != 'BHP' && ticker != 'CBA' && ticker != 'COH' && ticker != 'ANZ' && ticker != 'TCL') {
            //if (ticker != 'TCL') {
            //    return; // limit tickers for now
            //}

            if (!ticker) {
                console.log('missing ticker in input for ', stockEntry);
                return;
            }
            count++;
            setTimeout(function(){
                processEntry(cloudant, ticker, existingDbs, [dataForSingleDate]);
            }, count * 100);

        });
    }).catch(function(err) {
        console.log('db list error ', err);
    });
}

function processEntry(cloudant, ticker, existingDbs, stockEntry) {
    var dbname = 'stock_' + ticker.toLowerCase();

    if (existingDbs.indexOf(dbname) > -1) {
        console.log('dbname-existing', dbname);
        var db = cloudant.db.use(dbname);
        processStockForDb(stockEntry, db);
    }
    else {
        console.log('dbname doesnt exist.  Should be created via price process or alternative?', dbname);
    }
}

function processStockForDb(updatedDividendArray, db) {
    console.log('processStockForDb', updatedDividendArray  );
    db.get(dataKey()).then(function(data) {
        var existingDividendArray = data.dataArray;
        if (existingDividendArray) {
            console.log("values already existing", data.dataArray.length);
            existingDividendArray = data.dataArray;
        }
        else {
            console.log("creating new data array");
        }

        var existingPriceMap = existingDividendArray.reduce(function(result, item) {
            console.log("switch data to be mapped by key", item.ex);
            result[item.ex] = item;
            return result;
        }, {});

        //[{"ticker":"TCL","ex":"2017-06-29","pay":"2017-08-11","rate":"0.27","franking":"13.21"}
        // overwrite existing from input
        updatedDividendArray.forEach(function(elem) {
            console.log("overwrite existing from input ", elem);
            if (elem.franking && typeof elem.franking != "number") {
                console.log("testing ", elem.franking, typeof elem.franking);
                rateelem.franking = parseFloat(elem.franking);
            }
            if (elem.rate && typeof elem.rate != "number") {
                elem.rate = parseFloat(elem.rate);
            }
            var existingEntry = existingPriceMap[elem.ex];
            if (!existingEntry && elem.rate) {
                existingEntry = {};
                existingPriceMap[elem.ex] = existingEntry;
                console.log("adding ", elem.ex);
            }
            if (elem.rate) {
                existingEntry.ex = elem.ex;
                existingEntry.pay = elem.pay;
                existingEntry.rate = elem.rate;
                existingEntry.franking = elem.franking;
            }
            delete existingEntry.ticker;
        });

        // turn updated existingPriceMap back into dataArray
        var updatedDataArray = [];
        for (var exDate in existingPriceMap) {
            if (existingPriceMap.hasOwnProperty(exDate)) {
                updatedDataArray.push(existingPriceMap[exDate]);
            }
        }
        updatedDataArray.sort(compareByEx);
        data.dataArray = updatedDataArray;

        insertPrice(data, db);
    }).catch(function(err) {
        var data = {};
        data.dataArray = updatedDividendArray;
        console.log("nothing existing so initialising data from input ", updatedDividendArray.length);
        insertPrice(data, db);
    });
}

function compareByEx(a,b) {
    if (a.ex < b.ex)
        return -1;
    if (a.ex > b.ex)
        return 1;
    return 0;
}

function dataKey() {
    return 'dividendData';
}
function insertPrice(stockEntry, db) {
    var pk = dataKey();
    console.log("insertPrice stockEntry", pk, stockEntry.dataArray.length);
    db.insert(stockEntry, pk).then(function(data) {
        //console.log('you have inserted stockEntry.', data);
    }).catch(function(err) {
        console.log('insert error ', err);
    });
}
