var mongodb = require('mongodb');
var csv = require('csv-parser');
var fs = require('fs');

var MongoClient = mongodb.MongoClient;
var mongoUrl = 'mongodb://localhost:27017/911-calls';

var insertCalls = function(db, callback) {
    var collection = db.collection('calls');
    collection.drop();
    collection.createIndex({location: '2dsphere'});
    collection.createIndex({category: "text", township: "text" });

    var calls = [];
    fs.createReadStream('../911.csv')
        .pipe(csv())
        .on('data', data => {


          const [address, title, station, schedule] = data.desc.split(';');
          const [category, subject] = data.title.split(':');

          const toPush = {
            location: {type: "Point", coordinates: [parseFloat(data.lng), parseFloat(data.lat)]},
            station: station.trim(),
            category: category.trim(),
            subject: subject.trim(),
            zipCode: data.zip,
            date: new Date(data.timeStamp),
            township: data.twp,
            address: data.addr
          }
            calls.push(toPush);
        })
        .on('end', () => {
          collection.insertMany(calls, (err, result) => {
            callback(result)
          });
        });
}

MongoClient.connect(mongoUrl, (err, db) => {

    insertCalls(db, result => {
        console.log(`${result.insertedCount} calls inserted`);
        db.close();
    });
});
