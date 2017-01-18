var elasticsearch = require('elasticsearch');
var csv = require('csv-parser');
var fs = require('fs');

var esClient = new elasticsearch.Client({
  host: 'localhost:9200',
  log: 'error'
});

var bulkRequest = [];


function createMapping() {

  var stringType = {
    type: "text",
    fields: {
      keyword: {
        type: "keyword",
        ignore_above: 256
      }
    }
  }

  return esClient.indices.putMapping({
    index: 'calls',
    type: 'call',
    body: {
      properties: {
      location: {
        type: "geo_point"
      },
      station: stringType,
      category: stringType,
      subject: stringType,
      zipCode: stringType,
      date: {
        type: "date"
      },
      township: stringType,
      address: stringType
    }
  }
});
}

function createIndexAndFillData() {
  esClient.indices.create({index: 'calls'})
  .then(resp => createMapping()).then(function() {

  console.log('INSERTING DATA...')

  fs.createReadStream('../911.csv')
      .pipe(csv())
      .on('data', data => {

        const [address, title, station, schedule] = data.desc.split(';');
        const [category, subject] = data.title.split(':');

        const toPush = {
          location: [parseFloat(data.lng), parseFloat(data.lat)],
          station: station.trim(),
          category: category.trim(),
          subject: subject.trim(),
          zipCode: data.zip,
          date: new Date(data.timeStamp),
          township: data.twp,
          address: data.addr
        }

        bulkRequest.push({index: {_index: 'calls', '_type': 'call'}})
        bulkRequest.push(toPush);
      })
      .on('end', () => {
        esClient.bulk({body: bulkRequest}, function(err, resp) {
            console.log(resp);
        });
      });

});
}


esClient.indices.delete({index: 'calls'})
  .catch(resp => createIndexAndFillData())
  .then(resp => createIndexAndFillData());
