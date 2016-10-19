const fs = require('fs');
const csv = require('csv-parser');
const async = require('async');
const Buffer = require('./Buffer');
const DynamoDbEmittwr = require('./DynamoDbEmitter');

const size = 2;
const fileName = './hoge.csv';
const buffer = new Buffer(size);
const emitter = new DynamoDbEmittwr('atlas-member-attributes');
const delimiter = ',';

function commit(task) {
  const records = task.data;
  emitter.emit(records)
    .then((res) => {
      console.log(`${records.length} records put to DynamoDB successfully: ${res}`);
    })
    .catch((err) => {
      console.log(`error occurred during emitting: ${err}`);
    });
}

const queue = async.queue(commit, 1);

function processRow(row) {
  buffer.putRecord(row);

  if (buffer.shouldFlush()) {
    const records = buffer.flushAndClear();
    queue.push({ data: records });
  }
}

function processCsv(csvFileName) {
  const parser = csv({
    raw: false,
    separator: delimiter,
    strict: true,
  });
  fs.createReadStream(csvFileName)
    .pipe(parser)
    .on('data', (data) => {
      console.log(data);
      if (data) {
        processRow(data);
      }
    })
    .on('headers', (headerList) => {
      console.log(`headers detected: ${headerList}`);
    })
    .on('error', (err) => {
      console.log(`error occurs during parsing csv: ${err}`);
    });
}

processCsv(fileName);
