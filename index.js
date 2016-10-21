const fs = require('fs');
const csv = require('csv-parser');
const async = require('async');
const Buffer = require('./Buffer');
const DynamoDbEmittwr = require('./DynamoDbEmitter');

const size = 20;
const waitTime = 10000;
const queueSize = 20;
let emitInterval = 1000;
const countThreshold = 20;
const diffInterval = 10;
const fileName = './file.csv';
const buffer = new Buffer(size);
const emitter = new DynamoDbEmittwr('table_name');
const delimiter = ',';

const commitQueue = async.queue(commit, 1);
const processQueue = async.queue(process, 1);
const readStream = fs.createReadStream(fileName);
let unprocessedCount = 0;
let processedCount = 0;

const filterKeys = [];

function commit(task, callback) {
  const records = task.data;
  emitter.emit(records)
    .then((res) => {
      console.log(`${records.length} records put to DynamoDB successfully: ${res}`);
      const unprocessed = JSON.parse(res).UnprocessedItems;
      if (Object.keys(unprocessed).length !== 0) {
        const remainItems = unprocessed['table_name'];
        console.log(`push unprocessed ${remainItems.length} items`);
        commitQueue.push({ data: remainItems }, (err) => {
          console.log('commited');
        });

        unprocessedCount += 1;
        processedCount = 0;
      } else {
        processedCount += 1;
        unprocessedCount = 0;
      }

      if (unprocessedCount >= countThreshold) {
        emitInterval += diffInterval;
        unprocessedCount = 0;
        processedCount = 0;
      }
      if (processedCount >= countThreshold) {
        emitInterval -= diffInterval;
        unprocessedCount = 0;
        processedCount = 0;
      }

      setTimeout(() => {
        callback(null);
      }, emitInterval);
    })
    .catch((err) => {
      console.log(`error occurred during emitting: ${err}`);
      callback(err);
    });
}

function controllFlow(readstream) {
  if (readstream.isPaused()) {
    if (commitQueue.length() < queueSize) {
      console.log('restart!');
      readstream.resume();
    } else {
      setTimeout(controllFlow, waitTime);
    }
  } else {
    if (commitQueue.length() > queueSize) {
      console.log('waiting...');
      readstream.pause();
      setTimeout(controllFlow, waitTime);
    }
  }
}

function process(task, callback) {
  controllFlow(readStream);
  if (task.end) {
    const records = buffer.flushAndClear();
    commitQueue.push({ data: records }, (err) => {
      console.log('commited')
    });
    callback(null);
    return;
  }
  const row = task.row;
  // filter
  const filteredRow = {};
  if (filterKeys.length > 0) {
    Object.keys(row).filter((key) => {
      return filterKeys.includes(key);
    }).forEach((key) => {
      filteredRow[key] = row[key];
    });
  }
  // empty string is replaced by null
  const record = {};
  Object.keys(filteredRow).map((key) => {
    if (row[key] === '') {
      record[key] = null;
    } else {
      record[key] = row[key];
    }
  });
  buffer.putRecord(record);

  if (buffer.shouldFlush()) {
    const records = buffer.flushAndClear();
    commitQueue.push({ data: records }, (err) => {
      console.log('commited')
    });
  }
  callback(null);
}

function processCsv(rs) {
  const parser = csv({
    raw: false,
    separator: delimiter,
    strict: true,
  });
  rs.pipe(parser)
    .on('data', (data) => {
      if (data) {
        processQueue.push({ row: data }, (err) => {
          if (err) {
            console.log('error occurred pushing process');
          }
        });
      }
    })
    .on('headers', (headerList) => {
      console.log(`headers detected: ${headerList}`);
    })
    .on('error', (err) => {
      console.log(`error occurs during parsing csv: ${err}`);
    })
    .on('end', () => {
      processQueue.push({ end: 'end' }, (err) => {
        if (err) {
          console.log('error occurred pushing last process');
        }
      });
      console.log('csv file end');
    });
}

processCsv(readStream);
