const csv = require('csv-parser');
const async = require('async');
const config = require('config');
const Buffer = require('./Buffer');
const DynamoDbEmitter = require('./DynamoDbEmitter');
const consoleWriter = require('./consoleWriter');

const {
  emitSize,
  queueSize,
  defaultEmitIntervalMillis,
  defaultReadWaitIntervalMillis,
  threshold,
  diffIntervalMillis,
  delimiter,
  filterKeys,
} = config.processor;

const {
  tableName,
} = config.aws.dynamoDb;

class CsvProcessor {
  constructor(readStream) {
    /** @private */
    this.readStream = readStream;
    /** @private */
    this.buffer = new Buffer(emitSize);
    /** @private */
    this.emitter = new DynamoDbEmitter(tableName);
    /** @private */
    this.commitQueue = async.queue(this.commit.bind(this), 1);
    /** @private */
    this.processQueue = async.queue(this.process.bind(this), 1);
    /** @private */
    this.succeededEmitCount = 0;
    /** @private */
    this.emitInterval = defaultEmitIntervalMillis;
    /** @private */
    this.readWaitInterval = defaultReadWaitIntervalMillis;
    /** @private */
    this.isPaused = false;
  }

  /** @private */
  commit(task, callback) {
    const items = task.items;
    if (items.length <= 0) {
      callback(null);
      return;
    }
    this.emitter.emit(items)
      .then((res) => {
        const unprocessed = JSON.parse(res).UnprocessedItems;
        if (Object.keys(unprocessed).length !== 0) {
          const unprocessedItems = unprocessed[tableName];
          this.commitQueue.push({ items: unprocessedItems }, (err) => {
            if (err) {
              consoleWriter.log(`error occurred at committing: ${err}`);
            }
          });

          const processedCount = items.length - unprocessedItems.length;
          consoleWriter.countTotalProcessedCount(processedCount);
          consoleWriter.countTempProcessedCount(processedCount);
          consoleWriter.countTempRetriedCount(unprocessedItems.length);
          this.succeededEmitCount -= 1;
        } else {
          consoleWriter.countTotalProcessedCount(items.length);
          consoleWriter.countTempProcessedCount(items.length);
          this.succeededEmitCount += 1;
        }

        if (this.succeededEmitCount >= threshold) {
          this.emitInterval += diffIntervalMillis;
          this.succeededEmitCount = 0;
          if (this.readWaitInterval < 10) {
            this.readWaitInterval = defaultReadWaitIntervalMillis;
          }
        } else if (this.succeededEmitCount <= -threshold) {
          this.emitInterval -= diffIntervalMillis;
          this.succeededEmitCount = 0;
          if (this.emitInterval <= 0) {
            this.emitInterval = 0;
            this.readWaitInterval = 1;
          }
        }
        consoleWriter.setEmitInterval(this.emitInterval);

        setTimeout(() => {
          callback(null);
        }, this.emitInterval);
      })
      .catch((err) => {
        consoleWriter.countTotalFailedCount(items.length);
        callback(err);
      });
  }

  /** @private */
  process(task, callback) {
    this.controllFlow();
    // if end key exists, this is the last of file.
    if (task.end) {
      const items = DynamoDbEmitter.wrapItemsDynamoRequest(this.buffer.flushAndClear());
      this.commitQueue.push({ items }, (err) => {
        if (err) {
          consoleWriter.log(`error occurred at committing: ${err}`);
        }
        consoleWriter.log('processing finished');
        consoleWriter.stop();
      });
      callback(null);
      return;
    }
    const row = task.data;
    // filter row
    const filteredRow = {};
    if (filterKeys.length > 0) {
      Object.keys(row).filter(key => filterKeys.includes(key)
      ).forEach((key) => {
        filteredRow[key] = row[key];
      });
    } else {
      Object.keys(row).forEach((key) => {
        filteredRow[key] = row[key];
      });
    }
    // empty string should be replaced by null
    Object.keys(filteredRow).forEach((key) => {
      filteredRow[key] = filteredRow[key] === '' ? null : filteredRow[key];
    });
    this.buffer.putRecord(filteredRow);

    if (this.buffer.shouldFlush()) {
      const items = DynamoDbEmitter.wrapItemsDynamoRequest(this.buffer.flushAndClear());
      this.commitQueue.push({ items }, (err) => {
        if (err) {
          consoleWriter.log(`error occurred at committing: ${err}`);
        }
      });
    }
    callback(null);
  }


  /** @private */
  controllFlow() {
    if (this.readStream.isPaused()) {
      if (this.commitQueue.length() < queueSize) {
        // if commitQueue become empty during pausing, read wait interval should be shorter.
        if (this.commitQueue.length() <= 0 && this.readWaitInterval > 1) {
          this.readWaitInterval -= 1;
        }
        this.isPaused = false;
        consoleWriter.setIsPaused(this.isPaused);
        this.readStream.resume();
      } else {
        setTimeout(this.controllFlow.bind(this), this.readWaitInterval);
      }
    } else if (this.commitQueue.length() > queueSize) {
      this.isPaused = true;
      consoleWriter.setIsPaused(this.isPaused);
      this.readStream.pause();
      setTimeout(this.controllFlow.bind(this), this.readWaitInterval);
    }
  }

  run() {
    consoleWriter.run();
    const parser = csv({
      raw: false,
      separator: delimiter,
      strict: true,
    });
    this.readStream.pipe(parser)
      .on('data', (data) => {
        if (data) {
          this.processQueue.push({ data }, (err) => {
            if (err) {
              consoleWriter.log(`error occurred at processing: ${err}`);
            }
          });
        }
      })
      .on('headers', (headerList) => {
        consoleWriter.setHeaders(headerList);
      })
      .on('error', (err) => {
        consoleWriter.log(`error occurred at parsing csv: ${err}`);
      })
      .on('end', () => {
        this.processQueue.push({ end: 'end' }, (err) => {
          if (err) {
            consoleWriter.log(`error occurred at processing: ${err}`);
          }
        });
      });
  }
}

module.exports = CsvProcessor;
