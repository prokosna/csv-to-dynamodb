const readline = require('readline');

class ConsoleWriter {
  constructor() {
    /** @private */
    this.startTime = new Date().getTime();
    /** @private */
    this.regularLines = 11;
    /** @private */
    this.logQueue = [];
    /** @private */
    this.totalProcessedCount = 0;
    /** @private */
    this.totalFailedCount = 0;
    /** @private */
    this.tempProcessedCount = 0;
    /** @private */
    this.tempRetriedCount = 0;
    /** @private */
    this.headers = '';
    /** @private */
    this.isPaused = false;
    /** @private */
    this.emitInterval = 0;
    /** @private */
    this.running = 0;
  }

  countTotalProcessedCount(n) {
    this.totalProcessedCount += n;
  }

  countTotalFailedCount(n) {
    this.totalFailedCount += n;
  }

  countTempProcessedCount(n) {
    this.tempProcessedCount += n;
  }

  countTempRetriedCount(n) {
    this.tempRetriedCount += n;
  }

  setHeaders(headerList) {
    this.headers = headerList;
  }

  setIsPaused(isPause) {
    this.isPaused = isPause;
  }

  setEmitInterval(ms) {
    this.emitInterval = ms;
  }

  /** @private */
  clearAndWrite(text) {
    readline.clearLine(process.stdout);
    process.stdout.write(text);
  }

  /** @private */
  writePeriodically() {
    readline.moveCursor(process.stdout, 0, -this.regularLines);
    readline.cursorTo(process.stdout, 0);

    // write irregular logs
    while (this.logQueue.length > 0) {
      this.clearAndWrite(this.logQueue.shift());
    }

    // write regular logs
    const now = new Date().getTime();
    const elapsedSec = (now - this.startTime) / 1000.0;
    const averageRate = this.totalProcessedCount / elapsedSec;
    this.clearAndWrite('########## Loading to DynamoDB ##########\n');
    this.clearAndWrite(`CSV Headers: ${this.headers}\n`);
    this.clearAndWrite(`Elasped Time: ${Math.floor(elapsedSec)} sec\n`);
    this.clearAndWrite(`Emit Interval Time: ${this.emitInterval} ms\n`);
    this.clearAndWrite(`File Stream is paused?: ${this.isPaused}\n`);
    this.clearAndWrite('-----------------------------------------\n');
    this.clearAndWrite(`Total Processed Count: ${this.totalProcessedCount}\n`);
    this.clearAndWrite(`Total Failed Count: ${this.totalFailedCount}\n`);
    this.clearAndWrite(`Average Processed Rate: ${averageRate} /sec\n`);
    this.clearAndWrite(`Processed Rate: ${this.tempProcessedCount} /sec\n`);
    this.clearAndWrite(`Retried Rate: ${this.tempRetriedCount} /sec\n`);
    this.tempProcessedCount = 0;
    this.tempRetriedCount = 0;
  }

  log(text) {
    this.logQueue.push(`${text}\n`);
  }

  run() {
    process.stdout.write(Array(this.regularLines + 1).join('\n'));
    this.writePeriodically();
    this.running = setInterval(this.writePeriodically.bind(this), 1000);
  }

  stop() {
    this.writePeriodically();
    clearInterval(this.running);
  }
}

const consoleWriter = new ConsoleWriter();
module.exports = consoleWriter;
