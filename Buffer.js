class Buffer {
  constructor(size) {
    /** @private */
    this.buffer = [];
    /** @private */
    this.totalCount = 0;
    /** @private */
    this.maxSize = size;
  }

  /** @private */
  clear() {
    this.buffer.length = 0;
  }

  putRecord(data) {
    if (!data) {
      return;
    }

    this.totalCount += 1;
    this.buffer.push(data);
  }

  shouldFlush() {
    return this.buffer.length >= this.maxSize;
  }

  flushAndClear() {
    const records = this.buffer.map(record => (
    {
      PutRequest: {
        Item: record,
      },
    }
    ));
    this.clear();
    console.log(`total flushed records count: ${this.totalCount}`);
    return records;
  }
}

module.exports = Buffer;
