class Buffer {
  constructor(size) {
    /** @private */
    this.buffer = [];
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
      })
    );
    this.clear();
    return records;
  }
}

module.exports = Buffer;
