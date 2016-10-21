const aws = require('aws-sdk');

class DynamoDbEmitter {
  constructor(tableName) {
    /** @private */
    this.dynamo = new aws.DynamoDB.DocumentClient({
      apiVersion: '2012-08-10',
      region: 'ap-northeast-1',
    });
    /** @private */
    this.tableName = tableName;
  }

  createTable() {
    // TODO implement
  }

  emit(items) {
    const params = {
      RequestItems: {
        [this.tableName]: items,
      },
    };

    return new Promise((resolve, reject) => {
      this.dynamo.batchWrite(params, (err, res) => {
        if (err) {
          reject(JSON.stringify(err));
        }
        resolve(JSON.stringify(res));
      });
    });
  }
}

module.exports = DynamoDbEmitter;
