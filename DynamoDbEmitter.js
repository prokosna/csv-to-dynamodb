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

  emit(records) {
    const items = records.map(record => (
      {
        PutRequest: {
          Item: record,
        },
      })
    );
    const params = {
      RequestItems: {
        [this.tableName]: items,
      },
    };

    return new Promise((resolve, reject) => {
      this.dynamo.batchWrite(params, (err, res) => {
        if (err) {
          reject(err);
        }
        resolve(res);
      });
    });
  }
}

module.exports = DynamoDbEmitter;