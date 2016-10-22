const aws = require('aws-sdk');
const consoleWriter = require('./consoleWriter');

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

  static wrapItemsDynamoRequest(items) {
    return items.map(item => (
      {
        PutRequest: {
          Item: item,
        },
      })
    );
  }

  emit(items) {
    const params = {
      RequestItems: {
        [this.tableName]: items,
      },
    };

    if (process.env.NODE_ENV === 'development') {
      consoleWriter.log(`debug request: ${JSON.stringify(params)}`);
      return Promise.resolve(JSON.stringify({ UnprocessedItems: {} }));
    }

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
