# Loading CSV file to DynamoDB

Just a test code.

## Features

* Retry UnprocessedItems returned by DynamoDB

* Adjust emit interval time according to frequency of retrying

## Usage

* Fill out ./config/default.json.

Carefully set parameters of 'emitSize' and 'defaultEmitIntervalMillis' according to your record size and DynamoDB writing capacity.

If you have plenty capacity, interval time is shortened automatically.

```
{
  "aws": {
    "region": "ap-northeast-1",
    "dynamoDb": {
      "tableName": "tableName"
    }
  },
  "processor": {
    "emitSize": 25,     // max 25
    "queueSize": 20,
    "defaultEmitIntervalMillis": 100,       // if you don't have enough capacity, specify larger value at first.
    "defaultReadWaitIntervalMillis": 10,
    "threshold": 10,
    "diffIntervalMillis": 10,
    "delimiter": ",",
    "filterKeys": [
      "A",
      "B"
    ]
  }
}

```

* Debug run

```
$ NODE_ENV=development node index.js fileName.csv
```

* Run

```
$ node index.js fileName.csv
```
