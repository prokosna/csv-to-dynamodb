const fs = require('fs');
const CsvProcessor = require('./CsvProcessor');

fs.stat(process.argv[2] ? process.argv[2] : '', (err) => {
  if (err) {
    console.log('Please specify csv file.');
  } else {
    const readStream = fs.createReadStream(process.argv[2]);
    const csvProcessor = new CsvProcessor(readStream);
    csvProcessor.run();
  }
});
