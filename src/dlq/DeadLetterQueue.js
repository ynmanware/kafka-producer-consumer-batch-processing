const async = require('async');
const {log} = require('../../util/logger');
//const kafkaProducer = require('../kafkaClient/producer');

const MAX_MESSAGE = 1; // adjust this size relative to the configuration -> fetchMaxBytes

const deadLetterQueue = async.queue(async (data, done) => {
    await produceFailedMessages(data);
    done();
}, MAX_MESSAGE); // number of parallel threads

async function produceFailedMessages(event) {
    log.info('producing messages.... $$$$$$$$$$$$$$$$$$$$$$' + JSON.stringify(event));
    //await kafkaProducer.kafkaProducer(event, Date.now());
}

module.exports = deadLetterQueue;



