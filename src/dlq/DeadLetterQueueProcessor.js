const {log} = require('../../util/logger');
const deadLetterQueue = require('./DeadLetterQueue');

const {kafkaConnection} = require('../kafkaClient/producer');

let producer = kafkaConnection();

const RETRY_AFTER = 2000;

const kafkaTopic = process.env.PRODUCER_KAFKA_TOPIC || "get-test-transformer-output-topic-experiment";

const getDeadLetterQueue = function () {
    return deadLetterQueue;
};

const handleFailedMessages = async () => {
    // try connecting to kafka
    if (deadLetterQueue.length() == 0) {
        log.info('no messages in dead letter queue');
        return;
    }

    while (true) {
        try {
            log.info('Trying to reconnect kafka... ');
            await reconnect();
            deadLetterQueue.resume();
            break;
        } catch (e) {
            log.error('Failed to reconnect kafka, retrying...');
            await sleep(RETRY_AFTER);
        }
    }
};

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function reconnect() {
    return new Promise((resolve, reject) => {
        producer.client.refreshMetadata([kafkaTopic], (err) => {
            log.info("Refreshing metadata to attempt producer reconnect");
            if (err) {
                log.error("Error occurred while refreshing metadata for producer", err);
                reject();
            } else {
                log.info("Successfully reconnected.....");
                resolve();
            }
        });
    });
}


module.exports = {handleFailedMessages, getDeadLetterQueue} ;



