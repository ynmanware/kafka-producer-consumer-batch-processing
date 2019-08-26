const kafka = require('kafka-node');
const defaults = require('./config.js');
const kafkaTopic = process.env.CONSUMER_KAFKA_TOPIC || "get-test-result-output-topic-experiment";

const flowController = require('../flowcontroller');
const util = require('../util');

// init consumer
let consumer = new kafka.ConsumerGroup(defaults.client.options, kafkaTopic);

const init = (msgQueue) => {
    consumer.on('message', function (message) {
        msgQueue.push(message);
        console.info("------------->>>>>>>>>>>>> msgQueue.length():  " + msgQueue.length());

        if ((msgQueue.length() >= defaults.MAX_MESSAGE) && !flowController.isPaused()) {
            console.warn(`got enough messages to process, pausing... -------------------------------------------------------`);
            consumer.pause();
            flowController.pause();
        } else {
            console.debug(`messaged added to queue - partition: ${message.partition} offset: ${message.offset}`);
        }
    });

    consumer.on('error', async err => {
        console.error("Error while reading message from Kafka topic ", err);
        console.error("Error Details", JSON.stringify(err));
        if (err) {
            console.error("Received error in consumer.. attempting reconnect", err);

            let i = 0;
            let reconnectSuccessful = false;
            while (i < 100) {
                try {
                    console.info('Trying to reconnect kafka... ');
                    i++;
                    await reconnect();
                    reconnectSuccessful = true;
                    break;
                } catch (e) {
                    console.error('Failed to reconnect kafka, retrying...');
                    await util.sleep(2000);
                }
            }
            if (!reconnectSuccessful) {
                console.error('Failed to reconnect kafka, exiting...');
                process.exit(0)
            }
        }
    });
};

function reconnect() {
    return new Promise((resolve, reject) => {
        consumer.client.refreshMetadata([kafkaTopic], (err) => {
            console.info("Refreshing metadata to attempt producer reconnect");
            if (err) {
                console.error("Error occurred while refreshing metadata for producer", err);
                reject();
            } else {
                console.info("Successfully reconnected.....");
                resolve();
            }
        });
    });
}


// todo what if it produces the message but fails to commit back?
function commitAndResume() {
    consumer.commit(function (error, data) {
        console.info(`SUCCESS in committing DATA: ${JSON.stringify(data)}`);
        console.warn('The queue is empty, resuming! ===========================================');
        if (!error) {
            console.info('start consuming data again...');
            consumer.resume();
        } else {
            // todo - keep retrying
            console.error(`Failed to commit messages!`, error);
            util.sleep(2000);
        }
    });
}


module.exports = {
    init,
    commitAndResume
};
