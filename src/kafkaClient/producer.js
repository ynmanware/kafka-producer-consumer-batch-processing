const kafka = require('kafka-node');
const defaults = require('./config.js');
const kafkaTopic = process.env.PRODUCER_KAFKA_TOPIC || "get-test-transformer-output-topic-experiment";
const {log, configureLogger} = require('../../util/logger');
log.addContext('moduleName', 'producer');
const promMetrics = require('../../util/promConfig');

const util = require('../util');

let producer = kafkaConnection();

module.exports = {
    produce: async function (events) {
        const payloads = [];
        events.forEach(function (event) {
            payloads.push({"topic": kafkaTopic, "messages": JSON.stringify(event)});
        });
        try {
            await sendMessage(payloads);
        } catch (e) {
            let i = 0;
            let reconnectSuccessful = false;
            while (i < 100) {
                try {
                    log.info('Trying to reconnect kafka... ');
                    i++;
                    await reconnect();
                    reconnectSuccessful = true;
                    break;
                } catch (e) {
                    log.error('Failed to reconnect kafka, retrying...');
                    await util.sleep(2000);
                }
            }
            if (!reconnectSuccessful) {
                log.error('Failed to reconnect kafka, exiting...');
                throw new Error('Failed to reconnect kafka')
            } else {
                log.info('reconnected to kafka, exiting...');
                try {
                    await sendMessage(payloads);
                } catch (e) {
                    log.error('Failed to resend to kafka, exiting...');
                    throw new Error('Failed to resend to kafka...')
                }
            }
        }
    },
};

async function sendMessage(payloads) {
    const promise = new Promise(function (resolve, reject) {
        producer.send(payloads, async (err, data) => {
            if (err) {
                log.error('Error while pushing message to kafka topic.', err);
                promMetrics.kafkaProducerError.inc();
                reject();
            }
            if (data) {
                console.info("Produced Message on to es-proxy topic.", JSON.stringify(data));
                resolve(data);
            }
        });
    });
    return promise;
}


function kafkaConnection() {
    const client = new kafka.KafkaClient({
        kafkaHost: defaults.client.options.kafkaHost,
        requestTimeout: defaults.client.options.requestTimeout,
        requireAcks: 1,
        ackTimeoutMs: 100,
        connectRetryOptions: {
            retries: 100
        },
    });

    const producer = new kafka.HighLevelProducer(client);

    producer.on('ready', function () {
        log.info("Kafka producer is connected");
    });

    producer.on('error', async function (err) {
            log.error('Kafka producer is not able to connect', err);
            let i = 0;
            let reconnectSuccessful = false;
            while (i < 100) {
                try {
                    log.info('Trying to reconnect kafka... ');
                    i++;
                    await reconnect();
                    reconnectSuccessful = true;
                    break;
                } catch (e) {
                    log.error('Failed to reconnect kafka, retrying...');
                    await util.sleep(2000);
                }
            }
            if (!reconnectSuccessful) {
                log.error('Failed to reconnect kafka, exiting...');
                process.exit(0);
            } else {
                log.info('reconnected to kafka, exiting...');
            }
        }
    );
    return producer;
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




