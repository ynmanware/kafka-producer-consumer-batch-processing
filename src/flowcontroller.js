const defaults = require('./kafkaClient/config');
const async = require('async');

const {log, configureLogger} = require('../util/logger');

let msgQueue;

let paused = false;
let terminated = false;

const payloads = [];

const initQueue = (transform, produce, commitAndResume) => {
    msgQueue = async.queue(async (data, done) => {
        configureLogger(data);
        try {
            payloads.push(await transform(data));
        } catch (e) {
            log.error('failed to process the message', e)
        }
        done();
    }, defaults.MAX_MESSAGE);

    // start consuming messages when the queue is empty
    msgQueue.drain = async () => {
        log.log(`Paused: ${paused}, Terminated: ${terminated}`);

        //produce messages
        try {
            console.info(`producing messages.... ${payloads.length}`);
            await produce(payloads);
        } catch (e) {
            // exceptions are already handled in producer, so terminate here, raising alarm
            log.error('unable to produce message, exiting......', e);
            process.exit(0);
        }

        // commit only when it is paused and there is not error while producing the messages
        log.warn('commit and resume... ');
        commitAndResume();
        paused = false;
    };

    return msgQueue;
};

const pause = () => {
    paused = true;
};


const isPaused = () => {
    return paused;
};


module.exports = {
    pause,
    initQueue,
    isPaused,
};
