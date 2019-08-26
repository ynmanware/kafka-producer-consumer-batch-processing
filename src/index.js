const transformer = require('./transformer');
const express = require('express');

const app = express();
const port = process.env.PORT || 8080;

const consumer = require('./kafkaClient/consumer');
const producer = require('./kafkaClient/producer');
const flowController = require('./flowcontroller');

function processTndResult() {

    // init msgQueue
    const msgQueue = flowController.initQueue(transform, producer.produce, consumer.commitAndResume);

    //init consumer
    consumer.init(msgQueue);
}

const transform = async (message) => {
    let parsedMessage = JSON.parse(message.value);
    console.info(`processing partition: ${message.partition} offset: ${message.offset}  received message: ${JSON.stringify(parsedMessage)}`);
    return transformer.transformer(parsedMessage);
};

app.get('/health', (req, res) => {
    let health = {
        description: "assn-gtas-transformer health check",
        status: "UP"
    };
    res.send(health);
    res.end();
});


app.listen(port, function () {
    processTndResult();
});







