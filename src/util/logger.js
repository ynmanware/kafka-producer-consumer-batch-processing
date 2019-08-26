const log4j = require('log4js');

const COMPONENT_NAME = 'kafka-producer-consumer-batch-processing';

log4j.configure({
    appenders: {
        COMPONENT_NAME: {
            type: 'stdout', //stdout -> for console log || file -> for Appender log
            maxLogSize: 10485760,
            backups: 3,
            compress: true,
            layout: {
                type: 'pattern',
                pattern: '%d %p [ %c %X{fileName}] [%h] %m'
            }
        }
    },
    categories: {default: {appenders: [COMPONENT_NAME], level: 'ALL'}}
});

const log = log4j.getLogger(COMPONENT_NAME);

exports.log = log;

exports.configureLogger = (fileName) => {
    try {
        log.addContext('fileName', (fileName ? fileName : ''));
    }
    catch (e) {
        console.error('error in configuring logger -' + e.message);
    }
};

