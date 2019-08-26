const util = require('./util');

const {log, configureLogger} = require('../util/logger');

exports.transformer = async (event) => {
    configureLogger(event);
    log.info('uploading image start ~ 3000');
    await util.sleep(3000);
    configureLogger(event);
    log.info('uploading image end');
    return event;
};
