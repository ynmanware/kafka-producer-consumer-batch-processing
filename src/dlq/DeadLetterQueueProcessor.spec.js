const {handleFailedMessages} = require('./DeadLetterQueueProcessor');
const kafkaProducer = require('../kafkaClient/producer');

const deadLetterQueue = require('./DeadLetterQueue');

const MAX = 50000;


// terminate one of the brokers while running this test
describe('test Producer', () => {
    it('should be able to push item', async function (done) {
        let startTime = Date.now();

        for(let i = 1; i <= MAX; i++) {
            await kafkaProducer.kafkaProducer({'id': i}, startTime);
            await sleep(100);
        }

        console.log(`$$$$$$$$$$$ DeadLetterQueue Size: ${deadLetterQueue.length()}`);

        await handleFailedMessages();
        done();
    });
});

beforeAll(async () => {
    jest.setTimeout(300000000);
});

afterAll(async done => {
    // Closing the DB connection allows Jest to exit successfully.
    done();
});

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}