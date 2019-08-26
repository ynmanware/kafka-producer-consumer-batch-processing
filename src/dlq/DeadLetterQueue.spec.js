const deadLetterQueue = require('./DeadLetterQueue');

describe('test queue', () => {
    it('should be able to push item', function () {
        //deadLetterQueue.push('A');
        deadLetterQueue.pause();
        expect(deadLetterQueue.length()).toEqual(1);
    });
});

describe('Test Reconnect', () => {
    it('producer is able to reconnect and produce the message', async function () {
        deadLetterQueue.push('B');
        //await handleFailedMessages();
    });
});
