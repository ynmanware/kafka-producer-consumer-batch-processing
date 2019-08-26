# kafka-producer-consumer-batch-processing
trying to address the issue of kafka consumer disconnect and producer failing to push messages 
using Async library and flow controller

It is an example of common Microservice that 
1. consumes messages from kafka queue A (in batches) 
2. transforms/processes it
3. produces back to the kafka queue B

Conditions:
2. it is required to process the batch of messages in parallel and asynchronously
3. it is also required to have transaction around batch of messages 
    - the microservice either processes a whole batch or nothing

However there are may be few error scenarios
1. Consumer is not able to connect to kafka queue A
    - it may be because there is a bug in kafka-node library or kafka get reset/restarts/crashes
2. Producer is not able to produce the messages because of one of the reason mentioned above

It is an attempt write a resilient microservice that overcome these issues
through a small framework - refer flowcontroller.js

