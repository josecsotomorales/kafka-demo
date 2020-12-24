package com.jose

import groovy.util.logging.Slf4j

@Slf4j
class Kafka {
    static void main(String[] args) {

        def producer = new Producer(bootstrapServers: '127.0.0.1:9092')
        // call the producer methods
        producer.producerDemo()
        producer.producerDemoKeys()

        def consumer = new Consumer(bootstrapServers: '127.0.0.1:9092')
        // call the consumer methods
        consumer.consumerDemo()


    }
}
