package com.jose

import groovy.util.logging.Slf4j

import java.util.concurrent.CountDownLatch

@Slf4j
class Kafka {
    static void main(String[] args) {

        // execute the producer
        new Producer(bootstrapServers: '127.0.0.1:9092').producerDemo()

        // execute the consumer
        //new Consumer(bootstrapServers: '127.0.0.1:9092').consumerDemo()

        // execute the consumer with threads

        // create a runnable instance
        CountDownLatch latch = new CountDownLatch(1)
        Runnable consumerRunnable = new ConsumerThreads(latch)
        // start the thread
        Thread consumer = new Thread(consumerRunnable)

        consumer.start()

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Caught shutdown hook");
            ((ConsumerThreads) consumerRunnable).shutdown()
            try {
                latch.await()
            } catch (InterruptedException e) {
                e.printStackTrace()
            }
            log.info("Application has exited")
        }

        ))

        try {
            latch.await()
        } catch(InterruptedException e) {
            log.info("Application Interrupted", e)
        }

    }
}
