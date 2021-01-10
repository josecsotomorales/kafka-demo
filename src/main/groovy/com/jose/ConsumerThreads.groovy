package com.jose

import groovy.util.logging.Slf4j
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer

import java.time.Duration

import java.util.concurrent.CountDownLatch

@Slf4j
class ConsumerThreads implements Runnable {

    def bootstrapServers = '127.0.0.1:9092'
    def groupId = 'demo-application'
    def topic = 'kafka-demo'
    def consumer

    // Latch for dealing with multiple threads
    CountDownLatch latch

    ConsumerThreads(CountDownLatch latch) {

        // create producer properties
        def properties = new Properties()
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.name)
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.name)
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, 'earliest')

        // create the consumer
        consumer = new KafkaConsumer<String, String>(properties)

        // subscribe the consumer to a topic
        consumer.subscribe(Arrays.asList(topic))

        this.latch = latch
    }

    @Override
    void run() {
        try {
            while(true) {
                // poll for new data
                def records = consumer.poll(Duration.ofMillis(100)) as ConsumerRecords<String, String>

                records.each {
                    log.info("Partition: ${it.partition()}, Offset: ${it.offset()} - Key: ${it.key()}, Value: ${it.value()}")
                }
            }
        } catch(WakeupException e) {
            log.info('Received Shutdown Signal!')
        } finally {
            // close the consumer
            consumer.close()
            latch.countDown()
        }
    }

    void shutdown() {
        // the wakeup method interrupts consumer.poll(), it will throw WakeUpException
        consumer.wakeup()
    }
}
