package com.jose

import groovy.util.logging.Slf4j
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer

@Slf4j
class Kafka {
    static void main(String[] args) {

        def bootstrapServers = '127.0.0.1:9092'

        // create producer properties
        def properties = new Properties()
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.name)
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.name)

        // create the producer
        def producer = new KafkaProducer<String, String>(properties)

       // create producer record
       def record = new ProducerRecord<String, String>('kafka-demo', 'Hello Kafka!')

        // send data to producer - asynchronous
        producer.send(record, new Callback() {
            @Override
            void onCompletion(RecordMetadata metadata, Exception exception) {
                // executes everytime a record is sent or and exception is thrown
                if(!exception) {
                    // the record was sent
                    log.info(""" 
                                 |Received new metadata.
                                 |Topic:        ${metadata.topic()}
                                 |Partition:    ${metadata.partition()}
                                 |Offset:       ${metadata.offset()}
                                 |Timestamp:    ${metadata.timestamp()}       
                                 |""".stripMargin())
                } else {
                    log.error("Error while producing.", exception)
                }

            }
        })

        // flush data
        producer.flush()

        // flush and close producer
        producer.close()

    }
}
