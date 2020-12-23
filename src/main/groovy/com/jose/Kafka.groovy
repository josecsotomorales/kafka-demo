package com.jose

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer

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
        producer.send(record)

        // flush data
        producer.flush()

        // flush and close producer
        producer.close()

    }
}
