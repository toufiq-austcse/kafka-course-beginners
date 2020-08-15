package com.toufiq;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
        // Setup Properties
        Properties properties = new Properties();
        String bootstrapServer = "127.0.0.1:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the producer

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // Producer Record
        final ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world");
        // Send Data
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
//                if (e == null) {
//                  logger.info("Received new metadata \n"+
//                          "Topic "+recordMetadata.topic()+
//                          "\n Partition "+recordMetadata.partition()+
//                          "\n Offset "+recordMetadata.offset()+
//                          "\n Timestamp "+recordMetadata.timestamp());
//                } else {
//                    e.printStackTrace();
//                }
                logger.info("Received new metadata \n"+
                        "Topic "+recordMetadata.topic()+
                        "\n Partition "+recordMetadata.partition()+
                        "\n Offset "+recordMetadata.offset()+
                        "\n Timestamp "+recordMetadata.timestamp());
            }
        });


    }
}
