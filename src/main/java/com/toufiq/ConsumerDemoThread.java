package com.toufiq;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoThread {
    private Logger logger = LoggerFactory.getLogger(ConsumerDemoThread.class);

    public static void main(String[] args) {
        new ConsumerDemoThread().run();


    }

    private ConsumerDemoThread() {

    }

    private void run() {

        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-Sixth-application";
        String topic = "first_topic";
        CountDownLatch countDownLatch = new CountDownLatch(1);

        logger.info("Creating consumer the consumer thread");
        Runnable myConsumerRunable = new ConsumerRunable(topic, bootstrapServer, groupId, countDownLatch);
        Thread myThread = new Thread(myConsumerRunable);
        myThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught Shut down hoot");
            ((ConsumerRunable) myConsumerRunable).shutDown();
            try {
                countDownLatch.await();
            }catch (InterruptedException e){
                e.printStackTrace();
            }
            logger.info("Application Has Been Exited");

        }));

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            logger.error("Application Got Interrupted", e);
        } finally {
            logger.info("Application is closing");
        }

    }

    public class ConsumerRunable implements Runnable {
        private Logger logger = LoggerFactory.getLogger(ConsumerRunable.class);
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;

        public ConsumerRunable(String topic, String bootstrapServer, String groupId, CountDownLatch latch) {
            this.latch = latch;

            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumer = new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + ", value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received Shutdown Signal");
            } finally {
                consumer.close();
                // tell our main code we're done with consumer
                latch.countDown();
            }

        }

        public void shutDown() {
            // to interrupt consumer.poll();
            // it will throw the WakeUpException
            consumer.wakeup();
        }
    }
}
