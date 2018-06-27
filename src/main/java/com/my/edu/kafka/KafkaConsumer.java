package com.my.edu.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by lenovo510 on 27.06.2018.
 */
public class KafkaConsumer {

    private final static String TOPIC = "test_topic";
    private final static String BOOTSTRAP_SERVERS =
            "192.168.0.13:9092";

    public static void main(String[] args) {

        try {
            runConsumer(100);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    static void runConsumer(int readMessageCount) throws Exception {
        org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer = createConsumer();
        consumer.subscribe(Arrays.asList(TOPIC));
        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(readMessageCount);
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println("key:" + consumerRecord.key()
                        + " val:" + consumerRecord.value()
                        + " partition:" + consumerRecord.partition()
                        + "topic" + consumerRecord.topic());
            }
        }
    }

    private static org.apache.kafka.clients.consumer.KafkaConsumer<String, String> createConsumer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "KafkaConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");//commit offsets periodically
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");//commit offsets every second
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");//if no offsets presented before start from first offset
        return new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(props);
    }
}
