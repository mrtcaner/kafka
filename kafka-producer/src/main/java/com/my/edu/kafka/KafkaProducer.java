package com.my.edu.kafka;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Created by lenovo510 on 26.06.2018.
 */
public class KafkaProducer {

    private final static String TOPIC = "test_topic";
    private final static String BOOTSTRAP_SERVERS =
            "192.168.0.13:9092";

    public static void main(String[] args) {

        try {
            runProducer(10);
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }

    static void runProducer(final int sendMessageCount) throws Exception {
        final org.apache.kafka.clients.producer.KafkaProducer<String, String> producer = createProducer();
        long time = System.currentTimeMillis();

        try {
            for (long index = time; index < time + sendMessageCount; index++) {
                final ProducerRecord<String, String> record =
                        new ProducerRecord(TOPIC, Long.toString(index),
                                "Hello Mom " +  Long.toString(index));

                RecordMetadata metadata = producer.send(record).get();

                long elapsedTime = System.currentTimeMillis() - time;
                System.out.printf("sent record(key=%s value=%s) " +
                                "meta(partition=%d, offset=%d) time=%d\n",
                        record.key(), record.value(), metadata.partition(),
                        metadata.offset(), elapsedTime);

            }
        } finally {
            producer.flush();
            producer.close();
        }
    }

    private static org.apache.kafka.clients.producer.KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG,"1");

        return new org.apache.kafka.clients.producer.KafkaProducer<String, String>(props);
    }
}
