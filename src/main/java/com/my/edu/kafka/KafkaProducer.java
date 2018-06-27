package com.my.edu.kafka;

import org.apache.kafka.clients.producer.Callback;
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

    static void runProducer(int sendMessageCount) throws Exception {
        org.apache.kafka.clients.producer.KafkaProducer<String, String> producer = createProducer();

        try {
            for (int i = 0; i < sendMessageCount; i++) {
                String message= "Hello there " + i;
                ProducerRecord<String, String> record =
                        new ProducerRecord(TOPIC, message);
                long startTime = System.currentTimeMillis();
                RecordMetadata metadata = producer.send(record,new ProducerCallBack(startTime,i,message)).get();

               System.out.printf("sent record(value=%s) " +
                                        "meta(partition=%d, offset=%d) \n", record.value(), metadata.partition(),
                        metadata.offset());
            }
        } finally {
            producer.close();
        }
    }

    private static org.apache.kafka.clients.producer.KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        return new org.apache.kafka.clients.producer.KafkaProducer<String, String>(props);
    }
}

class ProducerCallBack implements Callback {

    private long startTime;
    private int key;
    private String message;


    public ProducerCallBack(long startTime, int key, String message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if(recordMetadata == null) {
            e.printStackTrace();
        }else{
            System.out.println("key:" + key
                    + " val:" + message
                    + " partition:" + recordMetadata.partition()
                    + "offset:" + recordMetadata.offset()
                    + "elapsedTime:" + elapsedTime);
        }
    }
}
