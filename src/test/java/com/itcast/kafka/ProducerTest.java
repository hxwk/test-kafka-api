package com.itcast.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerTest {
    public static void main(String[] args) {

        Properties properties = new Properties();
        //传递参数
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, PropertiesUtil.getStringByKey("bootstrap_servers", "application.properties"));
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //创建生产者
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        String topic = "kafkatopic";
        //创建ProducerRecord
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> event = new ProducerRecord<>(topic, "event-" + i);
            //send数据
            kafkaProducer.send(event);
        }
        //关闭数据
        kafkaProducer.close();
    }
}
