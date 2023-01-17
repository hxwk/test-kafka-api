package cn.itcast.kafka.produce;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Author itcast
 * Date 2020/5/16 9:41
 * Desc 发布数据到 kafka 集群
 */
public class KafkaProducerDemo {
    /**
     * 1.创建配置 properties
     * 2.创建 KafkaProducer
     * 3.发布数据到 kafka 集群
     * 4.关闭 KafkaProducer
     */
    public static void main(String[] args) {
        //1.创建配置 properties
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node01:9092,node02:9092,node03:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        //props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"");
        //2.创建 KafkaProducer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        //3.发布数据到 kafka 集群
        for(int i=0;i<10;i++) {
            ProducerRecord<String, String> records = new ProducerRecord<>("test1", "key", "value_" + i);
            producer.send(records);
        }
        //4.关闭 KafkaProducer
        producer.close();
    }
}
