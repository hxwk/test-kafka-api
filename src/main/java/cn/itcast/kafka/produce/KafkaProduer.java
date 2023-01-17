package cn.itcast.kafka.produce;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProduer {
    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node01:9092,node02:9092,node03:9092");
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.BATCH_SIZE_CONFIG, 20);
        //configs.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomePartition.class);
        //1.kafkaProducer
        KafkaProducer producer = new KafkaProducer(configs);
        //2.生产数据载体
        for (int idx = 0; idx < 5000; idx++) {
            ProducerRecord record = new ProducerRecord("test","key",  "record-" + idx);
            //3.发送数据
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e != null) {
                        System.out.println("failed!");
                    } else {
                        System.out.printf("offset %s,topic %s,partition %s", metadata.offset(), metadata.topic(), metadata.partition());
                    }
                }
            });
        }
        //4.关闭
        producer.close();
    }
}
