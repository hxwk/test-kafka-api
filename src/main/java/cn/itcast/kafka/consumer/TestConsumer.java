package cn.itcast.kafka.consumer;

import cn.itcast.kafka.util.PropertiesUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

public class TestConsumer {
    public static void main(String[] args) {

        Properties properties = new Properties();
        //bootstrap-server
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, PropertiesUtil.getStringByKey("bootstrap_servers", "application.properties"));
        //group-id
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        //从最近最大的offset开始消费
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //key deserializer
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        //value deserializer
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        //enable_auto_commit 默认自动提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        //auto_commit_interval_ms 自动提交offerset时间 单位毫秒
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);

        //kafkaConsumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Arrays.asList("B2CDATA_COLLECTION3,kafkatopic"));

        //kafkaConsumer 1000 轮询
        ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(1000);
        //while
            while (true) {
                //消费者记录不为空
                //遍历消费者记录
                //如果消费者的分区为0，打印对应的topic 分区 offset key value
                if (consumerRecords != null) {
                    for (ConsumerRecord<String, String> record : consumerRecords) {
                        if (record.partition() == 0) {
                            System.out.println("topic : " + record.topic());
                            System.out.println("partition : " + record.partition());
                            System.out.println("offset " + record.offset());
                            System.out.println("key " + record.key());
                            System.out.println("value " + record.value());
                        }

                    }

                }
            //System.out.println("data is null, please check");
        }
    }
}
