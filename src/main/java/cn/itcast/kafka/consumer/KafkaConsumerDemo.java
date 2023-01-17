package cn.itcast.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.lang.reflect.Executable;
import java.util.Arrays;
import java.util.Properties;

/**
 * Author itcast
 * Date 2020/5/16 10:14
 * Desc 实现 kafka 消费者
 */
public class KafkaConsumerDemo {
    public static void main(String[] args) {
        /**
         * 1.创建 properties
         * 2.创建 KafkaConsumer
         * 3.订阅 指定 kafka topic 中的数据
         * 4.消费数据
         */
        //1.创建 properties
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"node01:9092,node02:9092,node03:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //创建 消费组
        props.put(ConsumerConfig.GROUP_ID_CONFIG,"consumer-group1");
        //2.创建 KafkaConsumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        try{
            //3.订阅 指定 kafka topic 中的数据
            consumer.subscribe(Arrays.asList("test1"));
            //4.消费数据
            while(true){
                ConsumerRecords<String, String> records = consumer.poll(5000);
                for(ConsumerRecord<String,String> record:records){
                    int partition = record.partition();
                    System.out.println(partition);
                    if(partition == 0){
                        System.out.println(record.topic());
                        System.out.println(record.value());
                        System.out.println(record.offset());
                    }
                }
            }
        }catch (Exception ex){

        }finally {
            consumer.close();
        }
    }
}
