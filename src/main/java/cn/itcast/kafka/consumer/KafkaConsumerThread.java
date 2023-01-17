package cn.itcast.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class KafkaConsumerThread  implements Runnable{

    private final KafkaConsumer<String, String> consumer;
    private final List<String> topics;

    public KafkaConsumerThread(String groupId, List<String> topics) {
        this.topics = topics;
        Properties props = new Properties();
        props.put("bootstrap.servers", "node01:9092,node02:9092,node03:9092");
        props.put("group.id", groupId);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        this.consumer = new KafkaConsumer<String, String>(props);
    }

    public void run() {
        try {
            consumer.subscribe(this.topics);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(5000);
                Set<Integer> partitionCount = new HashSet<>();
                for (ConsumerRecord<String, String> record : records) {
                    Map<String, Object> data = new HashMap<>();
                    data.put("partition", record.partition());
                    partitionCount.add(record.partition());
                    data.put("offset", record.offset());
                    data.put("value", record.value());

                    System.out.println(data);
                }
                System.out.println(partitionCount.size());
            }
        } catch (WakeupException e) {
            // ignore for shutdown
        } finally {
            consumer.close();
        }
    }

    public void shutdown() {
        consumer.wakeup();
    }

    public static void main(String[] args) {
        int numConsumers = 2;
        String groupId = "consumer-group1";
        List<String> topics = Arrays.asList("test1");
        ExecutorService executor = Executors.newFixedThreadPool(numConsumers);

        final List<KafkaConsumerThread> consumers = new ArrayList<>();
        for (int i = 0; i < numConsumers; i++) {
            KafkaConsumerThread consumer = new KafkaConsumerThread(groupId, topics);
            consumers.add(consumer);
            executor.submit(consumer);
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                for (KafkaConsumerThread consumer : consumers) {
                    consumer.shutdown();
                }
                executor.shutdown();
                try {
                    executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }
}