package cn.itcast.kafka.produce;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class CustomePartition implements Partitioner {
    AtomicInteger i = new AtomicInteger(0);
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if(i.get()>=65534){
            i.set(1);
        }
        int partition = i.get() % cluster.partitionCountForTopic(topic);
        i.getAndIncrement();
        return partition;
}

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
