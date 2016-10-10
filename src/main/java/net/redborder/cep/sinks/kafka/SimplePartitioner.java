package net.redborder.cep.sinks.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * This class is used by KafkaSink as a partitioning method for
 * the messages. It defines which kafka partition will receive
 * each message produced by the Sink.
 */

public class SimplePartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        return (key == null) ? 0 : Math.abs(key.hashCode() % cluster.partitionCountForTopic(topic));
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> conf) {
    }
}

