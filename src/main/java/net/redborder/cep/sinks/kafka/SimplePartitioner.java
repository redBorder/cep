package net.redborder.cep.sinks.kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * This class is used by KafkaSink as a partitioning method for
 * the messages. It defines which kafka partition will receive
 * each message produced by the Sink.
 */

public class SimplePartitioner implements Partitioner {
    public SimplePartitioner (VerifiableProperties props) { }

    /**
     * Returns the partition index that will receive the message associated
     * with the given key.
     *
     * @param key The key of the message
     * @param a_numPartitions The number of partitions
     * @return The index of the partition that will receive the message associated with the given key
     */
    public int partition(Object key, int a_numPartitions) {
        String stringKey = (String) key;
        int offset = stringKey.hashCode();
        return Math.abs(offset % a_numPartitions);
    }
}
