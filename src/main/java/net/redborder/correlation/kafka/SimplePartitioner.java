package net.redborder.correlation.kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class SimplePartitioner implements Partitioner {
    public SimplePartitioner (VerifiableProperties props) { }

    public int partition(Object key, int a_numPartitions) {
        String stringKey = (String) key;
        int offset = stringKey.hashCode();
        return Math.abs(offset % a_numPartitions);
    }
}
