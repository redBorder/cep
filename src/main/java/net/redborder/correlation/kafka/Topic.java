package net.redborder.correlation.kafka;

import net.redborder.correlation.kafka.disruptor.EventProducer;
import net.redborder.correlation.kafka.parsers.Parser;

import java.util.HashMap;
import java.util.Map;

public class Topic {
    public final String name;
    public final Parser parser;
    public final EventProducer eventProducer;
    private Integer partitions;

    public Topic(String name, Integer partitions, Parser parser, EventProducer eventProducer) {
        this.name = name;
        this.partitions = partitions;
        this.parser = parser;
        this.eventProducer = eventProducer;
    }

    public Integer getPartitions() {
        return partitions;
    }

    public void setPartitions(Integer partitions) {
        this.partitions = partitions;
    }

    public Map<String, Integer> toMap(){
        Map<String, Integer> hash = new HashMap<>();
        hash.put(name, partitions);
        return hash;
    }
}
