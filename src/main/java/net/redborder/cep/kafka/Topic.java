package net.redborder.cep.kafka;

import net.redborder.cep.kafka.disruptor.EventProducer;
import net.redborder.cep.kafka.parsers.Parser;

import java.util.HashMap;
import java.util.Map;

public class Topic {
    private final String name;
    private final Parser parser;
    private final EventProducer eventProducer;
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

    public String getName() {
        return name;
    }

    public Parser getParser() {
        return parser;
    }

    public EventProducer getEventProducer() {
        return eventProducer;
    }

    public Map<String, Integer> toMap(){
        Map<String, Integer> hash = new HashMap<>();
        hash.put(name, partitions);
        return hash;
    }

    public String toString() {
        return this.name;
    }
}
