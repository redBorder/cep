package net.redborder.cep.sources.kafka;

import net.redborder.cep.sources.Source;
import net.redborder.cep.sources.disruptor.EventProducer;
import net.redborder.cep.sources.parsers.Parser;

import java.util.HashMap;
import java.util.Map;

public class Topic {
    private final String name;
    private final Parser parser;
    private final Source source;
    private Integer partitions;

    public Topic(String name, Integer partitions, Parser parser, Source source) {
        this.name = name;
        this.partitions = partitions;
        this.parser = parser;
        this.source = source;
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

    public Source getSource() {
        return source;
    }

    public Map<String, Integer> toMap(){
        Map<String, Integer> hash = new HashMap<>();
        hash.put(name, partitions);
        return hash;
    }

    public String toString() {
        return this.name + "(" + this.partitions + ")";
    }
}
