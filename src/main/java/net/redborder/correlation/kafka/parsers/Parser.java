package net.redborder.correlation.kafka.parsers;

import java.util.Map;

public interface Parser {
     Map<String, Object> parse(String event);
}
