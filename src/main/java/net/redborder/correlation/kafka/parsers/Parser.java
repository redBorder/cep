package net.redborder.correlation.kafka.parsers;

import java.util.Map;

public interface Parser<T>{
     Map<String, Object> parse(T event);
}
