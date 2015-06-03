package net.redborder.correlation.kafka;

import java.util.Map;

public interface Parser<T>{
     Map<String, Object> parse(T event);
}
