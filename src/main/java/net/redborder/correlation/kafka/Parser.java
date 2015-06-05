package net.redborder.correlation.kafka;

import java.util.Map;

public interface Parser{
     Map<String, Object> parse(String event);
}
