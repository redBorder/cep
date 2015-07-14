package net.redborder.cep.kafka.parsers;

import java.util.Map;

public interface Parser {
     Map<String, Object> parse(String event);
}
