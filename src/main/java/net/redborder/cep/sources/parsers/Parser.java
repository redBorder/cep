package net.redborder.cep.sources.parsers;

import java.util.Map;

public interface Parser {
     Map<String, Object> parse(String event);
}
