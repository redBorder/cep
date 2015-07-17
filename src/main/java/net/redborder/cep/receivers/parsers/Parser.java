package net.redborder.cep.receivers.parsers;

import java.util.Map;

public interface Parser {
     Map<String, Object> parse(String event);
}
