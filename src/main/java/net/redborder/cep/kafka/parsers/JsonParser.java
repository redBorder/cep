package net.redborder.cep.kafka.parsers;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Map;

public class JsonParser implements Parser {
    private ObjectMapper mapper;

    public JsonParser(){
        mapper = new ObjectMapper();
    }

    @Override
    public Map<String, Object> parse(String event) {
        Map<String, Object> map = null;

        try {
            map = mapper.readValue(event, Map.class);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return map;
    }
}
