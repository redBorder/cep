package net.redborder.correlation.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Map;

public class JsonParse implements Parser<String>{
    private ObjectMapper mapper;

    public JsonParse(){
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
