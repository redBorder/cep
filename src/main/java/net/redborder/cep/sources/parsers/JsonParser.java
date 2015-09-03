package net.redborder.cep.sources.parsers;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Map;

/**
 * This class implements a JSON parser that uses the
 * Parser interface to parse strings messages formatted as
 * JSON text.
 */

public class JsonParser implements Parser {
    // The Jackson object that transforms a JSON string into a Java map
    private ObjectMapper mapper;

    // Init a new JSON parser
    public JsonParser(){
        mapper = new ObjectMapper();
    }

    /**
     * This method produces a map from a given JSON string
     *
     * @param message The JSON string
     * @return A map with the values of the parsed JSON string given
     */

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> parse(String message) {
        Map<String, Object> map = null;

        try {
            map = mapper.readValue(message, Map.class);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return map;
    }
}
