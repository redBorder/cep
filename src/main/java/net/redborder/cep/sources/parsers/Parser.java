package net.redborder.cep.sources.parsers;

import java.util.Map;

/**
 * This interface defines a parser that can be used by the
 * ParsersManager to parse messages from a string into a Java map.
 *
 * @see ParsersManager
 */

public interface Parser {

    /**
     * This method receives a message string and produces a
     * java map from the string given.
     *
     * @param message The string message that will be parsed
     * @return A map that is the result of parsing the given string
     */

    Map<String, Object> parse(String message);
}
