package net.redborder.cep.sources.parsers;

import net.redborder.cep.sources.parsers.exceptions.ParserNotExistException;
import net.redborder.cep.util.ConfigData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

/**
 * This class manages and serves the different parsers available on the system.
 * <p>It has two main responsibilities: instanciate the different parsers specified on the config file,
 * and serve those parsers to the objects that should use them.
 * <p>To do so, it stores the parser instances along the parser names, as specified on the config file
 *
 * @see ConfigData
 */

public class ParsersManager {
    private static final Logger log = LogManager.getLogger(ParsersManager.class);

    // Stores the parsers instances associated with each parser name
    Map<String, Parser> parsers = new HashMap<>();

    // Stores the parsers instances associated with each stream
    Map<String, Parser> streams = new HashMap<>();

    /**
     * This method instanciates the parsers specified on the config file and
     * builds a cache that stores the parser that will be used with each stream
     */

    public ParsersManager() {
        // For each parser specified on the config file...
        for (Map.Entry<String, String> parserEntry : ConfigData.getParsers().entrySet()) {
            try {
                // Get the parser full qualified class name from config and instanciate the class
                Class parserClass = Class.forName(parserEntry.getValue());
                Constructor<Parser> constructor = parserClass.getConstructor();
                Parser parser = constructor.newInstance();

                // Save a reference to that parser
                parsers.put(parserEntry.getKey(), parser);
            } catch (ClassNotFoundException e) {
                log.error("Couldn't find the class associated with the parser " + parserEntry.getKey());
            } catch (NoSuchMethodException | InstantiationException | InvocationTargetException | IllegalAccessException e) {
                log.error("Couldn't create the instance associated with the parser " + parserEntry.getKey(), e);
            }
        }

        // Save a reference of the parser associated with each stream
        // from the config file
        for (String streamName : ConfigData.getStreams()) {
            String parseName = ConfigData.getParser(streamName);
            try {
                streams.put(streamName, getParserByName(parseName));
            } catch (ParserNotExistException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * This method returns the parser instance associated with the given parser name
     *
     * @param parseName The parser that will be returned
     * @return The parser associated with the given parser name
     * @throws ParserNotExistException if the parser is not present on the config file
     */

    public Parser getParserByName(String parseName) throws ParserNotExistException {
        Parser parser = parsers.get(parseName);
        if (parser == null) {
            throw new ParserNotExistException("This " + parseName + " doesn't appear on config file");
        } else {
            return parser;
        }
    }

    /**
     * This method returns the parser instance associated with the stream name
     *
     * @param streamName The stream name
     * @return The parser associated with the given stream name
     * @throws ParserNotExistException if the parser is not present on the config file
     */

    public Parser getParserByStream(String streamName) throws ParserNotExistException {
        Parser parser = streams.get(streamName);
        if (parser == null) {
            throw new ParserNotExistException("This " + streamName + " doesn't appear on config file or has incorrect parsers: " + ConfigData.getParser(streamName));
        } else {
            return parser;
        }
    }

    /**
     * This method parses the given message with the parser associated
     * with the given stream name
     *
     * @param streamName The stream name
     * @param msg        The message to be parsed
     * @return A map that results from parsing the given message with the
     * parser associated with the given stream name
     */

    public Map<String, Object> parse(String streamName, String msg) {
        Parser parser = getParserByStream(streamName);
        return parser.parse(msg);
    }
}
