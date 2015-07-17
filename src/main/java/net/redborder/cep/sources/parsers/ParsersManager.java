package net.redborder.cep.sources.parsers;


import net.redborder.cep.sources.parsers.exceptions.ParserNotExistException;
import net.redborder.cep.util.ConfigData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

public class ParsersManager {
    private static final Logger log = LoggerFactory.getLogger(ParsersManager.class);
    Map<String, Parser> parsers = new HashMap<>();
    Map<String, Parser> streams = new HashMap<>();

    public ParsersManager() {
        for (Map.Entry<String, String> parserEntry : ConfigData.getParsers().entrySet()) {
            try {
                // Get parser from config
                Class parserClass = Class.forName(parserEntry.getValue());
                Constructor<Parser> constructor = parserClass.getConstructor();
                Parser parser = constructor.newInstance();
                parsers.put(parserEntry.getKey(), parser);
            } catch (ClassNotFoundException e) {
                log.error("Couldn't find the class associated with the parser " + parserEntry.getKey());
            } catch (NoSuchMethodException | InstantiationException | InvocationTargetException | IllegalAccessException e) {
                log.error("Couldn't create the instance associated with the parser " + parserEntry.getKey(), e);
            }
        }

        for (String streamName : ConfigData.getStreams()){
            String parseName = ConfigData.getParser(streamName);
            try {
                streams.put(streamName, getParserByName(parseName));
            } catch (ParserNotExistException e) {
                e.printStackTrace();
            }
        }
    }

    public Parser getParserByName(String parseName) throws ParserNotExistException {
        Parser parser = parsers.get(parseName);
        if(parser == null){
            throw new ParserNotExistException("This " + parseName + " doesn't appear on config file");
        } else {
            return parser;
        }
    }

    public Parser getParserByStream(String streamName) throws ParserNotExistException {
        Parser parser = streams.get(streamName);
        if(parser == null){
            throw new ParserNotExistException("This " + streamName + " doesn't appear on config file or has incorrect parsers: " + ConfigData.getParser(streamName));
        } else {
            return parser;
        }
    }

    public Map<String, Object> parse(String streamName, String msg){
        Parser parser = getParserByStream(streamName);
        return parser.parse(msg);
    }
}
