package net.redborder.correlation.kafka;

import net.redborder.correlation.kafka.parsers.Parser;
import net.redborder.correlation.util.ConfigData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

public class KafkaManager {
    private  final Logger log = LoggerFactory.getLogger(KafkaManager.class);
    private ConsumerManager consumerManager;
    private List<Topic> topics;

    public KafkaManager() {
        initTopics();
        consumerManager = new ConsumerManager();
        consumerManager.start(getTopics());
    }

    private void initTopics() {
        topics = new CopyOnWriteArrayList<>();
        for (Map.Entry<String, String> entry : ConfigData.getTopics().entrySet()) {
            String parserName = entry.getValue();

            try {
                Class parserClass = Class.forName(parserName);
                Constructor<Parser> constructor = parserClass.getConstructor();
                Parser parser = constructor.newInstance();
                Topic t = new Topic(entry.getKey(), /* TODO Disover parttions using ZK */ 4, parser);
                topics.add(t);
            } catch (ClassNotFoundException e) {
                log.error("Couldn't find the class associated with the parser " + parserName);
            } catch (NoSuchMethodException | InstantiationException | InvocationTargetException | IllegalAccessException e) {
                log.error("Couldn't create the instance associated with the parser " + parserName, e);
            }
        }

    }

    public List<Topic> getTopics() {
        return topics;
    }

    private void shutdown(){
        consumerManager.shutdown();
    }
}
