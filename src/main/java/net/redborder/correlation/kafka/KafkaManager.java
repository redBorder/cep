package net.redborder.correlation.kafka;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.dsl.Disruptor;
import net.redborder.correlation.kafka.disruptor.EventProducer;
import net.redborder.correlation.kafka.disruptor.MapEvent;
import net.redborder.correlation.kafka.disruptor.MapEventFactory;
import net.redborder.correlation.kafka.parsers.Parser;
import net.redborder.correlation.util.ConfigData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

@ThreadSafe
public class KafkaManager {
    private final Logger log = LoggerFactory.getLogger(KafkaManager.class);

    private final ConsumerManager consumerManager;
    private final List<Topic> topics;
    private final List<Topic> unmodifiableTopics;

    public KafkaManager(EventHandler eventHandler) {
        Integer ringBufferSize = ConfigData.getRingBufferSize();
        topics = new ArrayList<>();
        unmodifiableTopics = Collections.unmodifiableList(topics);

        for (Map.Entry<String, String> entry : ConfigData.getTopics().entrySet()) {
            String parserName = entry.getValue();

            try {
                // Get parser from config
                Class parserClass = Class.forName(parserName);
                Constructor<Parser> constructor = parserClass.getConstructor();
                Parser parser = constructor.newInstance();

                // Create the disruptor for this topic and start it
                Disruptor<MapEvent> disruptor = new Disruptor<>(new MapEventFactory(), ringBufferSize, Executors.newCachedThreadPool());
                disruptor.handleEventsWith(eventHandler);
                disruptor.start();

                // Create topic entry
                /* TODO Discover partitions using ZK */
                topics.add(new Topic(entry.getKey(), 4, parser, new EventProducer(disruptor.getRingBuffer())));
            } catch (ClassNotFoundException e) {
                log.error("Couldn't find the class associated with the parser " + parserName);
            } catch (NoSuchMethodException | InstantiationException | InvocationTargetException | IllegalAccessException e) {
                log.error("Couldn't create the instance associated with the parser " + parserName, e);
            }
        }

        consumerManager = new ConsumerManager();
        consumerManager.start(unmodifiableTopics);
    }

    public List<Topic> getTopics() {
        return unmodifiableTopics;
    }

    public void shutdown() {
        consumerManager.shutdown();
    }
}
