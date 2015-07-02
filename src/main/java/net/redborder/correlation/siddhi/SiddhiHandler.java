package net.redborder.correlation.siddhi;

import com.lmax.disruptor.EventHandler;
import net.redborder.correlation.kafka.disruptor.MapEvent;
import net.redborder.correlation.rest.RestException;
import net.redborder.correlation.rest.RestListener;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;

import javax.ws.rs.NotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SiddhiHandler implements RestListener, EventHandler<MapEvent> {
    private final Logger log = LoggerFactory.getLogger(SiddhiHandler.class);

    private Map<String, Map<String, Object>> rawQueries = new HashMap<>();
    private List<ExecutionPlan> executionPlans;
    private SiddhiManager siddhiManager;
    private ObjectMapper mapper;

    public SiddhiHandler() {
        this.siddhiManager = new SiddhiManager();
        this.executionPlans = new ArrayList<>();
        this.mapper = new ObjectMapper();
    }

    public void stop() {
        log.info("Stopping down execution plans...");

        for (ExecutionPlan executionPlan : executionPlans) {
            executionPlan.stop();
        }
    }

    @Override
    public void onEvent(MapEvent mapEvent, long sequence, boolean endOfBatch) throws Exception {
        Map<String, Object> data = mapEvent.getData();
        String src = (String) data.get("src");
        String dst = (String) data.get("dst");
        Integer bytes = (Integer) data.get("bytes");

        for (ExecutionPlan executionPlan : executionPlans) {
            executionPlan.getInputHandler().send(new Object[]{src, dst, bytes});
        }
    }

    @Override
    public void add(Map<String, Object> element) throws RestException {
        String id = element.get("id").toString();
        if (rawQueries.containsKey(id)) {
            throw new RestException("Query with id " + id + " already exist");
        } else {
            ExecutionPlan executionPlan = ExecutionPlan.fromMap(element);
            executionPlan.start(siddhiManager);
            executionPlans.add(executionPlan);

            rawQueries.put(id, element);
            log.info("New query added: {}", element);
        }
    }

    @Override
    public void remove(String id) throws NotFoundException, RestException {
        if (rawQueries.remove(id) != null) {
            log.info("Query with the id {} has been removed", id);
            log.info("Current queries: {}", rawQueries);
        } else {
            throw new NotFoundException("Query with the id " + id + " is not present");
        }
    }

    @Override
    public void synchronize(List<Map<String, Object>> elements) throws RestException {
        Map<String, Map<String, Object>> newQueries = new HashMap<>();

        for (Map<String, Object> element : elements) {
            String id = element.get("id").toString();
            newQueries.put(id, element);
        }

        rawQueries = newQueries;
    }

    @Override
    public String list() throws RestException {
        List<Map<String, Object>> listQueries = new ArrayList<>();

        for (Map.Entry<String, Map<String, Object>> query : rawQueries.entrySet()) {
            listQueries.add(query.getValue());
        }

        try {
            return mapper.writeValueAsString(listQueries);
        } catch (IOException e) {
            throw new RestException("Couldn't serialize list of queries", e);
        }
    }
}
