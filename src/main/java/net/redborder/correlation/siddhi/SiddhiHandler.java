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

    private Map<String, ExecutionPlan> executionPlans;
    private SiddhiManager siddhiManager;
    private ObjectMapper mapper;

    public SiddhiHandler() {
        this.siddhiManager = new SiddhiManager();
        this.executionPlans = new HashMap<>();
        this.mapper = new ObjectMapper();
    }

    public void stop() {
        log.info("Stopping down execution plans...");

        for (ExecutionPlan executionPlan : executionPlans.values()) {
            executionPlan.stop();
        }
    }

    @Override
    public void onEvent(MapEvent mapEvent, long sequence, boolean endOfBatch) throws Exception {
        Map<String, Object> data = mapEvent.getData();
        String src = (String) data.get("src");
        String dst = (String) data.get("dst");
        String namespace = (String) data.get("namespace_uuid");
        Integer bytes = (Integer) data.get("bytes");

        for (ExecutionPlan executionPlan : executionPlans.values()) {
            executionPlan.getInputHandler().send(new Object[] { src, dst, namespace, bytes });
        }
    }

    @Override
    public void add(Map<String, Object> element) throws RestException {
        if (element.containsKey("id")) {
            String id = element.get("id").toString();

            if (executionPlans.containsKey(id)) {
                throw new RestException("query with id " + id + " already exists");
            } else {
                ExecutionPlan executionPlan = ExecutionPlan.fromMap(element);
                executionPlan.start(siddhiManager);
                executionPlans.put(executionPlan.getId(), executionPlan);
                log.info("New query added: {}", element);
            }
        } else {
            throw new RestException("invalid rule");
        }
    }

    @Override
    public void remove(String id) throws NotFoundException, RestException {
        ExecutionPlan executionPlan = executionPlans.get(id);

        if (executionPlan != null) {
            executionPlan.stop();
            executionPlans.remove(id);
            log.info("Query with the id {} has been removed", id);
            log.info("Current queries: {}", executionPlans.keySet());
        } else {
            throw new NotFoundException("Query with the id " + id + " is not present");
        }
    }

    @Override
    public void synchronize(List<Map<String, Object>> elements) throws RestException {
        // TODO
    }

    @Override
    public String list() throws RestException {
        List<Map<String, Object>> listQueries = new ArrayList<>();

        for (ExecutionPlan executionPlan : executionPlans.values()) {
            listQueries.add(executionPlan.toMap());
        }

        try {
            return mapper.writeValueAsString(listQueries);
        } catch (IOException e) {
            throw new RestException("Couldn't serialize list of queries", e);
        }
    }
}
