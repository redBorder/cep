package net.redborder.correlation.siddhi;

import com.lmax.disruptor.EventHandler;
import net.redborder.correlation.kafka.disruptor.MapEvent;
import net.redborder.correlation.rest.RestListener;
import net.redborder.correlation.rest.exceptions.RestException;
import net.redborder.correlation.rest.exceptions.RestInvalidException;
import net.redborder.correlation.rest.exceptions.RestNotFoundException;
import net.redborder.correlation.siddhi.exceptions.AlreadyExistsException;
import net.redborder.correlation.siddhi.exceptions.ExecutionPlanException;
import net.redborder.correlation.siddhi.exceptions.InvalidExecutionPlanException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.query.compiler.exception.SiddhiParserException;

import java.util.*;

public class SiddhiHandler implements RestListener, EventHandler<MapEvent> {
    private final Logger log = LoggerFactory.getLogger(SiddhiHandler.class);

    private Map<String, ExecutionPlan> executionPlans;
    private SiddhiManager siddhiManager;

    public SiddhiHandler() {
        this.siddhiManager = new SiddhiManager();
        this.executionPlans = new HashMap<>();
    }

    public void stop() {
        log.info("Stopping down execution plans...");

        for (ExecutionPlan executionPlan : executionPlans.values()) {
            executionPlan.stop();
        }
    }

    public Map<String, ExecutionPlan> getExecutionPlans() {
        return Collections.unmodifiableMap(executionPlans);
    }

    @Override
    public void onEvent(MapEvent mapEvent, long sequence, boolean endOfBatch) throws Exception {
        if (!executionPlans.isEmpty()) {
            Map<String, Object> data = mapEvent.getData();
            String src = (String) data.get("src");
            String dst = (String) data.get("dst");
            String namespace = (String) data.get("namespace_uuid");
            Integer bytes = (Integer) data.get("bytes");

            for (ExecutionPlan executionPlan : executionPlans.values()) {
                executionPlan.getInputHandler().send(new Object[]{src, dst, namespace, bytes});
            }
        }
    }

    @Override
    public void add(Map<String, Object> element) throws RestException {
        try {
            ExecutionPlan executionPlan = ExecutionPlan.fromMap(element);
            add(executionPlan);
        } catch (ExecutionPlanException e) {
            throw new RestInvalidException(e.getMessage(), e);
        }
    }

    public void add(ExecutionPlan executionPlan) throws ExecutionPlanException {
        if (executionPlans.containsKey(executionPlan.getId())) {
            throw new AlreadyExistsException("execution plan with id " + executionPlan.getId() + " already exists");
        } else {
            try {
                executionPlan.start(siddhiManager);
                executionPlans.put(executionPlan.getId(), executionPlan);
                log.info("New execution plan added: {}", executionPlan.toMap());
            } catch (SiddhiParserException e) {
                throw new InvalidExecutionPlanException("invalid siddhi execution plan", e);
            }
        }
    }

    @Override
    public void remove(String id) throws RestException {
        ExecutionPlan executionPlan = executionPlans.get(id);

        if (executionPlan != null) {
            executionPlan.stop();
            executionPlans.remove(id);
            log.info("Execution plan with the id {} has been removed", id);
            log.info("Current queries: {}", executionPlans.keySet());
        } else {
            throw new RestNotFoundException("Execution plan with id " + id + " is not present");
        }
    }

    @Override
    public void synchronize(List<Map<String, Object>> elements) throws RestException {
        Map<String, ExecutionPlan> newExecutionPlans = new HashMap<>();

        for (Map<String, Object> element : elements) {
            try {
                ExecutionPlan executionPlan = ExecutionPlan.fromMap(element);

                if (newExecutionPlans.containsKey(executionPlan.getId())) {
                    throw new RestInvalidException("you can't specify two or more execution plans with the same id");
                }

                executionPlan.start(siddhiManager);
                newExecutionPlans.put(executionPlan.getId(), executionPlan);
            } catch (ExecutionPlanException e) {
                throw new RestInvalidException(e.getMessage(), e);
            } catch (SiddhiParserException e) {
                throw new RestInvalidException("invalid siddhi rule: " + e.getMessage(), e);
            }
        }

        executionPlans = newExecutionPlans;
    }

    @Override
    public List<Map<String, Object>> list() throws RestException {
        List<Map<String, Object>> listQueries = new ArrayList<>();

        for (ExecutionPlan executionPlan : executionPlans.values()) {
            listQueries.add(executionPlan.toMap());
        }

        return listQueries;
    }
}
