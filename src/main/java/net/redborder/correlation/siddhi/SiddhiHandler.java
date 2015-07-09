package net.redborder.correlation.siddhi;

import com.lmax.disruptor.EventHandler;
import net.redborder.correlation.kafka.disruptor.MapEvent;
import net.redborder.correlation.receivers.ConsoleReceiver;
import net.redborder.correlation.receivers.EventReceiver;
import net.redborder.correlation.rest.RestListener;
import net.redborder.correlation.rest.exceptions.RestException;
import net.redborder.correlation.rest.exceptions.RestInvalidException;
import net.redborder.correlation.rest.exceptions.RestNotFoundException;
import net.redborder.correlation.siddhi.exceptions.AlreadyExistsException;
import net.redborder.correlation.siddhi.exceptions.ExecutionPlanException;
import net.redborder.correlation.siddhi.exceptions.InvalidExecutionPlanException;
import net.redborder.correlation.util.ConfigData;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.extension.timeseries.LinearRegressionForecastStreamProcessor;
import org.wso2.siddhi.query.compiler.exception.SiddhiParserException;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class SiddhiHandler implements RestListener, EventHandler<MapEvent> {
    private final Logger log = LoggerFactory.getLogger(SiddhiHandler.class);

    private Map<String, ExecutionPlan> executionPlans;
    private SiddhiManager siddhiManager;
    private SiddhiCallback siddhiCallback;
    private ObjectMapper objectMapper;

    public SiddhiHandler(EventReceiver eventReceiver) {
        this.siddhiCallback = new SiddhiCallback(eventReceiver);
        this.executionPlans = new HashMap<>();
        this.objectMapper = new ObjectMapper();
        this.siddhiManager = new SiddhiManager();
        this.siddhiManager.setExtension("timeseries:forecast", LinearRegressionForecastStreamProcessor.class);
    }

    public SiddhiHandler() {
        this(new ConsoleReceiver());
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
            Integer pkts = (Integer) data.get("pkts");

            for (ExecutionPlan executionPlan : executionPlans.values()) {
                executionPlan.send(new Object[]{src, dst, namespace, bytes, pkts});
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
        ExecutionPlan present = executionPlans.get(executionPlan.getId());

        if (present != null) {
            if (present.getVersion() >= executionPlan.getVersion()) {
                throw new AlreadyExistsException("execution plan with id " + executionPlan.getId() + " already exists with an equal or greater version");
            } else {
                try {
                    remove(executionPlan.getId());
                } catch (RestException e) {
                    log.error("Weird! Execution plan id {} does not exists but it should!", executionPlan.getId(), e);
                }
            }
        }

        try {
            executionPlan.start(siddhiManager, siddhiCallback);
            executionPlans.put(executionPlan.getId(), executionPlan);
            save();

            log.info("New execution plan added: {}", executionPlan.toMap());
        } catch (SiddhiParserException e) {
            throw new InvalidExecutionPlanException("invalid siddhi execution plan", e);
        }
    }

    @Override
    public void remove(String id) throws RestException {
        ExecutionPlan executionPlan = executionPlans.get(id);

        if (executionPlan != null) {
            executionPlan.stop();
            executionPlans.remove(id);
            save();

            log.info("Execution plan with the id {} has been removed", id);
            log.info("Current queries: {}", executionPlans.keySet());
        } else {
            throw new RestNotFoundException("Execution plan with id " + id + " is not present");
        }
    }

    @Override
    public void synchronize(List<Map<String, Object>> elements) throws RestException {
        Map<String, ExecutionPlan> newExecutionPlans = new HashMap<>();
        Set<String> executionPlanstoKeep = new TreeSet<>();

        // Check that all the execution plans are valid
        for (Map<String, Object> element : elements) {
            try {
                ExecutionPlan executionPlan = ExecutionPlan.fromMap(element);
                if (!newExecutionPlans.containsKey(executionPlan.getId())) {
                    siddhiManager.validateExecutionPlan(executionPlan.getFullExecutionPlan());
                    newExecutionPlans.put(executionPlan.getId(), executionPlan);
                } else {
                    throw new AlreadyExistsException("execution plans with id " + executionPlan.getId() + " was specified twice");
                }
            } catch (ExecutionPlanException e) {
                throw new RestInvalidException(e.getMessage(), e);
            } catch (SiddhiParserException e) {
                throw new RestInvalidException("invalid siddhi rule: " + e.getMessage(), e);
            }
        }

        // Remove the execution plans that are already present and with an equal or greater version
        Set<String> presentExecutionPlansIds = new TreeSet<>(executionPlans.keySet());
        Set<String> newExecutionPlansIds = new TreeSet<>(newExecutionPlans.keySet());
        newExecutionPlansIds.retainAll(presentExecutionPlansIds);

        for (String id : newExecutionPlansIds) {
            ExecutionPlan present = executionPlans.get(id);
            ExecutionPlan newer = executionPlans.get(id);

            if (present.getVersion() >= newer.getVersion()) {
                newExecutionPlans.remove(id);
                executionPlanstoKeep.add(id);
            }
        }

        // Add the newer elements
        newExecutionPlansIds = new TreeSet<>(newExecutionPlans.keySet());
        newExecutionPlansIds.removeAll(presentExecutionPlansIds);

        for (String id : newExecutionPlansIds) {
            ExecutionPlan newer = newExecutionPlans.get(id);

            try {
                add(newer);
                executionPlanstoKeep.add(id);
            } catch (ExecutionPlanException e) {
                throw new RestInvalidException(e.getMessage(), e);
            }
        }

        // Now remove the ones that are not necessary anymore
        presentExecutionPlansIds = new TreeSet<>(executionPlans.keySet());
        presentExecutionPlansIds.removeAll(executionPlanstoKeep);

        for (String id : presentExecutionPlansIds) {
           remove(id);
        }

        // Save the new state
        save();
    }

    @Override
    public List<Map<String, Object>> list() throws RestException {
        List<Map<String, Object>> listQueries = new ArrayList<>();

        for (ExecutionPlan executionPlan : executionPlans.values()) {
            listQueries.add(executionPlan.toMap());
        }

        return listQueries;
    }

    public void save() {
        String stateFilePath = ConfigData.getStateFile();
        if (stateFilePath == null) return;

        List<Map<String, Object>> executionPlansList = new ArrayList<>();

        for (ExecutionPlan executionPlan : executionPlans.values()) {
            Map<String, Object> executionPlanMap = executionPlan.toMap();
            executionPlansList.add(executionPlanMap);
        }

        try {
            String listString = objectMapper.writeValueAsString(executionPlansList);
            PrintWriter writer = new PrintWriter(stateFilePath, "UTF-8");
            writer.print(listString);
            writer.close();
        } catch (IOException e) {
            log.debug("Couldn't write the state file", e);
        }
    }

    public void restore() {
        String stateFilePath = ConfigData.getStateFile();
        if (stateFilePath == null) return;

        try {
            Path path = Paths.get(stateFilePath);
            byte[] bytes = Files.readAllBytes(path);
            String contents = new String(bytes, "UTF-8");
            List<Map<String, Object>> executionPlanList = objectMapper.readValue(contents, List.class);
            synchronize(executionPlanList);
        } catch (IOException e) {
            log.error("Couldn't read the state file", e);
        } catch (RestException e) {
            log.error("Couldn't synchronize the state file", e);
        }
    }
}
