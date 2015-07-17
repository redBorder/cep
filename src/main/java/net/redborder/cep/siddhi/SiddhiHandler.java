package net.redborder.cep.siddhi;

import com.lmax.disruptor.EventHandler;
import net.redborder.cep.rest.RestListener;
import net.redborder.cep.rest.exceptions.RestException;
import net.redborder.cep.rest.exceptions.RestInvalidException;
import net.redborder.cep.rest.exceptions.RestNotFoundException;
import net.redborder.cep.sinks.console.ConsoleSink;
import net.redborder.cep.sinks.Sink;
import net.redborder.cep.siddhi.exceptions.AlreadyExistsException;
import net.redborder.cep.siddhi.exceptions.ExecutionPlanException;
import net.redborder.cep.siddhi.exceptions.InvalidExecutionPlanException;
import net.redborder.cep.sources.disruptor.MapEvent;
import net.redborder.cep.util.ConfigData;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.query.compiler.exception.SiddhiParserException;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class SiddhiHandler implements RestListener, EventHandler<MapEvent> {
    private final Logger log = LoggerFactory.getLogger(SiddhiHandler.class);

    private SiddhiManager siddhiManager;
    private SiddhiCallback siddhiCallback;
    private ObjectMapper objectMapper;
    private Map<String, SiddhiPlan> executionPlans;

    public SiddhiHandler(Sink eventReceiver) {
        this.siddhiManager = new SiddhiManager();
        this.siddhiCallback = new SiddhiCallback(eventReceiver);
        this.objectMapper = new ObjectMapper();
        this.executionPlans = new HashMap<>();
    }

    public SiddhiHandler() {
        this(new ConsoleSink());
    }

    public void stop() {
        log.info("Stopping execution plans...");

        for (SiddhiPlan siddhiPlan : executionPlans.values()) {
            siddhiPlan.stop();
        }
    }

    public Map<String, SiddhiPlan> getExecutionPlans() {
        return Collections.unmodifiableMap(executionPlans);
    }

    @Override
    public void onEvent(MapEvent mapEvent, long sequence, boolean endOfBatch) throws Exception {
        if (!executionPlans.isEmpty()) {
            Map<String, Object> data = mapEvent.getData();
            String topic = mapEvent.getSource();

            for (SiddhiPlan siddhiPlan : executionPlans.values()) {
                if (siddhiPlan.getInput().contains(topic)) {
                    siddhiPlan.send(topic, data);
                }
            }
        }
    }

    @Override
    public void add(Map<String, Object> element) throws RestException {
        try {
            SiddhiPlan siddhiPlan = SiddhiPlan.fromMap(element);
            add(siddhiPlan);
        } catch (ExecutionPlanException e) {
            throw new RestInvalidException(e.getMessage(), e);
        }
    }

    public synchronized void add(SiddhiPlan siddhiPlan) throws ExecutionPlanException {
        SiddhiPlan present = executionPlans.get(siddhiPlan.getId());
        boolean mustBeRemoved = false;

        if (present != null) {
            if (present.getVersion() >= siddhiPlan.getVersion()) {
                throw new AlreadyExistsException("execution plan with id " + siddhiPlan.getId() + " already exists with an equal or greater version");
            } else {
                mustBeRemoved = true;
            }
        }

        try {
            log.info("Adding new execution plan: {}", siddhiPlan.toMap());

            siddhiPlan.start(siddhiManager, siddhiCallback);
            executionPlans.put(siddhiPlan.getId(), siddhiPlan);
            save();

            if (mustBeRemoved) {
                present.stop();
            }
        } catch (SiddhiParserException e) {
            throw new InvalidExecutionPlanException("invalid siddhi execution plan", e);
        }
    }

    @Override
    public synchronized void remove(String id) throws RestException {
        SiddhiPlan siddhiPlan = executionPlans.get(id);

        if (siddhiPlan != null) {
            siddhiPlan.stop();
            executionPlans.remove(id);
            save();

            log.info("Execution plan with the id {} has been removed", id);
            log.info("Current queries: {}", executionPlans.keySet());
        } else {
            throw new RestNotFoundException("Execution plan with id " + id + " is not present");
        }
    }

    @Override
    public synchronized void synchronize(List<Map<String, Object>> elements) throws RestException {
        Map<String, SiddhiPlan> newExecutionPlans = new HashMap<>();
        Set<String> executionPlansToKeep = new TreeSet<>();

        // Check that all the execution plans are valid
        for (Map<String, Object> element : elements) {
            try {
                SiddhiPlan siddhiPlan = SiddhiPlan.fromMap(element);
                if (!newExecutionPlans.containsKey(siddhiPlan.getId())) {
                    siddhiManager.validateExecutionPlan(siddhiPlan.getFullExecutionPlan());
                    newExecutionPlans.put(siddhiPlan.getId(), siddhiPlan);
                } else {
                    throw new AlreadyExistsException("execution plans with id " + siddhiPlan.getId() + " was specified twice");
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
            SiddhiPlan present = executionPlans.get(id);
            SiddhiPlan newer = executionPlans.get(id);

            if (present.getVersion() >= newer.getVersion()) {
                newExecutionPlans.remove(id);
                executionPlansToKeep.add(id);
            }
        }

        // Add the newer elements
        newExecutionPlansIds = new TreeSet<>(newExecutionPlans.keySet());
        newExecutionPlansIds.removeAll(presentExecutionPlansIds);

        for (String id : newExecutionPlansIds) {
            SiddhiPlan newer = newExecutionPlans.get(id);

            try {
                add(newer);
                executionPlansToKeep.add(id);
            } catch (ExecutionPlanException e) {
                throw new RestInvalidException(e.getMessage(), e);
            }
        }

        // Now remove the ones that are not necessary anymore
        presentExecutionPlansIds = new TreeSet<>(executionPlans.keySet());
        presentExecutionPlansIds.removeAll(executionPlansToKeep);

        for (String id : presentExecutionPlansIds) {
           remove(id);
        }

        // Save the new state
        save();
    }

    @Override
    public List<Map<String, Object>> list() throws RestException {
        List<Map<String, Object>> listQueries = new ArrayList<>();

        for (SiddhiPlan siddhiPlan : executionPlans.values()) {
            listQueries.add(siddhiPlan.toMap());
        }

        return listQueries;
    }

    public synchronized void save() {
        String stateFilePath = ConfigData.getStateFile();
        if (stateFilePath == null) return;

        List<Map<String, Object>> executionPlansList = new ArrayList<>();

        for (SiddhiPlan siddhiPlan : executionPlans.values()) {
            Map<String, Object> executionPlanMap = siddhiPlan.toMap();
            executionPlansList.add(executionPlanMap);
        }

        try {
            String listString = objectMapper.writeValueAsString(executionPlansList);
            PrintWriter writer = new PrintWriter(stateFilePath, "UTF-8");
            writer.print(listString);
            writer.close();
        } catch (IOException e) {
            log.error("Couldn't write the state file");
        }
    }

    public synchronized void restore() {
        String stateFilePath = ConfigData.getStateFile();
        if (stateFilePath == null) return;

        try {
            log.info("Restoring state from file {}", stateFilePath);
            Path path = Paths.get(stateFilePath);
            byte[] bytes = Files.readAllBytes(path);
            String contents = new String(bytes, "UTF-8");
            List<Map<String, Object>> executionPlanList = objectMapper.readValue(contents, List.class);
            synchronize(executionPlanList);
        } catch (IOException e) {
            log.warn("Couldn't read the state file");
        } catch (RestException e) {
            log.error("Couldn't synchronize the state file", e);
        }
    }
}
