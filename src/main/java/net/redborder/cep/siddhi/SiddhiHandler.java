package net.redborder.cep.siddhi;

import com.lmax.disruptor.EventHandler;
import net.redborder.cep.rest.RestListener;
import net.redborder.cep.rest.exceptions.RestException;
import net.redborder.cep.rest.exceptions.RestInvalidException;
import net.redborder.cep.rest.exceptions.RestNotFoundException;
import net.redborder.cep.siddhi.exceptions.AlreadyExistsException;
import net.redborder.cep.siddhi.exceptions.ExecutionPlanException;
import net.redborder.cep.siddhi.exceptions.InvalidExecutionPlanException;
import net.redborder.cep.sinks.SinksManager;
import net.redborder.cep.sinks.console.ConsoleSink;
import net.redborder.cep.sources.disruptor.MapEvent;
import net.redborder.cep.util.ConfigData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.query.compiler.exception.SiddhiParserException;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static net.redborder.cep.util.Constants.*;

/**
 * This class handles the state of the correlation engine, which is Siddhi.
 * It also manages the events coming from the HTTP API, and the messages produced by
 * the sources.
 *
 * @see RestListener
 * @see net.redborder.cep.sources.Source
 * @see SiddhiManager
 */

public class SiddhiHandler implements RestListener, EventHandler<MapEvent> {
    private final Logger log = LogManager.getLogger(SiddhiHandler.class);

    // The manager that we use as the interface to Siddhi
    private SiddhiManager siddhiManager;

    //
    private SiddhiCallback siddhiCallback;

    // An object to parse JSON messages
    private ObjectMapper objectMapper;

    // The list of siddhi plans that are currently running
    private Map<String, SiddhiPlan> siddhiPlans;

    /**
     * Creates a new Siddhi Handler.
     *
     * @param sinksManager The sink manager that will process the messages that Siddhi generates
     */

    public SiddhiHandler(SinksManager sinksManager) {
        this.siddhiManager = new SiddhiManager();
        this.siddhiCallback = new SiddhiCallback(sinksManager);
        this.objectMapper = new ObjectMapper();
        this.siddhiPlans = new HashMap<>();
    }

    /**
     * Creates a new Siddhi Handler with ConsoleSink as the sink.
     *
     * @see #SiddhiHandler()
     * @see ConsoleSink
     */

    public SiddhiHandler() {
        this(SinksManager.withConsoleSink());
    }

    /**
     * Stops all the siddhi plans that are currently running
     */

    public void stop() {
        log.info("Stopping siddhi plans...");

        for (SiddhiPlan siddhiPlan : siddhiPlans.values()) {
            siddhiPlan.stop();
        }
    }

    /**
     * Returns an unmodifiable map with the currently running siddhi plans.
     * The key of each entry is the siddhi plan id, and the value is an
     * SiddhiPlan object that describes that siddhi plan.
     *
     * @return Unmodifiable map of running siddhi plans
     */

    public Map<String, SiddhiPlan> getSiddhiPlans() {
        return Collections.unmodifiableMap(siddhiPlans);
    }

    /**
     * This method is called every time that a message is received by any
     * source that is currently started on the application.
     * <p>This method forwards each message received to every siddhi plan
     * that has marked the message's stream as an input stream.
     *
     * @param mapEvent   The message received from the source
     * @param sequence   unused
     * @param endOfBatch unused
     * @throws Exception if there is an error when an siddhi plans read the message
     */

    @Override
    public void onEvent(MapEvent mapEvent, long sequence, boolean endOfBatch) throws Exception {
        if (!siddhiPlans.isEmpty()) {
            Map<String, Object> data = mapEvent.getData();
            String key = mapEvent.getKey();
            String topic = mapEvent.getSource();
            for (SiddhiPlan siddhiPlan : siddhiPlans.values()) {
                if (siddhiPlan.getInput().contains(topic)) {
                    data.put(__KEY, key);
                    siddhiPlan.send(topic, data);
                }
            }
        }
    }

    /**
     * This method is called when the HTTP REST API receives an add request from a user.
     * It creates a new siddhi plan from the map given, and tries to add it.
     *
     * @param element The map representation of the JSON that the user provided.
     * @throws RestException If the map given does not follow the right siddhi plan schema, or if the
     *                       SiddhiPlan could not be added.
     * @see #add(SiddhiPlan)
     * @see SiddhiPlan
     */

    @Override
    public void add(Map<String, Object> element) throws RestException {
        try {
            SiddhiPlan siddhiPlan = SiddhiPlan.fromMap(element);
            add(siddhiPlan);
        } catch (ExecutionPlanException e) {
            throw new RestInvalidException(e.getMessage(), e);
        }
    }

    /**
     * This method adds a new siddhi plan.
     *
     * @param siddhiPlan The SiddhiPlan to be added
     * @throws ExecutionPlanException if there is a SiddhiPlan with an equal or greater version already running
     *                                or if the execution plan associated with the siddhi plan is invalid
     */

    public synchronized void add(SiddhiPlan siddhiPlan) throws ExecutionPlanException {
        // Check if there is a plan already running with that ID
        SiddhiPlan present = siddhiPlans.get(siddhiPlan.getId());
        boolean mustBeRemoved = false;

        // If it is already present, check the version.
        // If the version of the siddhi plan already running is equal or greater, then throw an exception
        if (present != null) {
            if (present.getVersion() >= siddhiPlan.getVersion()) {
                throw new AlreadyExistsException("siddhi plan with id " + siddhiPlan.getId() + " already exists with an equal or greater version");
            } else {
                mustBeRemoved = true;
            }
        }

        // Add the siddhi plan
        try {
            log.info("Adding new siddhi plan: {}", siddhiPlan.toMap());

            siddhiPlan.start(siddhiManager, siddhiCallback);
            siddhiPlans.put(siddhiPlan.getId(), siddhiPlan);
            save();

            if (mustBeRemoved) {
                present.stop();
            }
        } catch (SiddhiParserException e) {
            throw new InvalidExecutionPlanException("invalid siddhi execution plan", e);
        }
    }

    /**
     * Removes a siddhi plan with the id given.
     *
     * @param id The id of the siddhi plan that will be removed
     * @throws RestException if there is not siddhi plan with the id given
     */

    @Override
    public synchronized void remove(String id) throws RestException {
        SiddhiPlan siddhiPlan = siddhiPlans.get(id);

        if (siddhiPlan != null) {
            siddhiPlan.stop();
            siddhiPlans.remove(id);
            save();

            log.info("Siddhi plan with the id {} has been removed", id);
            log.info("Current queries: {}", siddhiPlans.keySet());
        } else {
            throw new RestNotFoundException("Siddhi plan with id " + id + " is not present");
        }
    }

    /**
     * Synchronizes the list of elements provided.
     *
     * @param elements A list of elements to synchronize
     * @throws RestException
     * @see RestListener#synchronize(List)
     */

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
        Set<String> presentExecutionPlansIds = new TreeSet<>(siddhiPlans.keySet());
        Set<String> newExecutionPlansIds = new TreeSet<>(newExecutionPlans.keySet());
        newExecutionPlansIds.retainAll(presentExecutionPlansIds);

        for (String id : newExecutionPlansIds) {
            SiddhiPlan present = siddhiPlans.get(id);
            SiddhiPlan newer = newExecutionPlans.get(id);

            if (present.getVersion() >= newer.getVersion()) {
                newExecutionPlans.remove(id);
                executionPlansToKeep.add(id);
            }
        }

        // Add the newer elements
        newExecutionPlansIds = new TreeSet<>(newExecutionPlans.keySet());

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
        presentExecutionPlansIds = new TreeSet<>(siddhiPlans.keySet());
        presentExecutionPlansIds.removeAll(executionPlansToKeep);

        for (String id : presentExecutionPlansIds) {
            remove(id);
        }

        // Save the new state
        save();
    }

    /**
     * @return A list of the currently running siddhi plans
     */

    @Override
    public List<Map<String, Object>> list() throws RestException {
        List<Map<String, Object>> listQueries = new ArrayList<>();

        for (SiddhiPlan siddhiPlan : siddhiPlans.values()) {
            listQueries.add(siddhiPlan.toMap());
        }

        return listQueries;
    }

    /**
     * Stores the list of currently running siddhi plans into an state file.
     * The path to the state file is provided by the config file.
     */

    public synchronized void save() {
        String stateFilePath = ConfigData.getStateFile();
        if (stateFilePath == null) return;

        List<Map<String, Object>> executionPlansList = new ArrayList<>();

        for (SiddhiPlan siddhiPlan : siddhiPlans.values()) {
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

    /**
     * Restores a list of siddhi plans from a state file provided by the config file.
     */

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
