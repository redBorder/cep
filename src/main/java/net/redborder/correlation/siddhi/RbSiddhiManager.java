package net.redborder.correlation.siddhi;

import net.redborder.correlation.rest.RestManager;
import net.redborder.correlation.rest.RestListener;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class RbSiddhiManager implements RestListener {
    private final Logger log = LoggerFactory.getLogger(RbSiddhiManager.class);

    private static SiddhiHandler siddhiHandler = new SiddhiHandler();
    private RestManager restManager = new RestManager();
    private Map<String, Object> query;
    private Map<String, Map<String, Object>> listOfMaps = new HashMap<>();

    private static RbSiddhiManager instance;

    public static SiddhiHandler getHandler() {
        return siddhiHandler;
    }

    public static synchronized RbSiddhiManager getInstance() {
        if (instance == null) {
            instance = new RbSiddhiManager();
        }

        return instance;
    }

    private RbSiddhiManager() { }

    @Override
    public boolean add(String newQuery) {
        ObjectMapper mapper = new ObjectMapper();
        boolean isAdd = false;

        try {
            query = mapper.readValue(newQuery, Map.class);
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (!listOfMaps.containsKey(query.get("id").toString())) {
            isAdd = true;
            listOfMaps.put(query.get("id").toString(), query);
            log.info("New query added: " + query.toString());
        } else {
            log.error("Query with id " + query.get("id") + " already exist");
        }

        return isAdd;
    }

    @Override
    public boolean remove(String id) {
        boolean isRemove = false;

        if (!listOfMaps.isEmpty()) {
            if (listOfMaps.containsKey(id)) {
                listOfMaps.remove(id);
                isRemove = true;
                log.info("Query with the id " + id + " has been removed");
                log.info(listOfMaps.toString());
            } else {
                log.error("Query with the id " + id + " is not present ");
            }
        } else {
            log.error("Query's map is empty");
        }

        return isRemove;
    }
}
