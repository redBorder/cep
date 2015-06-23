package net.redborder.correlation.siddhi;

import com.lmax.disruptor.EventHandler;
import net.redborder.correlation.kafka.disruptor.MapEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.util.Map;

public class SiddhiHandler implements EventHandler<MapEvent> {
    private static final Logger log = LoggerFactory.getLogger(SiddhiHandler.class);
    private InputHandler inputHandler;

    public SiddhiHandler() {
        SiddhiManager siddhiManager = new SiddhiManager();

        String executionPlan = "@config(async = 'true')define stream testStream (src string, dst string, bytes int);" +
            "@info(name = 'queryTest') from testStream[bytes > 2990] select src,dst insert into outputTestStream;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);

        executionPlanRuntime.addCallback("queryTest", new QueryCallback() {
            @Override
            public void receive(long l, Event[] events, Event[] events1) {
                log.info("Alert with more than 2990 bytes!!! {}", l);
            }
        });

        executionPlanRuntime.start();
        inputHandler = executionPlanRuntime.getInputHandler("testStream");
    }


    @Override
    public void onEvent(MapEvent mapEvent, long sequence, boolean endOfBatch) throws Exception {
        Map<String, Object> data = mapEvent.getData();
        String src = (String) data.get("src");
        String dst = (String) data.get("dst");
        Integer bytes = (Integer) data.get("bytes");
        inputHandler.send(new Object[] { src, dst, bytes });
    }
}
