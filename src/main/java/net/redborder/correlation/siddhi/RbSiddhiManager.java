package net.redborder.correlation.siddhi;

/**
 * Date: 3/6/15 17:39.
 */
public class RbSiddhiManager {

    private static SiddhiHandler siddhiHandler;

    public static void init(){
        siddhiHandler = new SiddhiHandler();
    }

    public static SiddhiHandler getHandler(){
        return siddhiHandler;
    }

}
