package net.redborder.correlation.siddhi;

public class RbSiddhiManager {
    private static SiddhiHandler siddhiHandler = new SiddhiHandler();

    public static SiddhiHandler getHandler(){
        return siddhiHandler;
    }
}
