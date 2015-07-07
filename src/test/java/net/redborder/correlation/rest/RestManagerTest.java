package net.redborder.correlation.rest;

import junit.framework.TestCase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RestManagerTest extends TestCase {
    @Test
    public void startsAndStops() throws Exception {
        RestManager.startServer("http://localhost:1111/", null);
        assertTrue(RestManager.isStarted());
        RestManager.stopServer();
        Thread.sleep(1000);
        assertFalse(RestManager.isStarted());
    }
}
