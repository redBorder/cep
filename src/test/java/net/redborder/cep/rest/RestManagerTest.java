package net.redborder.cep.rest;

import junit.framework.TestCase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
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
