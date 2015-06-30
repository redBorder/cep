package net.redborder.correlation.rest;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.URI;

public class RestManager {
    private static final Logger log = LoggerFactory.getLogger(RestManager.class);

    // Base URI the Grizzly HTTP server will listen on
    private static final String BASE_URI = "http://localhost:8888/myapp/";
    private static RestListener listener = null;
    private static ResourceConfig rc;
    private static HttpServer server;

    private RestManager() {}

    /**
     * Starts Grizzly HTTP server exposing JAX-RS resources defined in this application.
     * TODO: Get URI from ConfigData
     */
    public static void startServer(RestListener rl) {
        // create a resource config that scans for JAX-RS resources and providers
        // in com.example package
        rc = new ResourceConfig().packages("net.redborder.correlation.rest");

        // create a resource config that scans for JAX-RS resources and providers
        // in com.example package
        // rc = new ResourceConfig().packages("net.redborder.correlation.rest");

        // Set the listener for the petitions
        listener = rl;

        // create and start a new instance of grizzly http server
        // exposing the Jersey application at BASE_URI
        server = GrizzlyHttpServerFactory.createHttpServer(URI.create(BASE_URI), rc);
        log.info("Starting server...");
    }

    public static void stopServer() {
        if (server != null) {
            server.shutdown();
        }
    }

    public static RestListener getListener() {
        return listener;
    }
}
