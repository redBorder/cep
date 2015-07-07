package net.redborder.correlation.rest;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

public class RestManager {
    private static final Logger log = LoggerFactory.getLogger(RestManager.class);
    private static RestListener listener;
    private static HttpServer server;

    private RestManager() {}

    /**
     * Starts Grizzly HTTP server exposing JAX-RS resources defined in this application.
     */
    public static void startServer(String wsUri, RestListener rl) {
        // create a resource config that scans for JAX-RS resources and providers
        ResourceConfig rc = new ResourceConfig()
                .register(Resource.class)
                .register(JacksonFeature.class);

        // Set the listener for the petitions
        listener = rl;

        // create a new instance of grizzly http server
        // exposing the Jersey application at BASE_URI
        server = GrizzlyHttpServerFactory.createHttpServer(URI.create(wsUri), rc);

        // start the server
        log.info("Starting server...");

        try {
            server.start();
        } catch (IOException e) {
            log.error(e.getMessage());
        }

        log.info("HTTP server started");
    }

    public static boolean isStarted() {
        return server.isStarted();
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
