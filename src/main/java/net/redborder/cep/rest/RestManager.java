package net.redborder.cep.rest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;

import java.io.IOException;
import java.net.URI;

/**
 * This class manages the state of the REST HTTP API.
 * It is responsible for starting and stopping the HTTP server, and manages
 * listeners and events produced by the API.
 */
public class RestManager {
    private static final Logger log = LogManager.getLogger(RestManager.class);
    private static RestListener listener;
    private static HttpServer server;

    private RestManager() {}

    /**
     * Starts the HTTP server exposing the resources defined in the RestRules class.
     * It takes a reference to an object that implements the RestListener interface,
     * which will be notified every time the REST API receives a request of some type.
     *
     * @param wsUri The base URL of the HTTP REST API
     * @param rl The RestListener object that will be notified when a user sends
     *           an HTTP request to the API
     * @see RestRules
     * @see RestListener
     */

    public static void startServer(String wsUri, RestListener rl) {
        // Create a resource config that scans for JAX-RS resources and providers
        ResourceConfig rc = new ResourceConfig()
                .register(RestRules.class)
                .register(JacksonFeature.class);

        // Set the listener for the petitions
        listener = rl;

        // Create a new instance of grizzly http server
        // exposing the Jersey application at BASE_URI
        server = GrizzlyHttpServerFactory.createHttpServer(URI.create(wsUri), rc);

        // start the server
        try {
            server.start();
        } catch (IOException e) {
            log.error(e.getMessage());
        }

        log.info("HTTP server started");
    }

    /**
     * Returns true if the server is started
     *
     * @return true if the server is started, false otherwise
     */

    public static boolean isStarted() {
        return server.isStarted();
    }

    /**
     * Stops the HTTP server.
     */

    public static void stopServer() {
        if (server != null) {
            server.shutdown();
        }
    }

    /**
     * Returns the object that is acting as a listener
     *
     * @return The RestListener object that is been used as a listener
     */

    public static RestListener getListener() {
        return listener;
    }
}
