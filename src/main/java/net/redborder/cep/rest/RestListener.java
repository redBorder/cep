package net.redborder.cep.rest;

import net.redborder.cep.rest.exceptions.RestException;

import java.util.List;
import java.util.Map;

/**
 * This interface declares the set of methods that a class needs to implement
 * in order to receive events coming from the REST HTTP API. Each one of the methods
 * in the interface are mapped to a HTTP method available for clients.
 * <p>For example, when a client sends an add request to the API, the REST HTTP API
 * process that request, and forwards the petition to the RestListener object, invoking
 * the add method of the interface and passing the user-provided JSON as the parameter of
 * the method.
 */

public interface RestListener {

    /**
     * Adds a new element.
     *
     * @param element The map representation of the JSON that the user provided.
     * @throws RestException If there was an error processing the request.
     */

    void add(Map<String, Object> element) throws RestException;

    /**
     * Removes an element.
     *
     * @param id The user-provided id of the element that will be removed
     * @throws RestException If there was an error processing the request.
     */

    void remove(String id) throws RestException;

    /**
     * Synchronizes a list of elements.
     * <p>When a list of elements are synchronized, the system must guarantee that
     * the list of elements provided will be added to the system, and that any
     * element previously present on the system but not present on the list of elements
     * to synchronize are removed.
     *
     * @param elements A list of elements to synchronize
     * @throws RestException If there was an error processing the request.
     */

    void synchronize(List<Map<String, Object>> elements) throws RestException;

    /**
     * Returns a list of the elements present on the system.
     *
     * @return A list with the elements present on the system represented as maps.
     * @throws RestException If there was an error processing the request.
     */

    List<Map<String, Object>> list() throws RestException;
}