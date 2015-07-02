package net.redborder.correlation.rest;

import javax.ws.rs.NotFoundException;
import java.util.List;
import java.util.Map;

public interface RestListener {
    void add(Map<String, Object> element) throws RestException;
    void remove(String id) throws NotFoundException, RestException;
    void synchronize(List<Map<String, Object>> elements) throws RestException;
    String list() throws RestException;
}