package net.redborder.correlation.rest;

public interface RestListener {
    boolean add(String newQuery);
    boolean remove(String id);
    // TODO: add removeAll method
}