package net.redborder.correlation.siddhi;

/**
 * Created by jmf on 24/06/15.
 */
public interface RbSiddhiManagerInterface {

    boolean add(String newQuery);
    boolean remove(String id);
    //Futuras adiciones:
    // -eliminar todas las querys de golpe
}