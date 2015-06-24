package net.redborder.correlation.rest;

/**
 * Created by jmf on 24/06/15.
 */

import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONObject;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by jmf on 3/06/15.
 */
public class Queries {

    private ArrayList<Map<String, Object>> listOfMaps;

    public Queries() {
        this.listOfMaps = new ArrayList<Map<String, Object>>();
    }

    public boolean addQuery(String query)
    {

        ObjectMapper mapper = new ObjectMapper();
        boolean toAdd = true;


        try
        {
            //Creamos un mapa con la query recibida
            Map<String, Object>json = mapper.readValue(query, Map.class);


            ////Introducimos la query en la lista
            ////Comprobamos que el id no esté repetido
            for (Iterator<Map<String, Object>> it = listOfMaps.iterator(); it.hasNext();)
            {
                if(it.next().entrySet().iterator().next().getValue() == json.entrySet().iterator().next().getValue())
                {
                    toAdd = false;
                }

            }

            if(toAdd)
            {
                listOfMaps.add(json);
            }


        } catch (IOException e) {
            e.printStackTrace();
        }


        //Comprobamos que la query haya sido incluida correctamente en la lista
        for (Iterator<Map<String, Object>> it = listOfMaps.iterator(); it.hasNext();)
        {
            Logger.getLogger(Queries.class.getName()).log(Level.INFO, it.next().toString());
        }


        return toAdd;
    }

    public boolean deleteQuery(int id)
    {
        boolean isDelete = false;


        //Buscamos el id de la query en la lista y la eliminamos
        for (Iterator<Map<String, Object>> it = listOfMaps.iterator(); it.hasNext();)
        {
            if(it.next().entrySet().iterator().next().getValue() == id)
            {
                it.remove();
                isDelete = true;
            }

        }

        //Comprobamos que la query realmente ha sido eliminada
        for (Iterator<Map<String, Object>> it = listOfMaps.iterator(); it.hasNext();)
        {
            Logger.getLogger(Queries.class.getName()).log(Level.INFO, it.next().toString());

        }



        return isDelete;
    }


}