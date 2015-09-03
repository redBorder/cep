package net.redborder.cep.siddhi;

import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.HashMap;
import java.util.Map;

/**
 * This class is an utility class for Siddhi
 */

public class SiddhiUtils {
    private static Map<String, Attribute.Type> typeMap = new HashMap<>();

    static {
        typeMap.put("string", Attribute.Type.STRING);
        typeMap.put("str", Attribute.Type.STRING);
        typeMap.put("integer", Attribute.Type.INT);
        typeMap.put("int", Attribute.Type.INT);
        typeMap.put("long", Attribute.Type.LONG);
        typeMap.put("float", Attribute.Type.FLOAT);
        typeMap.put("double", Attribute.Type.DOUBLE);
        typeMap.put("bool", Attribute.Type.BOOL);
        typeMap.put("object", Attribute.Type.OBJECT);
    }

    /**
     * Returns a attribute type from a string representing that attribute
     *
     * @param typeName The string representation of the attribute
     * @return A Attribute.Type from the given string
     * @see org.wso2.siddhi.query.api.definition.Attribute.Type
     */

    public static Attribute.Type typeOf(String typeName) {
        return typeMap.get(typeName);
    }
}
