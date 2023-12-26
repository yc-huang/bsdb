package tech.bsdb.serde;

import java.io.Serializable;

public class Field implements Serializable {
    public String name;
    public boolean isPrimitive;
    public Type type;

    public Type kType, vType;

    public enum Type {
        STRING,
        INT,
        FLOAT,
        LONG,
        DOUBLE,
        BOOLEAN,
        MAP,
        LIST,
        UNSUPPORTED
    }

}
