package org.apache.metadata.storage;


public class Id {

    public static final int UNASSIGNED = -1;

    public final int id;
    public final String className;

    public Id(int id, String className) {
        this.id = id;
        this.className = className;
    }

    public Id(String className) {
        this(UNASSIGNED, className);
    }

    public boolean isUnassigned() {
        return id == UNASSIGNED;
    }
}
