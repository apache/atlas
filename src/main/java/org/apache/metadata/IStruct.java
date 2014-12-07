package org.apache.metadata;

public interface IStruct {

    String getTypeName();
    Object get(String attrName) throws MetadataException;
    void set(String attrName, Object val) throws MetadataException;
}
