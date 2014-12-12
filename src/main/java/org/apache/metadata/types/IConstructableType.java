package org.apache.metadata.types;


import org.apache.metadata.ITypedInstance;
import org.apache.metadata.MetadataException;

public interface IConstructableType<U, T extends ITypedInstance> extends IDataType<U> {

    T createInstance() throws MetadataException;
    FieldMapping fieldMapping();
}
