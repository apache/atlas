package org.apache.metadata.types;


import org.apache.metadata.ITypedInstance;

public interface IConstructableType<U, T extends ITypedInstance> extends IDataType<U> {

    T createInstance();
    FieldMapping fieldMapping();
}
