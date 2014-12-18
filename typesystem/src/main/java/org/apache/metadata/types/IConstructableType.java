package org.apache.metadata.types;


import org.apache.metadata.ITypedInstance;
import org.apache.metadata.MetadataException;

import java.util.List;

public interface IConstructableType<U, T extends ITypedInstance> extends IDataType<U> {

    T createInstance() throws MetadataException;
    FieldMapping fieldMapping();
    List<String> getNames(AttributeInfo info);
}
