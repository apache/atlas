package org.apache.hadoop.metadata.types;


import org.apache.hadoop.metadata.ITypedInstance;
import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.ITypedInstance;
import org.apache.hadoop.metadata.MetadataException;

import java.util.List;

public interface IConstructableType<U, T extends ITypedInstance> extends IDataType<U> {

    T createInstance() throws MetadataException;
    FieldMapping fieldMapping();
    List<String> getNames(AttributeInfo info);
}
