package org.apache.metadata.types;

import org.apache.metadata.types.DataTypes.TypeCategory;
import org.apache.metadata.MetadataException;

public interface IDataType<T> {
    String getName();
    T convert(Object val, Multiplicity m) throws MetadataException;
    TypeCategory getTypeCategory();
    void output(T val, Appendable buf, String prefix) throws MetadataException;
}
