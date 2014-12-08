package org.apache.metadata.types;

import org.apache.metadata.MetadataException;

import java.io.IOException;

abstract class AbstractDataType<T> implements IDataType<T> {

    protected T convertNull( Multiplicity m) throws MetadataException {
        if (!m.nullAllowed() ) {
            throw new ValueConversionException.NullConversionException(m);
        }
        return null;
    }

    @Override
    public void output(T val, Appendable buf, String prefix) throws MetadataException {
        TypeUtils.outputVal(val == null ? "<null>" : val.toString(), buf, prefix);
    }
}

