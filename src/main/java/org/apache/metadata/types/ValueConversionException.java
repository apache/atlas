package org.apache.metadata.types;

import org.apache.metadata.MetadataException;

public class ValueConversionException  extends MetadataException {

    public ValueConversionException(IDataType typ, Object val) {
        this(typ, val, (Throwable) null);
    }

    public ValueConversionException(IDataType typ, Object val, Throwable t) {
        super(String.format("Cannot convert value '%s' to datatype %s", val.toString(), typ.getName()), t);
    }

    public ValueConversionException(IDataType typ, Object val, String msg) {
        super(String.format("Cannot convert value '%s' to datatype %s because: %s",
                val.toString(), typ.getName(), msg));
    }

    protected ValueConversionException(String msg) {
        super(msg);
    }

    public static class NullConversionException  extends ValueConversionException {
        public NullConversionException(Multiplicity m) {
            super(String.format("Null value not allowed for multiplicty %s", m));
        }

    }
}
