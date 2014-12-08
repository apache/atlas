package org.apache.metadata.types;


import org.apache.metadata.MetadataException;

import java.io.IOException;

public class TypeUtils {

    public static void outputVal(String val, Appendable buf, String prefix) throws MetadataException {
        try {
            buf.append(prefix).append(val);
        } catch(IOException ie) {
            throw new MetadataException(ie);
        }
    }
}
