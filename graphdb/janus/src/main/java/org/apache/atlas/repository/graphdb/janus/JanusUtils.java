package org.apache.atlas.repository.graphdb.janus;

import org.janusgraph.util.encoding.LongEncoding;

import javax.annotation.Nonnull;
import java.util.Objects;

public class JanusUtils {

    public static String toLongEncoding(Object vertex) {
        Objects.requireNonNull(vertex);
        return LongEncoding.encode(Long.parseLong(vertex.toString()));
    }

}