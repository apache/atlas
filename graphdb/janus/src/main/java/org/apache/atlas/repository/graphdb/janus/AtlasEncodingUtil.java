package org.apache.atlas.repository.graphdb.janus;

import org.janusgraph.util.encoding.LongEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class AtlasEncodingUtil {

    public static String encodeJanusVertexIdToESDocId(Object vertex) {
        Objects.requireNonNull(vertex);
        return LongEncoding.encode(Long.parseLong(vertex.toString()));
    }

}