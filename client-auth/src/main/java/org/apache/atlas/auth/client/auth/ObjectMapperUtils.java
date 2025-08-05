package org.apache.atlas.auth.client.auth;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;


final class ObjectMapperUtils {

    static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private ObjectMapperUtils() {
        // Utility class - prevent instantiation
    }
}