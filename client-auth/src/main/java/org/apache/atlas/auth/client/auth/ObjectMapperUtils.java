package org.apache.atlas.auth.client.auth;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Utility class providing shared ObjectMapper instances for auth clients.
 * Using static instances to avoid expensive ObjectMapper creation overhead.
 */
final class ObjectMapperUtils {

    /**
     * ObjectMapper configured for Keycloak clients - ignores unknown properties.
     * This handles cases where the Keycloak API returns additional fields not defined in client representation classes.
     * Example: Keycloak admin events returning "id" field not present in AdminEventRepresentation.
     */
    static final ObjectMapper KEYCLOAK_OBJECT_MAPPER = new ObjectMapper()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    /**
     * ObjectMapper configured for Heracles clients - ignores ignored properties.
     * This handles cases where JSON contains fields marked with @JsonIgnore in the target class.
     * Preserves the original behavior for Heracles client compatibility.
     */
    static final ObjectMapper HERACLES_OBJECT_MAPPER = new ObjectMapper()
            .disable(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES);

    private ObjectMapperUtils() {
        // Utility class - prevent instantiation
    }
}