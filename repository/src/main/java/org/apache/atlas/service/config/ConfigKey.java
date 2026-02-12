package org.apache.atlas.service.config;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Enum defining valid dynamic configuration keys with their default values.
 *
 * This enum is used for validation - only predefined config keys are allowed
 * to be stored in the dynamic config store.
 *
 * Note: This is separate from FeatureFlag enum to maintain backward compatibility.
 * Feature flags in Redis continue to work as-is.
 */
public enum ConfigKey {
    // Maintenance mode flag
    MAINTENANCE_MODE("MAINTENANCE_MODE", "false"),

    // Maintenance mode activation tracking (set by tag propagation when it stops)
    MAINTENANCE_MODE_ACTIVATED_AT("MAINTENANCE_MODE_ACTIVATED_AT", null),
    MAINTENANCE_MODE_ACTIVATED_BY("MAINTENANCE_MODE_ACTIVATED_BY", null),

    MAINTENANCE_START_TIME("MAINTENANCE_START_TIME", null),
    MAINTENANCE_END_TIME("MAINTENANCE_END_TIME", null),

    // Tag/Janus optimization flag (mirrors existing feature flag)
    ENABLE_JANUS_OPTIMISATION("ENABLE_JANUS_OPTIMISATION", "true"),

    // Write disable flag for maintenance
    DISABLE_WRITE_FLAG("disable_writes", "false"),

    // Persona hierarchy filter flag
    ENABLE_PERSONA_HIERARCHY_FILTER("enable_persona_hierarchy_filter", "false"),

    // Temporary ES index usage flag
    USE_TEMP_ES_INDEX("use_temp_es_index", "false"),

    // Delete batch operations flag
    DELETE_BATCH_ENABLED("atlas.delete.batch.enabled", "false");

    private final String key;
    private final String defaultValue;

    private static final Map<String, ConfigKey> KEY_TO_CONFIG_MAP =
        Arrays.stream(values())
              .collect(Collectors.toMap(ConfigKey::getKey, Function.identity()));

    ConfigKey(String key, String defaultValue) {
        this.key = key;
        this.defaultValue = defaultValue;
    }

    public String getKey() {
        return key;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    /**
     * Get ConfigKey from string key.
     * @param key the config key string
     * @return ConfigKey or null if not found
     */
    public static ConfigKey fromKey(String key) {
        return KEY_TO_CONFIG_MAP.get(key);
    }

    /**
     * Check if a key is valid (exists in this enum).
     * @param key the config key string
     * @return true if valid, false otherwise
     */
    public static boolean isValidKey(String key) {
        return KEY_TO_CONFIG_MAP.containsKey(key);
    }

    /**
     * Get all valid config keys as string array.
     * @return array of all valid keys
     */
    public static String[] getAllKeys() {
        return Arrays.stream(values())
                     .map(ConfigKey::getKey)
                     .toArray(String[]::new);
    }
}
