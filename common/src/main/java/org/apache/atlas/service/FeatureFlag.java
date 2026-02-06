package org.apache.atlas.service;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum FeatureFlag {
    ENABLE_JANUS_OPTIMISATION("ENABLE_JANUS_OPTIMISATION", true),
    ENABLE_PERSONA_HIERARCHY_FILTER("enable_persona_hierarchy_filter", false),
    DISABLE_WRITE_FLAG("disable_writes", false),
    USE_TEMP_ES_INDEX("use_temp_es_index", false);

    private final String key;
    private final boolean defaultValue;

    private static final Map<String, FeatureFlag> KEY_TO_FLAG_MAP = 
        Arrays.stream(values())
              .collect(Collectors.toMap(FeatureFlag::getKey, Function.identity()));

    FeatureFlag(String key, boolean defaultValue) {
        this.key = key;
        this.defaultValue = defaultValue;
    }

    public String getKey() {
        return key;
    }

    public boolean getDefaultValue() {
        return defaultValue;
    }

    public static FeatureFlag fromKey(String key) {
        return KEY_TO_FLAG_MAP.get(key);
    }

    public static boolean isValidFlag(String key) {
        return KEY_TO_FLAG_MAP.containsKey(key);
    }

    public static String[] getAllKeys() {
        return Arrays.stream(values())
                     .map(FeatureFlag::getKey)
                     .toArray(String[]::new);
    }
}
