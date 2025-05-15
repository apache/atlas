package org.apache.atlas.repository.store.graph.v2.utils;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.type.AtlasArrayType;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasMapType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class for mapping classification attributes in a consistent format for v1 and v2.
 * Used to ensure that classifications stored in Cassandra via v2 flow maintain the same
 * structure as classifications stored in JanusGraph via v1 flow.
 */
@Component
public class TagAttributeMapper {
    private static final Logger LOG = LoggerFactory.getLogger(TagAttributeMapper.class);

    private final AtlasTypeRegistry typeRegistry;

    @Inject
    public TagAttributeMapper(AtlasTypeRegistry typeRegistry) {
        this.typeRegistry = typeRegistry;
    }

    /**
     * Maps the attributes of a classification to a normalized structure, ensuring all defined attributes
     * are present with appropriate default values based on their type definitions.
     * To be used in any read path for attributes.
     *
     * @param classification The classification whose attributes need to be mapped
     * @param typeRegistry The type registry to look up type definitions
     * @return Updated classification with properly mapped attributes and default values
     * @throws AtlasBaseException If any error occurs during the mapping process
     */
    public static AtlasClassification mapClassificationAttributesWithDefaults(AtlasClassification classification, AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
        if (classification == null) {
            return null;
        }

        AtlasClassification result = classification.deepCopy();
        String typeName = classification.getTypeName();

        if (StringUtils.isEmpty(typeName)) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_NOT_FOUND, "Classification typeName is empty");
        }

        AtlasClassificationType classificationType = typeRegistry.getClassificationTypeByName(typeName);
        if (classificationType == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_NOT_FOUND, typeName);
        }

        // Get all defined attributes from the type
        Map<String, AtlasAttribute> typeAttributes = classificationType.getAllAttributes();
        if (MapUtils.isEmpty(typeAttributes)) {
            return result;
        }

        Map<String, Object> attributes = result.getAttributes();
        if (attributes == null) {
            attributes = new HashMap<>();
            result.setAttributes(attributes);
        }

        // Process each attribute defined in the type
        for (AtlasAttribute attribute : typeAttributes.values()) {
            String attrName = attribute.getName();
            Object attrValue = attributes.get(attrName);

            // If attribute value is null or empty, set appropriate default based on type
            if (attrValue == null) {
                AtlasType attrType = attribute.getAttributeType();
                if (attrType != null) {
                    switch (attrType.getTypeCategory()) {
                        case PRIMITIVE, ENUM -> attributes.put(attrName, null);
                        case ARRAY -> attributes.put(attrName, new ArrayList<>());
                        case MAP -> attributes.put(attrName, new HashMap<>());
                        case STRUCT -> {
                            AtlasStructType structType = (AtlasStructType) attrType;
                            Map<String, Object> structInstance = new HashMap<>();
                            structInstance.put("typeName", structType.getTypeName());
                            structInstance.put("attributes", new HashMap<>());
                            attributes.put(attrName, structInstance);
                        }
                        default -> {
                            LOG.warn("Unsupported attribute type {}. Using null as default.", attrType.getTypeCategory());
                            attributes.put(attrName, null);
                        }
                    }
                }
            }
        }

        return result;
    }


    /**
     * Maps the attributes of a classification to a normalized structure, similar to how
     * they would be mapped in tag vertex in old v1 tags flow.
     * To be used in any write path for attributes.
     * 
     * @param classification The classification whose attributes need to be mapped
     * @return Updated classification with properly mapped attributes
     * @throws AtlasBaseException If any error occurs during the mapping process
     */
    public AtlasClassification mapClassificationAttributes(AtlasClassification classification) throws AtlasBaseException {
        if (classification == null) {
            return null;
        }

        AtlasClassification result = classification.deepCopy();
        String typeName = classification.getTypeName();
        
        if (StringUtils.isEmpty(typeName)) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_NOT_FOUND, "Classification typeName is empty");
        }

        AtlasClassificationType classificationType = typeRegistry.getClassificationTypeByName(typeName);
        if (classificationType == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_NOT_FOUND, typeName);
        }

        Map<String, Object> attributes = result.getAttributes();
        if (MapUtils.isEmpty(attributes)) {
            return result;
        }

        Map<String, Object> mappedAttributes = new HashMap<>();
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            String attrName = entry.getKey();
            Object attrValue = entry.getValue();

            AtlasAttribute attribute = classificationType.getAttribute(attrName);
            if (attribute == null) {
                LOG.warn("Attribute {}.{} not found in type definition. Skipping.", typeName, attrName);
                continue;
            }

            Object mappedValue = mapAttributeValue(attribute, attrValue);
            if (mappedValue != null) {
                mappedAttributes.put(attrName, mappedValue);
            }
        }

        result.setAttributes(mappedAttributes);
        return result;
    }

    /**
     * Maps a single attribute value based on its type.
     */
    private Object mapAttributeValue(AtlasAttribute attribute, Object value) throws AtlasBaseException {
        if (value == null) {
            return null;
        }

        AtlasType attrType = attribute.getAttributeType();
        if (attrType == null) {
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, "Attribute type is null for " + attribute.getName());
        }

        return switch (attrType.getTypeCategory()) {
            case PRIMITIVE, ENUM -> value;
            case ARRAY -> mapArrayValue(attribute, value);
            case MAP -> mapMapValue(attribute, value);
            case STRUCT -> mapStructValue(attribute, value);
            default -> {
                LOG.warn("Unsupported attribute type {}. Using original value.", attrType.getTypeCategory());
                yield value;
            }
        };
    }

    /**
     * Maps an array value to ensure proper structure with typeName fields for struct elements.
     */
    @SuppressWarnings("unchecked")
    private Object mapArrayValue(AtlasAttribute attribute, Object value) throws AtlasBaseException {
        if (!(value instanceof List<?>)) {
            LOG.warn("Expected a List for array attribute {}, but found: {}", attribute.getName(), value.getClass().getName());
            return value;
        }

        List<Object> valueList = (List<Object>) value;
        if (CollectionUtils.isEmpty(valueList)) {
            return valueList;
        }

        AtlasArrayType arrayType = (AtlasArrayType) attribute.getAttributeType();
        AtlasType elementType = arrayType.getElementType();
        TypeCategory elementTypeCategory = elementType.getTypeCategory();

        List<Object> mappedList = new ArrayList<>(valueList.size());
        for (Object element : valueList) {
            if (element == null) {
                mappedList.add(null);
                continue;
            }

            switch (elementTypeCategory) {
                case PRIMITIVE, ENUM -> mappedList.add(element);

                case STRUCT -> {
                    if (element instanceof Map<?, ?> structMap) {
                        mappedList.add(buildStructInstance((AtlasStructType) elementType, (Map<String, Object>) structMap));
                    } else {
                        LOG.warn("Expected a Map for struct element in array attribute {}, found: {}", attribute.getName(), element.getClass().getName());
                        mappedList.add(element);
                    }
                }

                case ARRAY, MAP -> {
                    AtlasAttribute nestedAttr = new AtlasAttribute(attribute.getDefinedInType(),
                            new AtlasAttributeDef(attribute.getName(), elementType.getTypeName()), elementType);
                    mappedList.add(mapAttributeValue(nestedAttr, element));
                }

                default -> {
                    LOG.warn("Unhandled element type {} for array attribute {}", elementTypeCategory, attribute.getName());
                    mappedList.add(element);
                }
            }
        }

        return mappedList;
    }

    /**
     * Maps a map value to ensure proper structure with typeName fields for struct values.
     */
    @SuppressWarnings("unchecked")
    private Object mapMapValue(AtlasAttribute attribute, Object value) throws AtlasBaseException {
        if (!(value instanceof Map<?, ?> valueMap)) {
            LOG.warn("Expected a Map for map attribute {}, but found: {}", attribute.getName(), value.getClass().getName());
            return value;
        }

        if (valueMap.isEmpty()) return valueMap;

        AtlasMapType mapType = (AtlasMapType) attribute.getAttributeType();
        AtlasType valueType = mapType.getValueType();
        TypeCategory valueCategory = valueType.getTypeCategory();

        if (valueCategory == TypeCategory.PRIMITIVE || valueCategory == TypeCategory.ENUM) {
            return valueMap;
        }

        Map<String, Object> mappedMap = new HashMap<>();
        for (Map.Entry<?, ?> entry : valueMap.entrySet()) {
            String key = String.valueOf(entry.getKey());
            Object mapValue = entry.getValue();

            if (mapValue == null) {
                mappedMap.put(key, null);
                continue;
            }

            switch (valueCategory) {
                case STRUCT -> {
                    if (mapValue instanceof Map<?, ?> structMap) {
                        mappedMap.put(key, buildStructInstance((AtlasStructType) valueType, (Map<String, Object>) structMap));
                    } else {
                        LOG.warn("Expected a Map for struct value in map attribute {}, found: {}", attribute.getName(), mapValue.getClass().getName());
                        mappedMap.put(key, mapValue);
                    }
                }

                case ARRAY, MAP -> {
                    AtlasAttribute nestedAttr = new AtlasAttribute(attribute.getDefinedInType(),
                            new AtlasAttributeDef(attribute.getName(), valueType.getTypeName()), valueType);
                    mappedMap.put(key, mapAttributeValue(nestedAttr, mapValue));
                }

                default -> {
                    LOG.warn("Unhandled map value type {} for attribute {}", valueCategory, attribute.getName());
                    mappedMap.put(key, mapValue);
                }
            }
        }

        return mappedMap;
    }

    /**
     * Maps a struct value to ensure proper structure with typeName field.
     */
    @SuppressWarnings("unchecked")
    private Object mapStructValue(AtlasAttribute attribute, Object value) throws AtlasBaseException {
        if (!(value instanceof Map<?, ?> structMap)) {
            LOG.warn("Expected a Map for struct attribute {}, found: {}", attribute.getName(), value.getClass().getName());
            return value;
        }

        AtlasStructType structType = (AtlasStructType) attribute.getAttributeType();
        return buildStructInstance(structType, (Map<String, Object>) structMap);
    }

    /**
     * Maps the attributes of a struct to ensure proper structure.
     */
    private Map<String, Object> mapStructAttributes(AtlasStructType structType, Map<String, Object> structAttributes) throws AtlasBaseException {
        if (MapUtils.isEmpty(structAttributes)) {
            return structAttributes;
        }

        Map<String, Object> mappedAttributes = new HashMap<>();
        for (Map.Entry<String, Object> entry : structAttributes.entrySet()) {
            String attrName = entry.getKey();
            Object attrValue = entry.getValue();

            AtlasAttribute attribute = structType.getAttribute(attrName);
            if (attribute == null) {
                LOG.warn("Attribute {}.{} not found in struct definition. Skipping.", structType.getTypeName(), attrName);
                continue;
            }

            Object mappedValue = mapAttributeValue(attribute, attrValue);
            if (mappedValue != null) {
                mappedAttributes.put(attrName, mappedValue);
            }
        }
        return mappedAttributes;
    }

    private Object buildStructInstance(AtlasStructType structType, Map<String, Object> attributes) throws AtlasBaseException {
        Map<String, Object> result = new HashMap<>();
        result.put("typeName", structType.getTypeName());
        result.put("attributes", mapStructAttributes(structType, attributes));
        return result;
    }

}