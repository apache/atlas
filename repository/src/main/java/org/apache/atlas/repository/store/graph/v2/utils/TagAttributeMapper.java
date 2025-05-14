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
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class for mapping classification attributes in a consistent format for v1 and v2.
 * Used to ensure that classifications stored in Cassandra via v2 flow maintain the same
 * structure as classifications stored in JanusGraph via v1 flow.
 */
public class TagAttributeMapper {
    private static final Logger LOG = LoggerFactory.getLogger(TagAttributeMapper.class);

    private final AtlasTypeRegistry typeRegistry;

    public TagAttributeMapper(AtlasTypeRegistry typeRegistry) {
        this.typeRegistry = typeRegistry;
    }

    /**
     * Maps the attributes of a classification to a normalized structure, similar to how
     * they would be mapped in tag vertex in old v1 tags flow.
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
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Classification typeName is empty");
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
        TypeCategory typeCategory = attrType.getTypeCategory();

        switch (typeCategory) {
            case PRIMITIVE:
            case ENUM:
                return value;
                
            case ARRAY:
                return mapArrayValue(attribute, value);
                
            case MAP:
                return mapMapValue(attribute, value);
                
            case STRUCT:
                return mapStructValue(attribute, value);
                
            default:
                LOG.warn("Unsupported attribute type {}. Using original value.", typeCategory);
                return value;
        }
    }

    /**
     * Maps an array value to ensure proper structure with typeName fields for struct elements.
     */
    private Object mapArrayValue(AtlasAttribute attribute, Object value) throws AtlasBaseException {
        if (value == null) {
            return null;
        }

        if (!(value instanceof List)) {
            LOG.warn("Expected a List for array attribute {}, but found: {}", attribute.getName(), value.getClass().getName());
            return value;
        }

        List valueList = (List) value;
        if (valueList.isEmpty()) {
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
                case PRIMITIVE:
                    mappedList.add(element);
                    break;

                case STRUCT:
                    if (element instanceof Map) {
                        AtlasStructType structType = (AtlasStructType) elementType;
                        Map<String, Object> structValue = (Map<String, Object>) element;
                        Map<String, Object> mappedAttributes = mapStructAttributes(structType, structValue);
                        
                        // Create result with typeName field matching v1 format
                        Map<String, Object> result = new HashMap<>();
                        result.put("typeName", structType.getTypeName());
                        result.put("attributes", mappedAttributes);
                        
                        mappedList.add(result);
                    } else {
                        LOG.warn("Expected a Map for struct element in array attribute {}, but found: {}", 
                                attribute.getName(), element.getClass().getName());
                        mappedList.add(element);
                    }
                    break;

                case ARRAY:
                case MAP:
                    // Handle nested collections
                    AtlasAttribute nestedAttribute = new AtlasAttribute(attribute.getDefinedInType(), 
                            new AtlasAttributeDef(attribute.getName(), elementType.getTypeName()), elementType);
                    Object mappedElement = mapAttributeValue(nestedAttribute, element);
                    mappedList.add(mappedElement);
                    break;

                default:
                    LOG.warn("Unhandled element type {} for array attribute {}", elementTypeCategory, attribute.getName());
                    mappedList.add(element);
            }
        }

        return mappedList;
    }

    /**
     * Maps a map value to ensure proper structure with typeName fields for struct values.
     */
    private Object mapMapValue(AtlasAttribute attribute, Object value) throws AtlasBaseException {
        if (value == null) {
            return null;
        }

        if (!(value instanceof Map)) {
            LOG.warn("Expected a Map for map attribute {}, but found: {}", attribute.getName(), value.getClass().getName());
            return value;
        }

        Map<String, Object> valueMap = (Map<String, Object>) value;
        if (valueMap.isEmpty()) {
            return valueMap;
        }

        AtlasMapType mapType = (AtlasMapType) attribute.getAttributeType();
        AtlasType valueType = mapType.getValueType();
        TypeCategory valueTypeCategory = valueType.getTypeCategory();

        // If the map values are primitives, return as is
        if (valueTypeCategory == TypeCategory.PRIMITIVE || valueTypeCategory == TypeCategory.ENUM) {
            return valueMap;
        }

        Map<String, Object> mappedMap = new HashMap<>();
        for (Map.Entry<String, Object> entry : valueMap.entrySet()) {
            String key = entry.getKey();
            Object mapValue = entry.getValue();

            if (mapValue == null) {
                mappedMap.put(key, null);
                continue;
            }

            switch (valueTypeCategory) {
                case STRUCT:
                    AtlasStructType structType = (AtlasStructType) valueType;
                    if (mapValue instanceof Map) {
                        Map<String, Object> structValue = (Map<String, Object>) mapValue;
                        Map<String, Object> mappedAttributes = mapStructAttributes(structType, structValue);
                        
                        // Create result with typeName field matching v1 format
                        Map<String, Object> result = new HashMap<>();
                        result.put("typeName", structType.getTypeName());
                        result.put("attributes", mappedAttributes);
                        
                        mappedMap.put(key, result);
                    } else {
                        LOG.warn("Expected a Map for struct value in map attribute {}, but found: {}", 
                                attribute.getName(), mapValue.getClass().getName());
                        mappedMap.put(key, mapValue);
                    }
                    break;

                case ARRAY:
                case MAP:
                    // Create a new attribute for the map value and recursively map it
                    AtlasAttribute valueAttribute = new AtlasAttribute(attribute.getDefinedInType(), 
                            new AtlasAttributeDef(attribute.getName(), valueType.getTypeName()), valueType);
                    Object mappedValue = mapAttributeValue(valueAttribute, mapValue);
                    mappedMap.put(key, mappedValue);
                    break;

                default:
                    LOG.warn("Unhandled value type {} for map attribute {}", valueTypeCategory, attribute.getName());
                    mappedMap.put(key, mapValue);
            }
        }

        return mappedMap;
    }

    /**
     * Maps a struct value to ensure proper structure with typeName field.
     */
    private Object mapStructValue(AtlasAttribute attribute, Object value) throws AtlasBaseException {
        if (value == null) {
            return null;
        }

        AtlasStructType structType = null;
        if (attribute.getAttributeType().getTypeCategory() == TypeCategory.STRUCT) {
            structType = (AtlasStructType) attribute.getAttributeType();
        } else {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, 
                "Expected STRUCT type for attribute: " + attribute.getName());
        }

        if (value instanceof Map) {
            Map<String, Object> structValue = (Map<String, Object>) value;
            Map<String, Object> mappedAttributes = mapStructAttributes(structType, structValue);
            
            // Create result with typeName field matching v1 format
            Map<String, Object> result = new HashMap<>();
            result.put("typeName", structType.getTypeName());
            result.put("attributes", mappedAttributes);
            
            return result;
        } else {
            LOG.warn("Expected a Map for struct attribute {}, but found: {}", attribute.getName(), value.getClass().getName());
            return value;
        }
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
                LOG.warn("Attribute {}.{} not found in type definition. Skipping.", structType.getTypeName(), attrName);
                continue;
            }

            Object mappedValue = mapAttributeValue(attribute, attrValue);
            if (mappedValue != null) {
                mappedAttributes.put(attrName, mappedValue);
            }
        }

        return mappedAttributes;
    }
}