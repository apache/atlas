package org.apache.atlas.observability;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;

import java.util.*;

/**
 * Analyzes payload for observability metrics.
 */
public class PayloadAnalyzer {
    private final AtlasTypeRegistry typeRegistry;
    
    public PayloadAnalyzer(AtlasTypeRegistry typeRegistry) {
        this.typeRegistry = typeRegistry;
    }
    
    public void analyzePayload(AtlasEntity.AtlasEntitiesWithExtInfo entitiesWithExtInfo, AtlasObservabilityData data) {
        if (entitiesWithExtInfo == null) {
            return;
        }
        
        // Calculate payload asset size
        List<AtlasEntity> entities = entitiesWithExtInfo.getEntities();
        data.setPayloadAssetSize(CollectionUtils.isEmpty(entities) ? 0 : entities.size());
        
        // Extract asset GUIDs
        List<String> assetGuids = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(entities)) {
            for (AtlasEntity entity : entities) {
                if (entity.getGuid() != null) {
                    assetGuids.add(entity.getGuid());
                }
            }
        }
        data.setAssetGuids(assetGuids);
        
        // Calculate payload bytes (simplified - just estimate)
        data.setPayloadRequestBytes(estimatePayloadSize(entitiesWithExtInfo));
        
        // Analyze array relationships
        analyzeArrayRelationships(entitiesWithExtInfo, data);
    }
    
    private long estimatePayloadSize(AtlasEntity.AtlasEntitiesWithExtInfo entitiesWithExtInfo) {
        if (entitiesWithExtInfo == null) {
            return 0L;
        }
        
        List<AtlasEntity> entities = entitiesWithExtInfo.getEntities();
        if (CollectionUtils.isEmpty(entities)) {
            return 0L;
        }
        
        // Efficient estimation by counting fields and attributes without serialization
        long totalSize = 0L;
        
        for (AtlasEntity entity : entities) {
            // Base entity overhead (type, guid, status, etc.) - estimate ~200 bytes
            totalSize += 200L;
            
            // Count attributes
            if (MapUtils.isNotEmpty(entity.getAttributes())) {
                for (Map.Entry<String, Object> entry : entity.getAttributes().entrySet()) {
                    // Key size + estimated value size
                    totalSize += entry.getKey().length() + estimateValueSize(entry.getValue());
                }
            }
            
            // Count relationship attributes
            if (MapUtils.isNotEmpty(entity.getRelationshipAttributes())) {
                for (Map.Entry<String, Object> entry : entity.getRelationshipAttributes().entrySet()) {
                    totalSize += entry.getKey().length() + estimateValueSize(entry.getValue());
                }
            }
            
            // Count append relationship attributes
            if (MapUtils.isNotEmpty(entity.getAppendRelationshipAttributes())) {
                for (Map.Entry<String, Object> entry : entity.getAppendRelationshipAttributes().entrySet()) {
                    totalSize += entry.getKey().length() + estimateValueSize(entry.getValue());
                }
            }
            
            // Count remove relationship attributes
            if (MapUtils.isNotEmpty(entity.getRemoveRelationshipAttributes())) {
                for (Map.Entry<String, Object> entry : entity.getRemoveRelationshipAttributes().entrySet()) {
                    totalSize += entry.getKey().length() + estimateValueSize(entry.getValue());
                }
            }
            
            // Count classifications
            if (CollectionUtils.isNotEmpty(entity.getClassifications())) {
                totalSize += entity.getClassifications().size() * 100L; // ~100 bytes per classification
            }
        }
        
        return totalSize;
    }
    
    /**
     * Estimates the size of a value without serializing it.
     * Uses heuristics based on value type.
     */
    private long estimateValueSize(Object value) {
        if (value == null) {
            return 4L; // "null" string
        }
        
        if (value instanceof String) {
            return ((String) value).length();
        }
        
        if (value instanceof Number) {
            return 20L; // Average size for numbers (including JSON formatting)
        }
        
        if (value instanceof Boolean) {
            return 5L; // "true" or "false"
        }
        
        if (value instanceof Collection) {
            Collection<?> collection = (Collection<?>) value;
            long size = 2L; // Array brackets
            for (Object item : collection) {
                size += estimateValueSize(item) + 1L; // +1 for comma/separator
            }
            return size;
        }
        
        if (value instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) value;
            long size = 2L; // Object braces
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                size += estimateValueSize(entry.getKey()) + estimateValueSize(entry.getValue()) + 3L; // +3 for ": " and comma
            }
            return size;
        }
        
        // For other objects (like AtlasObjectId), use conservative estimate
        // Avoid toString() to prevent performance issues with complex objects
        return 100L; // Conservative estimate for complex objects (guid + typeName + other fields)
    }
    
    private void analyzeArrayRelationships(AtlasEntity.AtlasEntitiesWithExtInfo entitiesWithExtInfo, AtlasObservabilityData data) {
        Map<String, Integer> relationshipAttributes = new HashMap<>();
        Map<String, Integer> appendRelationshipAttributes = new HashMap<>();
        Map<String, Integer> removeRelationshipAttributes = new HashMap<>();
        Map<String, Integer> arrayAttributes = new HashMap<>();
        
        List<AtlasEntity> entities = entitiesWithExtInfo.getEntities();
        if (CollectionUtils.isEmpty(entities)) {
            data.setRelationshipAttributes(relationshipAttributes);
            data.setAppendRelationshipAttributes(appendRelationshipAttributes);
            data.setRemoveRelationshipAttributes(removeRelationshipAttributes);
            data.setArrayAttributes(arrayAttributes);
            data.setTotalArrayRelationships(0);
            data.setTotalArrayAttributes(0);
            return;
        }
        
        for (AtlasEntity entity : entities) {
            analyzeEntityRelationships(entity, relationshipAttributes, appendRelationshipAttributes, removeRelationshipAttributes);
            analyzeEntityArrayAttributes(entity, arrayAttributes);
        }
        
        data.setRelationshipAttributes(relationshipAttributes);
        data.setAppendRelationshipAttributes(appendRelationshipAttributes);
        data.setRemoveRelationshipAttributes(removeRelationshipAttributes);
        data.setArrayAttributes(arrayAttributes);
        
        // Calculate totals
        int totalRelationships = relationshipAttributes.values().stream().mapToInt(Integer::intValue).sum() +
                               appendRelationshipAttributes.values().stream().mapToInt(Integer::intValue).sum() +
                               removeRelationshipAttributes.values().stream().mapToInt(Integer::intValue).sum();
        data.setTotalArrayRelationships(totalRelationships);
        
        int totalAttributes = arrayAttributes.values().stream().mapToInt(Integer::intValue).sum();
        data.setTotalArrayAttributes(totalAttributes);
    }
    
    private void analyzeEntityRelationships(AtlasEntity entity, 
                                          Map<String, Integer> relationshipAttributes,
                                          Map<String, Integer> appendRelationshipAttributes,
                                          Map<String, Integer> removeRelationshipAttributes) {
        
        // Analyze relationshipAttributes
        if (MapUtils.isNotEmpty(entity.getRelationshipAttributes())) {
            for (Map.Entry<String, Object> entry : entity.getRelationshipAttributes().entrySet()) {
                if (isArrayRelationship(entry.getValue())) {
                    int count = getArrayCount(entry.getValue());
                    relationshipAttributes.merge(entry.getKey(), count, Integer::sum);
                }
            }
        }
        
        // Analyze appendRelationshipAttributes
        if (MapUtils.isNotEmpty(entity.getAppendRelationshipAttributes())) {
            for (Map.Entry<String, Object> entry : entity.getAppendRelationshipAttributes().entrySet()) {
                if (isArrayRelationship(entry.getValue())) {
                    int count = getArrayCount(entry.getValue());
                    appendRelationshipAttributes.merge(entry.getKey(), count, Integer::sum);
                }
            }
        }
        
        // Analyze removeRelationshipAttributes
        if (MapUtils.isNotEmpty(entity.getRemoveRelationshipAttributes())) {
            for (Map.Entry<String, Object> entry : entity.getRemoveRelationshipAttributes().entrySet()) {
                if (isArrayRelationship(entry.getValue())) {
                    int count = getArrayCount(entry.getValue());
                    removeRelationshipAttributes.merge(entry.getKey(), count, Integer::sum);
                }
            }
        }
    }
    
    private void analyzeEntityArrayAttributes(AtlasEntity entity, Map<String, Integer> arrayAttributes) {
        
        // Analyze regular attributes for array values
        if (MapUtils.isNotEmpty(entity.getAttributes())) {
            for (Map.Entry<String, Object> entry : entity.getAttributes().entrySet()) {
                if (isArrayAttribute(entry.getValue())) {
                    int count = getArrayCount(entry.getValue());
                    arrayAttributes.merge(entry.getKey(), count, Integer::sum);
                }
            }
        }
    }
    
    private boolean isArrayRelationship(Object value) {
        return value instanceof Collection && !((Collection<?>) value).isEmpty();
    }
    
    private boolean isArrayAttribute(Object value) {
        return value instanceof Collection && !((Collection<?>) value).isEmpty();
    }
    
    private int getArrayCount(Object value) {
        if (value instanceof Collection) {
            return ((Collection<?>) value).size();
        }
        return 0;
    }
}