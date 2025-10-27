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
        // Simple estimation - in real implementation, you might serialize and measure
        return entitiesWithExtInfo.toString().length();
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