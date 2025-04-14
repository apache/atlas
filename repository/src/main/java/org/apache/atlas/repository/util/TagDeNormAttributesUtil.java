package org.apache.atlas.repository.util;

import joptsimple.internal.Strings;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.repository.graph.IFullTextMapper;
import org.apache.atlas.repository.store.graph.v2.tags.TagDAO;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.atlas.repository.Constants.CLASSIFICATION_NAMES_KEY;
import static org.apache.atlas.repository.Constants.CLASSIFICATION_NAME_DELIMITER;
import static org.apache.atlas.repository.Constants.CLASSIFICATION_TEXT_KEY;
import static org.apache.atlas.repository.Constants.PROPAGATED_CLASSIFICATION_NAMES_KEY;
import static org.apache.atlas.repository.Constants.PROPAGATED_TRAIT_NAMES_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.TRAIT_NAMES_PROPERTY_KEY;
import static org.apache.atlas.repository.graph.FullTextMapperV2.FULL_TEXT_DELIMITER;
import static org.apache.atlas.repository.graph.GraphHelper.getDelimitedClassificationNames;

public class TagDeNormAttributesUtil {

    private static final Logger LOG = LoggerFactory.getLogger(TagDeNormAttributesUtil.class);

    public static Map<String, Object> getDirectTagAttachmentAttributesForAddTag(final TagDAO tagDAO,
                                                                                AtlasClassification tagAdded,
                                                                                List<AtlasClassification> currentTags,
                                                                                AtlasTypeRegistry typeRegistry,
                                                                                IFullTextMapper fullTextMapperV2) throws AtlasBaseException {
        // Add direct Tag

        Map<String, Object> deNormAttrs = new HashMap<>();

        if (CollectionUtils.isEmpty(currentTags)) {
            deNormAttrs = getDirectAttributesForNoTags(tagAdded.getTypeName());

        } else {
            currentTags.add(tagAdded);

            deNormAttrs.put(CLASSIFICATION_TEXT_KEY, getClassificationTextKey(currentTags, typeRegistry, fullTextMapperV2));

            //filter propagated attachments
            List<String> directTraits = currentTags.stream()
                    .filter(tag -> tagAdded.getEntityGuid().equals(tag.getEntityGuid()))
                    .map(AtlasStruct::getTypeName)
                    .collect(Collectors.toList());
            deNormAttrs.put(CLASSIFICATION_NAMES_KEY, getDelimitedClassificationNames(directTraits));

            deNormAttrs.put(TRAIT_NAMES_PROPERTY_KEY, directTraits);
        }

        return deNormAttrs;
    }

    public static Map<String, Object> getDirectTagAttachmentAttributesForDeleteTag(AtlasClassification tagDeleted,
                                                                                List<AtlasClassification> currentTags,
                                                                                AtlasTypeRegistry typeRegistry,
                                                                                IFullTextMapper fullTextMapperV2) throws AtlasBaseException {
        // Add direct Tag

        Map<String, Object> deNormAttrs = new HashMap<>();

        if (CollectionUtils.isEmpty(currentTags)) {
            deNormAttrs.put(CLASSIFICATION_NAMES_KEY, Strings.EMPTY);
            deNormAttrs.put(CLASSIFICATION_TEXT_KEY, Strings.EMPTY);
            deNormAttrs.put(TRAIT_NAMES_PROPERTY_KEY, Collections.EMPTY_LIST);

        } else {
            currentTags.remove(tagDeleted);

            deNormAttrs.put(CLASSIFICATION_TEXT_KEY, getClassificationTextKey(currentTags, typeRegistry, fullTextMapperV2));

            //filter propagated attachments
            List<String> directTraits = currentTags.stream()
                    .filter(tag -> tagDeleted.getEntityGuid().equals(tag.getEntityGuid()))
                    .map(AtlasStruct::getTypeName)
                    .collect(Collectors.toList());
            deNormAttrs.put(CLASSIFICATION_NAMES_KEY, getDelimitedClassificationNames(directTraits));

            deNormAttrs.put(TRAIT_NAMES_PROPERTY_KEY, directTraits);
        }

        return deNormAttrs;
    }

    public static Map<String, Map<String, Object>> getPropagatedTagDeNormForDeleteProp(final TagDAO tagDAO,
                                                                                       String sourceEntityGuid,
                                                                                       List<String> propagatedVertexIds,
                                                                                       AtlasTypeRegistry typeRegistry,
                                                                                       IFullTextMapper fullTextMapperV2) throws AtlasBaseException {
        // Delete tag propagation
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("getPropagatedTagDeNormForDeleteProp");

        Map<String, Map<String, Object>> finalDeNormMap = new HashMap<>();

        if(CollectionUtils.isNotEmpty(propagatedVertexIds)) {
            Map<String, Object> deNormAttrs = new HashMap<>();

            for(String vertexId : propagatedVertexIds) {
                List<AtlasClassification> finalTags = tagDAO.getTagsForVertex(vertexId);

                if (CollectionUtils.isNotEmpty(finalTags)) {
                    deNormAttrs.put(CLASSIFICATION_TEXT_KEY, getClassificationTextKey(finalTags, typeRegistry, fullTextMapperV2));

                    //filter propagated attachments
                    List<String> propTraits = finalTags.stream()
                            .filter(tag -> !sourceEntityGuid.equals(tag.getEntityGuid()))
                            .map(AtlasStruct::getTypeName)
                            .collect(Collectors.toList());

                    if (CollectionUtils.isNotEmpty(propTraits)) {
                        deNormAttrs.put(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY, propTraits);

                        StringBuilder finalTagNames = new StringBuilder();
                        propTraits.forEach(tagName -> finalTagNames.append(CLASSIFICATION_NAME_DELIMITER).append(tagName));

                        deNormAttrs.put(PROPAGATED_CLASSIFICATION_NAMES_KEY, finalTagNames.toString());
                    } else {
                        deNormAttrs.put(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY, Collections.EMPTY_LIST);
                        deNormAttrs.put(PROPAGATED_CLASSIFICATION_NAMES_KEY, Strings.EMPTY);
                    }

                } else {
                    deNormAttrs.put(CLASSIFICATION_TEXT_KEY, Strings.EMPTY);
                    deNormAttrs.put(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY, Collections.EMPTY_LIST);
                    deNormAttrs.put(PROPAGATED_CLASSIFICATION_NAMES_KEY, Strings.EMPTY);
                }

                finalDeNormMap.put(vertexId, deNormAttrs);
            }
        }

        RequestContext.get().endMetricRecord(metricRecorder);
        return finalDeNormMap;
    }

    private static Map<String, Object> getDirectAttributesForNoTags(String tagNameAdded) {
        // Add direct Tag

        Map<String, Object> deNormAttrs = new HashMap<>();

        deNormAttrs.put(CLASSIFICATION_NAMES_KEY, getDelimitedClassificationNames(Collections.singletonList(tagNameAdded)));
        deNormAttrs.put(CLASSIFICATION_TEXT_KEY, tagNameAdded + FULL_TEXT_DELIMITER);
        deNormAttrs.put(TRAIT_NAMES_PROPERTY_KEY, Collections.singletonList(tagNameAdded));

        return deNormAttrs;
    }

    public static Map<String, Object> getPropagatedAttributesForNoTags(String tagNamePropagated) {
        // Add tag Propagation, asset does not have any other tag

        Map<String, Object> deNormAttrs = new HashMap<>();

        deNormAttrs.put(CLASSIFICATION_TEXT_KEY, tagNamePropagated + FULL_TEXT_DELIMITER);
        deNormAttrs.put(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY, Collections.singletonList(tagNamePropagated));
        deNormAttrs.put(PROPAGATED_CLASSIFICATION_NAMES_KEY, CLASSIFICATION_NAME_DELIMITER + tagNamePropagated);

        return deNormAttrs;
    }

    public static Map<String, Object> getPropagatedAttributesForTags(AtlasClassification propagatedTag,
                                                                     List<AtlasClassification> finalTags,
                                                                     AtlasTypeRegistry typeRegistry,
                                                                     IFullTextMapper fullTextMapperV2) throws AtlasBaseException {
        // Add tag Propagation, asset having other tags
        Map<String, Object> deNormAttrs = new HashMap<>();

        final AtlasClassificationType classificationType = typeRegistry.getClassificationTypeByName(propagatedTag.getTypeName());

        StringBuilder sb = new StringBuilder();
        sb.append(propagatedTag.getTypeName()).append(FULL_TEXT_DELIMITER);
        fullTextMapperV2.mapAttributes(classificationType, propagatedTag.getAttributes(), null, sb, null, null, true);
        String classificationTextForEntity = sb.toString();

        deNormAttrs.put(CLASSIFICATION_TEXT_KEY, classificationTextForEntity);

        //filter propagated attachments
        List<String> propTraits = finalTags.stream()
                .filter(tag -> !propagatedTag.getEntityGuid().equals(tag.getEntityGuid()))
                .map(AtlasStruct::getTypeName)
                .collect(Collectors.toList());

        if (CollectionUtils.isNotEmpty(propTraits)) {
            deNormAttrs.put(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY, propTraits);

            StringBuilder finalTagNames = new StringBuilder();
            propTraits.forEach(tagName -> finalTagNames.append(CLASSIFICATION_NAME_DELIMITER).append(tagName));

            deNormAttrs.put(PROPAGATED_CLASSIFICATION_NAMES_KEY, finalTagNames.toString());
        } else {
            deNormAttrs.put(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY, Collections.singletonList(propagatedTag.getTypeName()));
            deNormAttrs.put(PROPAGATED_CLASSIFICATION_NAMES_KEY, CLASSIFICATION_NAME_DELIMITER + propagatedTag.getTypeName());
        }

        return deNormAttrs;
    }

    private static String getClassificationTextKey(List<AtlasClassification> tags, AtlasTypeRegistry typeRegistry, IFullTextMapper fullTextMapperV2) throws AtlasBaseException {
        if (typeRegistry == null) {
            LOG.error("typeRegistry can not be null");
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "typeRegistry can not be null");
        }
        if (fullTextMapperV2 == null) {
            LOG.error("fullTextMapperV2 can not be null");
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "fullTextMapperV2 can not be null");
        }

        StringBuilder sb = new StringBuilder();
        for (AtlasClassification currentTag : tags) {
            final AtlasClassificationType classificationType = typeRegistry.getClassificationTypeByName(currentTag.getTypeName());

            sb.append(currentTag.getTypeName()).append(FULL_TEXT_DELIMITER);
            fullTextMapperV2.mapAttributes(classificationType, currentTag.getAttributes(), null, sb, null, null, true);
        }

        return sb.toString();
    }
}
