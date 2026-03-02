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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

    public static Map<String, Object> getDirectTagAttachmentAttributesForAddTag(AtlasClassification tagAdded,
                                                                                List<AtlasClassification> currentTags,
                                                                                AtlasTypeRegistry typeRegistry,
                                                                                IFullTextMapper fullTextMapperV2) throws AtlasBaseException {
        // Add direct Tag
        Map<String, Object> deNormAttrs = new HashMap<>();

        deNormAttrs.put(CLASSIFICATION_TEXT_KEY, getClassificationTextKey(currentTags, typeRegistry, fullTextMapperV2));

        if (currentTags.size() == 1) {
            deNormAttrs.put(CLASSIFICATION_NAMES_KEY, getDelimitedClassificationNames(Collections.singletonList(tagAdded.getTypeName())));
            deNormAttrs.put(TRAIT_NAMES_PROPERTY_KEY, Collections.singletonList(tagAdded.getTypeName()));

        } else {
            //filter direct attachments
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
        // Delete direct Tag
        Map<String, Object> deNormAttrs = new HashMap<>();

        if (CollectionUtils.isEmpty(currentTags)) {
            deNormAttrs.put(CLASSIFICATION_NAMES_KEY, Strings.EMPTY);
            deNormAttrs.put(CLASSIFICATION_TEXT_KEY, Strings.EMPTY);
            deNormAttrs.put(TRAIT_NAMES_PROPERTY_KEY, Collections.EMPTY_LIST);

        } else {
            // Filtering by both typeName and entityGuid ensures we never include the deleted tag in ES updates.
            String deletedTagTypeName = tagDeleted.getTypeName();
            String deletedTagEntityGuid = tagDeleted.getEntityGuid();
            
            List<AtlasClassification> remainingTags = currentTags.stream()
                    .filter(tag -> !(Objects.equals(deletedTagTypeName, tag.getTypeName()) && 
                                     Objects.equals(deletedTagEntityGuid, tag.getEntityGuid())))
                    .collect(Collectors.toList());

            deNormAttrs.put(CLASSIFICATION_TEXT_KEY, getClassificationTextKey(remainingTags, typeRegistry, fullTextMapperV2));

            //filter direct attachments
            List<String> directTraits = remainingTags.stream()
                    .filter(tag -> Objects.equals(deletedTagEntityGuid, tag.getEntityGuid()))
                    .map(AtlasStruct::getTypeName)
                    .collect(Collectors.toList());
            deNormAttrs.put(CLASSIFICATION_NAMES_KEY, getDelimitedClassificationNames(directTraits));

            deNormAttrs.put(TRAIT_NAMES_PROPERTY_KEY, directTraits);
        }

        return deNormAttrs;
    }

    public static Map<String, Object> getPropagatedAttributesForNoTags() {
        // Add tag Propagation, asset does not have any other tag

        Map<String, Object> deNormAttrs = new HashMap<>();

        deNormAttrs.put(CLASSIFICATION_TEXT_KEY, FULL_TEXT_DELIMITER);
        deNormAttrs.put(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY, List.of());
        deNormAttrs.put(PROPAGATED_CLASSIFICATION_NAMES_KEY, CLASSIFICATION_NAME_DELIMITER);

        return deNormAttrs;
    }

    public static Map<String, Object> getPropagatedAttributesForTags(AtlasClassification propagatedTag,
                                                                     List<AtlasClassification> finalTags,
                                                                     List<AtlasClassification> finalPropagatedTags,
                                                                     AtlasTypeRegistry typeRegistry,
                                                                     IFullTextMapper fullTextMapperV2,
                                                                     boolean isDelete) throws AtlasBaseException {
        Map<String, Object> deNormAttrs = new HashMap<>();

        if (CollectionUtils.isNotEmpty(finalTags))
            deNormAttrs.put(CLASSIFICATION_TEXT_KEY, getClassificationTextKey(finalTags, typeRegistry, fullTextMapperV2));

        updateDenormAttributesForPropagatedTags(propagatedTag, finalPropagatedTags, deNormAttrs, isDelete);

        return deNormAttrs;
    }

    public static Map<String, Object> getAllAttributesForAllTagsForRepair(String sourceAssetGuid,
                                                                        List<AtlasClassification> currentTags,
                                                                        AtlasTypeRegistry typeRegistry,
                                                                        IFullTextMapper fullTextMapperV2) throws AtlasBaseException {
        Map<String, Object> deNormAttrs = new HashMap<>();

        String classificationTextKey = Strings.EMPTY;
        String classificationNamesKey = Strings.EMPTY;
        String propagatedClassificationNamesKey = Strings.EMPTY;

        List<String> traitNames= Collections.EMPTY_LIST;
        List<String> propagatedTraitNames = Collections.EMPTY_LIST;

        if (CollectionUtils.isNotEmpty(currentTags)) {
            // filter attachments
            traitNames = new ArrayList<>(0);
            propagatedTraitNames = new ArrayList<>(0);

            for (AtlasClassification tag : currentTags) {
                if (sourceAssetGuid.equals(tag.getEntityGuid())) {
                    traitNames.add(tag.getTypeName());
                } else {
                    propagatedTraitNames.add(tag.getTypeName());
                }
            }

            classificationTextKey = getClassificationTextKey(currentTags, typeRegistry, fullTextMapperV2);

            if (!traitNames.isEmpty()) {
                classificationNamesKey = getDelimitedClassificationNames(traitNames);
            }

            if (!propagatedTraitNames.isEmpty()) {
                StringBuilder finalTagNames = new StringBuilder();
                propagatedTraitNames.forEach(tagName -> finalTagNames.append(CLASSIFICATION_NAME_DELIMITER).append(tagName));

                propagatedClassificationNamesKey = finalTagNames.toString();
            }
        }

        deNormAttrs.put(CLASSIFICATION_TEXT_KEY, classificationTextKey);

        deNormAttrs.put(TRAIT_NAMES_PROPERTY_KEY, traitNames);
        deNormAttrs.put(CLASSIFICATION_NAMES_KEY, classificationNamesKey);

        deNormAttrs.put(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY, propagatedTraitNames);
        deNormAttrs.put(PROPAGATED_CLASSIFICATION_NAMES_KEY, propagatedClassificationNamesKey);

        return deNormAttrs;
    }

    private static void updateDenormAttributesForPropagatedTags(AtlasClassification propagatedTag,
                                                                  List<AtlasClassification> finalPropagatedTags,
                                                                  Map<String, Object> deNormAttrs,
                                                                  boolean isDelete) {
        List<String> propTraits = finalPropagatedTags.stream()
                .map(AtlasStruct::getTypeName)
                .collect(Collectors.toList());

        if (CollectionUtils.isNotEmpty(propTraits)) {
            deNormAttrs.put(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY, propTraits);

            StringBuilder finalTagNames = new StringBuilder();
            propTraits.forEach(tagName -> finalTagNames.append(CLASSIFICATION_NAME_DELIMITER).append(tagName));

            deNormAttrs.put(PROPAGATED_CLASSIFICATION_NAMES_KEY, finalTagNames.toString());
        } else {
            if (isDelete) {
                // MS-655: During delete propagation, no remaining propagated tags means
                // __propagatedTraitNames should be empty. Previously this branch incorrectly
                // wrote the deleted tag name back into ES.
                //TO DO: We will do a larger ES sync design change in future to avoid calculating de-norm attributes
                // but directly sync latest state from cassandra for the asset.
                deNormAttrs.put(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY, Collections.emptyList());
                deNormAttrs.put(PROPAGATED_CLASSIFICATION_NAMES_KEY, CLASSIFICATION_NAME_DELIMITER);
            } else {
                deNormAttrs.put(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY, Collections.singletonList(propagatedTag.getTypeName()));
                deNormAttrs.put(PROPAGATED_CLASSIFICATION_NAMES_KEY, CLASSIFICATION_NAME_DELIMITER + propagatedTag.getTypeName());
            }
        }
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
            fullTextMapperV2.mapAttributes(classificationType, currentTag.getAttributes(), null, sb, null, new HashSet<>(), true);
        }

        return sb.toString();
    }
}
