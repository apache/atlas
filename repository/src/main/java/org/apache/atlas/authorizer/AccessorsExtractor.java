package org.apache.atlas.authorizer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.authorize.AtlasAccessRequest;
import org.apache.atlas.authorize.AtlasAccessorResponse;
import org.apache.atlas.authorize.AtlasEntityAccessRequest;
import org.apache.atlas.authorize.AtlasRelationshipAccessRequest;
import org.apache.atlas.authorizer.store.PoliciesStore;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.plugin.model.RangerPolicy;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.authorize.AtlasPrivilege.ENTITY_ADD_CLASSIFICATION;
import static org.apache.atlas.authorize.AtlasPrivilege.ENTITY_ADD_LABEL;
import static org.apache.atlas.authorize.AtlasPrivilege.ENTITY_CREATE;
import static org.apache.atlas.authorize.AtlasPrivilege.ENTITY_DELETE;
import static org.apache.atlas.authorize.AtlasPrivilege.ENTITY_READ;
import static org.apache.atlas.authorize.AtlasPrivilege.ENTITY_READ_CLASSIFICATION;
import static org.apache.atlas.authorize.AtlasPrivilege.ENTITY_REMOVE_CLASSIFICATION;
import static org.apache.atlas.authorize.AtlasPrivilege.ENTITY_REMOVE_LABEL;
import static org.apache.atlas.authorize.AtlasPrivilege.ENTITY_UPDATE;
import static org.apache.atlas.authorize.AtlasPrivilege.ENTITY_UPDATE_BUSINESS_METADATA;
import static org.apache.atlas.authorize.AtlasPrivilege.ENTITY_UPDATE_CLASSIFICATION;
import static org.apache.atlas.authorize.AtlasPrivilege.RELATIONSHIP_ADD;
import static org.apache.atlas.authorize.AtlasPrivilege.RELATIONSHIP_REMOVE;
import static org.apache.atlas.authorize.AtlasPrivilege.RELATIONSHIP_UPDATE;
import static org.apache.atlas.authorizer.authorizers.EntityAuthorizer.validateEntityFilterCriteria;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;

public class AccessorsExtractor {

    private static final Logger LOG = LoggerFactory.getLogger(AccessorsExtractor.class);

    private static final Set<String> ENTITY_ACTIONS = new HashSet<String>(){{
        add(ENTITY_READ.getType());
        add(ENTITY_CREATE.getType());
        add(ENTITY_UPDATE.getType());
        add(ENTITY_DELETE.getType());
        add(ENTITY_READ_CLASSIFICATION.getType());
        add(ENTITY_ADD_CLASSIFICATION.getType());
        add(ENTITY_UPDATE_CLASSIFICATION.getType());
        add(ENTITY_REMOVE_CLASSIFICATION.getType());
        add(ENTITY_UPDATE_BUSINESS_METADATA.getType());
        add(ENTITY_ADD_LABEL.getType());
        add(ENTITY_REMOVE_LABEL.getType());
    }};

    private static final Set<String> RELATIONSHIP_ACTIONS = new HashSet<String>(){{
        add(RELATIONSHIP_ADD.getType());
        add(RELATIONSHIP_UPDATE.getType());
        add(RELATIONSHIP_REMOVE.getType());
    }};

    public static AtlasAccessorResponse getAccessors(AtlasAccessRequest request) throws AtlasBaseException {
        return getAccessorsInMemory(request);
    }

    private static void collectSubjects(AtlasAccessorResponse response, List<RangerPolicy> matchedPolicies) {
        for (RangerPolicy policy: matchedPolicies) {
            List<RangerPolicy.RangerPolicyItem> policyItems = null;
            if (CollectionUtils.isNotEmpty(policy.getPolicyItems())) {
                for (RangerPolicy.RangerPolicyItem policyItem:  policy.getPolicyItems()) {
                    response.getUsers().addAll(policyItem.getUsers());
                    response.getRoles().addAll(policyItem.getRoles());
                    response.getGroups().addAll(policyItem.getGroups());
                }
            } else if (CollectionUtils.isNotEmpty(policy.getDenyPolicyItems())) {
                for (RangerPolicy.RangerPolicyItem policyItem:  policy.getDenyPolicyItems()) {
                    response.getDenyUsers().addAll(policyItem.getUsers());
                    response.getDenyRoles().addAll(policyItem.getRoles());
                    response.getDenyGroups().addAll(policyItem.getGroups());
                }
            }
        }
    }

    public static AtlasAccessorResponse getAccessorsInMemory(AtlasAccessRequest request) throws AtlasBaseException {
        AtlasAccessorResponse response = new AtlasAccessorResponse();

        String action = request.getAction().getType();

        List<RangerPolicy> abacPolicies = PoliciesStore.getRelevantPolicies(null, null, "atlas_abac", Arrays.asList(action), null, true);

        List<RangerPolicy> matchedPolicies = getAccessorsInMemoryForAbacPolicies(request, abacPolicies);

        collectSubjects(response, matchedPolicies);

        return response;
    }

    public static List<RangerPolicy> getAccessorsInMemoryForAbacPolicies(AtlasAccessRequest request, List<RangerPolicy> abacPolicies) throws AtlasBaseException {
        List<RangerPolicy> matchedPolicies = new ArrayList<>();
        String action = request.getAction().getType();

        ObjectMapper mapper = new ObjectMapper();

        if (ENTITY_ACTIONS.contains(action)) {
            AtlasEntityAccessRequest entityAccessRequest = (AtlasEntityAccessRequest) request;

            AtlasEntityHeader entity = entityAccessRequest.getEntity();
            AtlasVertex vertex = AtlasGraphUtilsV2.findByGuid(entity.getGuid());
            if (vertex == null) {
                vertex = AtlasGraphUtilsV2.findByTypeAndUniquePropertyName(entity.getTypeName(), QUALIFIED_NAME, entity.getAttribute(QUALIFIED_NAME));
            }

            for (RangerPolicy policy : abacPolicies) {
                String filterCriteria = policy.getPolicyFilterCriteria();
                if (filterCriteria != null && !filterCriteria.isEmpty() ) {

                    JsonNode filterCriteriaNode = null;
                    try {
                        filterCriteriaNode = mapper.readTree(policy.getPolicyFilterCriteria());
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }

                    if (filterCriteriaNode != null && filterCriteriaNode.get("entity") != null) {
                        JsonNode entityFilterCriteriaNode = filterCriteriaNode.get("entity");
                        boolean matched = validateEntityFilterCriteria(entityFilterCriteriaNode, entity, vertex);

                        if (matched) {
                            matchedPolicies.add(policy);
                        }
                    }
                }
            }

        } else if (RELATIONSHIP_ACTIONS.contains(action)) {
            AtlasRelationshipAccessRequest relationshipAccessRequest = (AtlasRelationshipAccessRequest) request;

            AtlasEntityHeader entityOne = relationshipAccessRequest.getEnd1Entity();
            AtlasEntityHeader entityTwo = relationshipAccessRequest.getEnd2Entity();

            AtlasVertex vertexOne = AtlasGraphUtilsV2.findByGuid(entityOne.getGuid());
            AtlasVertex vertexTwo = AtlasGraphUtilsV2.findByGuid(entityTwo.getGuid());
            if (vertexOne == null) {
                vertexOne = AtlasGraphUtilsV2.findByTypeAndUniquePropertyName(entityOne.getTypeName(), QUALIFIED_NAME, entityOne.getAttribute(QUALIFIED_NAME));
            }
            if (vertexTwo == null) {
                vertexTwo = AtlasGraphUtilsV2.findByTypeAndUniquePropertyName(entityTwo.getTypeName(), QUALIFIED_NAME, entityTwo.getAttribute(QUALIFIED_NAME));
            }

            for (RangerPolicy policy : abacPolicies) {
                String filterCriteria = policy.getPolicyFilterCriteria();
                if (filterCriteria != null && !filterCriteria.isEmpty() ) {

                    JsonNode filterCriteriaNode = null;
                    try {
                        filterCriteriaNode = mapper.readTree(policy.getPolicyFilterCriteria());
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }

                    if (filterCriteriaNode != null && filterCriteriaNode.get("endOneEntity") != null) {
                        JsonNode entityFilterCriteriaNode = filterCriteriaNode.get("endOneEntity");
                        boolean matched = validateEntityFilterCriteria(entityFilterCriteriaNode, entityOne, vertexOne);

                        if (matched) {
                            entityFilterCriteriaNode = filterCriteriaNode.get("endTwoEntity");
                            matched = validateEntityFilterCriteria(entityFilterCriteriaNode, entityTwo, vertexTwo);
                        }

                        if (matched) {
                            matchedPolicies.add(policy);
                        }
                    }
                }
            }
        }

        return  matchedPolicies;
    }
}
