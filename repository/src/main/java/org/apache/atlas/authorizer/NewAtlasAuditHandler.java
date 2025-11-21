package org.apache.atlas.authorizer;

import org.apache.atlas.RequestContext;
import org.apache.atlas.audit.model.AuthzAuditEvent;
import org.apache.atlas.audit.provider.AuditHandler;
import org.apache.atlas.audit.provider.AuditProviderFactory;
import org.apache.atlas.audit.provider.MiscUtil;
import org.apache.atlas.authorize.AtlasAccessRequest;
import org.apache.atlas.authorize.AtlasAccessResult;
import org.apache.atlas.authorize.AtlasEntityAccessRequest;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.authorize.AtlasRelationshipAccessRequest;
import org.apache.atlas.authorizer.authorizers.AuthorizerCommonUtil;
import static org.apache.atlas.constants.RangerAtlasConstants.*;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.plugin.model.NewAccessResourceImpl;
import org.apache.atlas.plugin.model.RangerServiceDef;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

class NewAtlasAuditHandler {
    private static final Logger LOG = LoggerFactory.getLogger(NewAtlasAuditHandler.class);

    private final Map<String, AuthzAuditEvent> auditEvents;
    private final String                       resourcePath;
    private final String                       resourceType;
    private         long                       sequenceNumber = 0;

    public NewAtlasAuditHandler(AtlasEntityAccessRequest request, RangerServiceDef serviceDef) {
        Collection<AtlasClassification> classifications    = request.getEntityClassifications();
        String             strClassifications = classifications == null ? "[]" : classifications.toString();

        if (request.getClassification() != null) {
            strClassifications += ("," + request.getClassification().getTypeName());
        }

        NewAccessResourceImpl rangerResource = new NewAccessResourceImpl();

        rangerResource.setServiceDef(serviceDef);
        rangerResource.setValue(RESOURCE_ENTITY_TYPE, request.getEntityType());
        rangerResource.setValue(RESOURCE_ENTITY_CLASSIFICATION, strClassifications);
        rangerResource.setValue(RESOURCE_ENTITY_ID, request.getEntityId());

        if (AtlasPrivilege.ENTITY_ADD_LABEL.equals(request.getAction()) || AtlasPrivilege.ENTITY_REMOVE_LABEL.equals(request.getAction())) {
            rangerResource.setValue(RESOURCE_ENTITY_LABEL, "label=" + request.getLabel());
        } else if (AtlasPrivilege.ENTITY_UPDATE_BUSINESS_METADATA.equals(request.getAction())) {
            rangerResource.setValue(RESOURCE_ENTITY_BUSINESS_METADATA, "business-metadata=" + request.getBusinessMetadata());
        }

        auditEvents  = new HashMap<>();
        resourceType = rangerResource.getLeafName();
        resourcePath = rangerResource.getAsString();
    }

    public NewAtlasAuditHandler(AtlasRelationshipAccessRequest request, RangerServiceDef serviceDef) {
        NewAccessResourceImpl rangerResource = new NewAccessResourceImpl();
        rangerResource.setServiceDef(serviceDef);

        rangerResource.setValue(RESOURCE_RELATIONSHIP_TYPE, request.getRelationshipType());

        //End 1
        final String      end1EntityId = request.getEnd1EntityId();
        Set<String> end1EntityTypeAndSuperTypes = request.getEnd1EntityTypeAndAllSuperTypes();

        final Set<AtlasClassification> end1Classifications = new HashSet<AtlasClassification>(request.getEnd1EntityClassifications());
        Set<String> classificationsWithSuperTypesEnd1 = new HashSet();
        for (AtlasClassification classificationToAuthorize : end1Classifications) {
            classificationsWithSuperTypesEnd1.addAll(request.getClassificationTypeAndAllSuperTypes(classificationToAuthorize.getTypeName()));
        }

        rangerResource.setValue(RESOURCE_END_ONE_ENTITY_TYPE, end1EntityTypeAndSuperTypes);
        rangerResource.setValue(RESOURCE_END_ONE_ENTITY_CLASSIFICATION, classificationsWithSuperTypesEnd1);
        rangerResource.setValue(RESOURCE_END_ONE_ENTITY_ID, end1EntityId);

        //End 2
        final String      end2EntityId = request.getEnd2EntityId();
        Set<String> end2EntityTypeAndSuperTypes = request.getEnd2EntityTypeAndAllSuperTypes();

        final Set<AtlasClassification> end2Classifications = new HashSet<AtlasClassification>(request.getEnd2EntityClassifications());
        Set<String> classificationsWithSuperTypesEnd2 = new HashSet();
        for (AtlasClassification classificationToAuthorize : end2Classifications) {
            classificationsWithSuperTypesEnd2.addAll(request.getClassificationTypeAndAllSuperTypes(classificationToAuthorize.getTypeName()));
        }

        rangerResource.setValue(RESOURCE_END_TWO_ENTITY_TYPE, end2EntityTypeAndSuperTypes);
        rangerResource.setValue(RESOURCE_END_TWO_ENTITY_CLASSIFICATION, classificationsWithSuperTypesEnd2);
        rangerResource.setValue(RESOURCE_END_TWO_ENTITY_ID, end2EntityId);

        auditEvents  = new HashMap<>();
        resourceType = rangerResource.getLeafName();
        resourcePath = rangerResource.getAsString();
    }

    public void processResult(AtlasAccessResult result, AtlasAccessRequest request) {

        AuthzAuditEvent auditEvent = getAuthzEvents(result, request);

        if (auditEvent != null) {
            // audit event might have list of entity-types and classification-types; overwrite with the values in original request
            if (resourcePath != null) {
                auditEvent.setResourcePath(resourcePath);
            }

            if (resourceType != null) {
                auditEvent.setResourceType(resourceType);
            }

            if (StringUtils.isNotEmpty(result.getPolicyId())) {
                auditEvent.setPolicyId(result.getPolicyId());
            }

            auditEvents.put(auditEvent.getPolicyId() + auditEvent.getAccessType(), auditEvent);
        }
    }

    public void flushAudit() {
        if (auditEvents != null) {
            for (AuthzAuditEvent auditEvent : auditEvents.values()) {
                logAuthzAudit(auditEvent);
            }
        }
    }

    private void logAuthzAudit(AuthzAuditEvent auditEvent) {
        if(auditEvent != null) {
            populateDefaults(auditEvent);

            AuditHandler auditProvider = AuditProviderFactory.getInstance().getAuditProvider();
            if (auditProvider == null || !auditProvider.log(auditEvent)) {
                LOG.warn("fail to log audit event " + auditEvent);
            }
        }
    }

    private void populateDefaults(AuthzAuditEvent auditEvent) {
        auditEvent.setUser(AuthorizerCommonUtil.getCurrentUserName());

        if (auditEvent.getAgentHostname() == null || auditEvent.getAgentHostname().isEmpty()) {
            auditEvent.setAgentHostname(MiscUtil.getHostname());
        }

        if (auditEvent.getLogType() == null || auditEvent.getLogType().isEmpty()) {
            auditEvent.setLogType("NewAuthZAudit");
        }

        if (auditEvent.getEventId() == null || auditEvent.getEventId().isEmpty()) {
            auditEvent.setEventId(generateNextAuditEventId());
        }

        if (auditEvent.getAgentId() == null) {
            auditEvent.setAgentId(MiscUtil.getApplicationType());
        }

        auditEvent.setSeqNum(sequenceNumber++);
    }

    private String generateNextAuditEventId() {
        final String ret;

        ret = RequestContext.get().getTraceId() + "-" + System.currentTimeMillis();


        return ret;
    }

    public AuthzAuditEvent getAuthzEvents(AtlasAccessResult result, AtlasAccessRequest request) {
        AuthzAuditEvent ret = null;

        if(request != null) {
            ret = new AuthzAuditEvent();

            ret.setRepositoryName("atlas");
            ret.setEventTime(request.getAccessTime() != null ? request.getAccessTime() : new Date());
            ret.setAction(request.getAction().getType());
            ret.setAccessResult((short) (result.isAllowed() ? 1 : 0));
            ret.setAccessType(request.getAction().getType());
            ret.setClientIP(RequestContext.get().getClientIPAddress());
            /*Set<String> tags = getTags(request);
            if (tags != null) {
                ret.setTags(tags);
            }*/
            ret.setAgentHostname(MiscUtil.getHostname());
            ret.setAclEnforcer(result.getEnforcer());

            populateDefaults(ret);

            //result.setAuditLogId(ret.getEventId());
        }

        return ret;
    }
}