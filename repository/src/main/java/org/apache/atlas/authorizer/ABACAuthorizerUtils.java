package org.apache.atlas.authorizer;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.authorize.AtlasAccessRequest;
import org.apache.atlas.authorize.AtlasAccessorResponse;
import org.apache.atlas.authorize.AtlasEntityAccessRequest;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.authorize.AtlasRelationshipAccessRequest;
import org.apache.atlas.authorizer.authorizers.EntityAuthorizer;
import org.apache.atlas.authorizer.authorizers.ListAuthorizer;
import org.apache.atlas.authorizer.authorizers.RelationshipAuthorizer;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.plugin.model.RangerServiceDef;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.authorize.AtlasPrivilege.ENTITY_CREATE;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;

@Component
public class ABACAuthorizerUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ABACAuthorizerUtils.class);

    public static final String POLICY_TYPE_ALLOW = "allow";
    public static final String POLICY_TYPE_DENY = "deny";
    public static final int MAX_CLAUSE_LIMIT = 1024;

    public static final String DENY_POLICY_NAME_SUFFIX = "_deny";

    private static AtlasTypeRegistry typeRegistry;
    private static RangerServiceDef SERVICE_DEF_ATLAS = null;

    private static boolean ABACAuthorizerEnabled = false;
    static {
        try {
            ABACAuthorizerEnabled = ApplicationProperties.get().getBoolean("atlas.authorizer.enable.abac", false);

            if (ABACAuthorizerEnabled) {
                LOG.info("ABAC authorizer enabled!");
            }
        } catch (AtlasException e) {
            LOG.warn("Failed to read conf `atlas.authorizer.enable.abac`, ABAC authorizer is disabled by default");
        }
    }

    public static boolean isABACAuthorizerEnabled() {
        return ABACAuthorizerEnabled;
    }

    @Inject
    public ABACAuthorizerUtils(AtlasGraph graph, AtlasTypeRegistry typeRegistry) throws IOException {
        this.typeRegistry = typeRegistry;

        SERVICE_DEF_ATLAS = getResourceAsObject("/service-defs/atlas-servicedef-atlas.json", RangerServiceDef.class);
    }

    public static boolean isAccessAllowed(AtlasEntityHeader entityHeader, AtlasPrivilege action) {
        if (!ABACAuthorizerEnabled) {
            return false;
        }

        return verifyAccess(entityHeader, action);
    }

    public static boolean isAccessAllowed(String relationShipType, AtlasEntityHeader endOneEntity, AtlasEntityHeader endTwoEntity, AtlasPrivilege action) {
        if (!ABACAuthorizerEnabled) {
            return false;
        }

        return verifyAccess(relationShipType, endOneEntity, endTwoEntity, action);
    }

    private static boolean verifyAccess(AtlasEntityHeader entity, AtlasPrivilege action) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("verifyEntityAccess");

        AccessResult result = null;

        AtlasEntityAccessRequest request = new AtlasEntityAccessRequest(typeRegistry, action, entity);
        NewAtlasAuditHandler auditHandler = new NewAtlasAuditHandler(request, SERVICE_DEF_ATLAS);

        try {
            result = EntityAuthorizer.isAccessAllowedInMemory(entity, action.getType());
            auditHandler.processResult(result, request);
        } finally {
            auditHandler.flushAudit();
            RequestContext.get().endMetricRecord(recorder);
        }

        return result.isAllowed();
    }

    private static boolean verifyAccess(String relationshipType, AtlasEntityHeader endOneEntity, AtlasEntityHeader endTwoEntity, AtlasPrivilege action) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("verifyAccess");
        AccessResult result = new AccessResult();

        AtlasRelationshipAccessRequest request = new AtlasRelationshipAccessRequest(typeRegistry,
                action,
                relationshipType,
                endOneEntity,
                endTwoEntity);
        NewAtlasAuditHandler auditHandler = new NewAtlasAuditHandler(request, SERVICE_DEF_ATLAS);

        try {
            result = RelationshipAuthorizer.isAccessAllowedInMemory(action.getType(), relationshipType, endOneEntity, endTwoEntity);
            auditHandler.processResult(result, request);
        } catch (AtlasBaseException e) {
            LOG.error(e.getMessage());
        } finally {
            auditHandler.flushAudit();
            RequestContext.get().endMetricRecord(recorder);
        }

        return result.isAllowed();
    }

    public static AtlasAccessorResponse getAccessors(AtlasAccessRequest request) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("AuthorizerUtils.getAccessors");

        if (!ABACAuthorizerEnabled) {
            return null;
        }

        try {
            return AccessorsExtractor.getAccessors(request);
        } catch (AtlasBaseException e) {
            LOG.error(e.getMessage());
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }

        return null;
    }

    public static Map<String, Object> getPreFilterDsl(String persona, String purpose, List<String> actions) {
        return ListAuthorizer.getElasticsearchDSL(persona, purpose, actions);
    }

    private <T> T getResourceAsObject(String resourceName, Class<T> clazz) throws IOException {
        InputStream stream = getClass().getResourceAsStream(resourceName);
        return AtlasType.fromJson(stream, clazz);
    }
}
