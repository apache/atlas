/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.store.graph.v2.preprocessor.accesscontrol;

import org.apache.atlas.RequestContext;
import org.apache.atlas.accesscontrol.AccessControlUtil;
import org.apache.atlas.accesscontrol.persona.AtlasPersonaService;
import org.apache.atlas.accesscontrol.persona.PersonaContext;
import org.apache.atlas.accesscontrol.purpose.AtlasPurposeService;
import org.apache.atlas.accesscontrol.purpose.PurposeContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessor;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.atlas.model.instance.EntityMutations.EntityOperation.CREATE;
import static org.apache.atlas.model.instance.EntityMutations.EntityOperation.DELETE;
import static org.apache.atlas.model.instance.EntityMutations.EntityOperation.UPDATE;
import static org.apache.atlas.repository.Constants.POLICY_CATEGORY_PERSONA;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;

public class AccessControlPolicyPreProcessor implements PreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(AccessControlPolicyPreProcessor.class);

    private final AtlasTypeRegistry typeRegistry;
    private final EntityGraphRetriever entityRetriever;
    private final AtlasPersonaService personaService;
    private final AtlasPurposeService purposeService;

    public AccessControlPolicyPreProcessor(AtlasTypeRegistry typeRegistry, AtlasGraph graph, EntityGraphRetriever entityRetriever) {
        this.entityRetriever = entityRetriever;
        this.typeRegistry = typeRegistry;
        personaService = new AtlasPersonaService(graph, entityRetriever);
        purposeService = new AtlasPurposeService(graph, entityRetriever);
    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context, EntityMutations.EntityOperation operation) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("AccessControlPolicyPreProcessor.processAttributes: pre processing {}, {}",
                    entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }

        AtlasEntity entity = (AtlasEntity) entityStruct;
        AtlasVertex vertex = context.getVertex(entity.getGuid());

        switch (operation) {
            case CREATE:
                processCreateAccessControlPolicy(entity, vertex);
                break;
            case UPDATE:
                processUpdateAccessControlPolicy(entity, vertex);
                break;
        }
    }

    @Override
    public void processDelete(AtlasVertex vertex) throws AtlasBaseException {
        AtlasEntity policy = entityRetriever.toAtlasEntity(vertex);
        AtlasEntityWithExtInfo accessControlExtInfo = getAccessControl(policy, DELETE);

        String policyCategory = AccessControlUtil.getPolicyCategory(policy);
        if (POLICY_CATEGORY_PERSONA.equals(policyCategory)) {
            PersonaContext context = new PersonaContext(accessControlExtInfo, policy);
            personaService.deletePersonaPolicy(context);
        } else {
            PurposeContext context = new PurposeContext(accessControlExtInfo, policy);
            purposeService.deletePurposePolicy(context);
        }
    }

    private void processCreateAccessControlPolicy(AtlasEntity entity, AtlasVertex vertex) throws AtlasBaseException {

        String policyCategory = AccessControlUtil.getPolicyCategory(entity);

        if (POLICY_CATEGORY_PERSONA.equals(policyCategory)) {
            PersonaContext context = new PersonaContext(getAccessControl(entity, CREATE), entity);
            personaService.createPersonaPolicy(context);
        } else {
            PurposeContext context = new PurposeContext(getAccessControl(entity, CREATE), entity);
            purposeService.createPurposePolicy(context);
        }
    }


    private void processUpdateAccessControlPolicy(AtlasEntity entity, AtlasVertex vertex) throws AtlasBaseException {
        String vertexQName = vertex.getProperty(QUALIFIED_NAME, String.class);
        entity.setAttribute(QUALIFIED_NAME, vertexQName);

        String policyCategory = AccessControlUtil.getPolicyCategory(entity);

        if (POLICY_CATEGORY_PERSONA.equals(policyCategory)) {
            PersonaContext context = new PersonaContext(getAccessControl(entity, UPDATE), entity);
            context.setExistingPersonaPolicy(entityRetriever.toAtlasEntity(vertex));

            personaService.updatePersonaPolicy(context);
        } else {
            PurposeContext context = new PurposeContext(getAccessControl(entity, UPDATE), entity);
            context.setExistingPurposePolicy(entityRetriever.toAtlasEntity(vertex));

            purposeService.updatePurposePolicy(context);
        }
    }

    private AtlasEntityWithExtInfo getAccessControl(AtlasEntity entity, EntityMutations.EntityOperation op) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("AccessControlPolicyPreProcessor.getAccessControl");
        AtlasEntityWithExtInfo ret;

        AtlasObjectId objectId = (AtlasObjectId) entity.getRelationshipAttribute("accessControl");
        ret = entityRetriever.toAtlasEntityWithExtInfo(objectId);

        //as entity is not committed yet,
        //in case of create policy, AccessControl does not have relation with current new policy
        //in case of update policy, AccessControl does not have latest state of policy to be updated
        ret.addReferredEntity(entity);

        if (op == CREATE) {

            AtlasEntity accessControlEntity = ret.getEntity();
            List<AtlasObjectId> policies = (List<AtlasObjectId>) accessControlEntity.getRelationshipAttribute("policies");
            if (CollectionUtils.isEmpty(policies)) {
                policies = new ArrayList<>();
            }
            policies.add(new AtlasObjectId(entity.getGuid(), entity.getTypeName()));
            accessControlEntity.setRelationshipAttribute("policies", policies);
        }

        RequestContext.get().endMetricRecord(metricRecorder);
        return ret;
    }
}
