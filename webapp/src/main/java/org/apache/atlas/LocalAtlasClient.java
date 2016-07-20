/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas;

import com.google.inject.Inject;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.atlas.web.filters.AuditFilter;
import org.apache.atlas.web.resources.EntityResource;
import org.apache.atlas.web.service.ServiceState;
import org.apache.atlas.web.util.DateTimeHelper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import java.util.Date;
import java.util.List;

/**
 * Local atlas client which calls the resource methods directly. Used by NotificationHookConsumer.
 */
public class LocalAtlasClient extends AtlasClient {
    private static final String LOCALHOST = "localhost";
    private static final String CLASS =  LocalAtlasClient.class.getSimpleName();

    public static final Logger LOG = LoggerFactory.getLogger(LocalAtlasClient.class);

    private final EntityResource entityResource;

    private final ServiceState serviceState;

    @Inject
    public LocalAtlasClient(ServiceState serviceState, EntityResource entityResource) {
        super();
        this.serviceState = serviceState;
        this.entityResource = entityResource;
    }

    private String user;

    public void setUser(String user) {
        this.user = user;
    }

    private void setRequestContext() {
        RequestContext requestContext = RequestContext.createContext();
        requestContext.setUser(user);
    }

    @Override
    public boolean isServerReady() throws AtlasServiceException {
        return serviceState.getState() == ServiceState.ServiceStateValue.ACTIVE;
    }

    @Override
    protected List<String> createEntity(final JSONArray entities) throws AtlasServiceException {
        LOG.debug("Creating entities: {}", entities);
        EntityOperation entityOperation = new EntityOperation(API.CREATE_ENTITY) {
            @Override
            Response invoke() {
                return entityResource.submit(new LocalServletRequest(entities.toString()));
            }
        };
        JSONObject response = entityOperation.run();
        EntityResult results = extractEntityResult(response);
        LOG.debug("Create entities returned results: {}", results);
        return results.getCreatedEntities();
    }

    @Override
    protected EntityResult updateEntities(final JSONArray entities) throws AtlasServiceException {
        LOG.debug("Updating entities: {}", entities);
        EntityOperation entityOperation = new EntityOperation(API.UPDATE_ENTITY) {
            @Override
            Response invoke() {
                return entityResource.updateEntities(new LocalServletRequest(entities.toString()));
            }
        };
        JSONObject response = entityOperation.run();
        EntityResult results = extractEntityResult(response);
        LOG.debug("Update entities returned results: {}", results);
        return results;
    }

    private abstract class EntityOperation {
        private final API api;

        public EntityOperation(API api) {
            this.api = api;
        }

        public JSONObject run() throws AtlasServiceException {
            setRequestContext();
            AuditFilter.audit(user, CLASS, api.getMethod(), LOCALHOST, api.getPath(), LOCALHOST, DateTimeHelper.formatDateUTC(new Date()));

            try {
                Response response = invoke();
                return (JSONObject) response.getEntity();
            } catch(WebApplicationException e) {
                try {
                    throw new AtlasServiceException(api, e);
                } catch (JSONException e1) {
                    throw new AtlasServiceException(e);
                }
            }
        }

        abstract Response invoke();
    }

    @Override
    public EntityResult updateEntity(final String entityType, final String uniqueAttributeName,
                               final String uniqueAttributeValue, Referenceable entity) throws AtlasServiceException {
        final String entityJson = InstanceSerialization.toJson(entity, true);
        LOG.debug("Updating entity type: {}, attributeName: {}, attributeValue: {}, entity: {}", entityType,
                uniqueAttributeName, uniqueAttributeValue, entityJson);
        EntityOperation entityOperation = new EntityOperation(API.UPDATE_ENTITY_PARTIAL) {
            @Override
            Response invoke() {
                return entityResource.updateByUniqueAttribute(entityType, uniqueAttributeName, uniqueAttributeValue,
                        new LocalServletRequest(entityJson));
            }
        };
        JSONObject response = entityOperation.run();
        EntityResult result = extractEntityResult(response);
        LOG.debug("Update entity returned result: {}", result);
        return result;
    }

    @Override
    public EntityResult deleteEntity(final String entityType, final String uniqueAttributeName,
                                     final String uniqueAttributeValue) throws AtlasServiceException {
        LOG.debug("Deleting entity type: {}, attributeName: {}, attributeValue: {}", entityType, uniqueAttributeName,
                uniqueAttributeValue);
        EntityOperation entityOperation = new EntityOperation(API.DELETE_ENTITY) {
            @Override
            Response invoke() {
                return entityResource.deleteEntities(null, entityType, uniqueAttributeName, uniqueAttributeValue);
            }
        };
        JSONObject response = entityOperation.run();
        EntityResult results = extractEntityResult(response);
        LOG.debug("Delete entities returned results: {}", results);
        return results;
    }

    @Override
    public String getAdminStatus() throws AtlasServiceException {
        throw new IllegalStateException("Not supported in LocalAtlasClient");
    }

    @Override
    public List<String> createType(String typeAsJson) throws AtlasServiceException {
        throw new IllegalStateException("Not supported in LocalAtlasClient");
    }

    @Override
    public List<String> updateType(String typeAsJson) throws AtlasServiceException {
        throw new IllegalStateException("Not supported in LocalAtlasClient");
    }

    @Override
    public List<String> listTypes() throws AtlasServiceException {
        throw new IllegalStateException("Not supported in LocalAtlasClient");
    }

    @Override
    public TypesDef getType(String typeName) throws AtlasServiceException {
        throw new IllegalStateException("Not supported in LocalAtlasClient");
    }

    @Override
    public EntityResult updateEntityAttribute(final String guid, final String attribute, String value) throws AtlasServiceException {
        throw new IllegalStateException("Not supported in LocalAtlasClient");
    }

    @Override
    public EntityResult updateEntity(String guid, Referenceable entity) throws AtlasServiceException {
        throw new IllegalStateException("Not supported in LocalAtlasClient");
    }


    @Override
    public EntityResult deleteEntities(final String ... guids) throws AtlasServiceException {
        throw new IllegalStateException("Not supported in LocalAtlasClient");
    }

    @Override
    public Referenceable getEntity(String guid) throws AtlasServiceException {
        throw new IllegalStateException("Not supported in LocalAtlasClient");
    }

    @Override
    public Referenceable getEntity(final String entityType, final String attribute, final String value)
            throws AtlasServiceException {
        throw new IllegalStateException("Not supported in LocalAtlasClient");
    }

    @Override
    public List<String> listEntities(final String entityType) throws AtlasServiceException {
        throw new IllegalStateException("Not supported in LocalAtlasClient");
    }

    @Override
    public List<EntityAuditEvent> getEntityAuditEvents(String entityId, String startKey, short numResults)
            throws AtlasServiceException {
        throw new IllegalStateException("Not supported in LocalAtlasClient");
    }

    @Override
    public JSONArray search(final String searchQuery, final int limit, final int offset) throws AtlasServiceException {
        throw new IllegalStateException("Not supported in LocalAtlasClient");
    }

    @Override
    public JSONArray searchByDSL(final String query, final int limit, final int offset) throws AtlasServiceException {
        throw new IllegalStateException("Not supported in LocalAtlasClient");
    }

    @Override
    public JSONObject searchByFullText(final String query, final int limit, final int offset) throws AtlasServiceException {
        throw new IllegalStateException("Not supported in LocalAtlasClient");
    }

    @Override
    public JSONObject getInputGraph(String datasetName) throws AtlasServiceException {
        throw new IllegalStateException("Not supported in LocalAtlasClient");
    }

    @Override
    public JSONObject getOutputGraph(String datasetName) throws AtlasServiceException {
        throw new IllegalStateException("Not supported in LocalAtlasClient");
    }
}
