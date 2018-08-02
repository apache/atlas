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

package org.apache.atlas.repository.impexp;

import org.apache.atlas.annotation.AtlasService;
import org.apache.atlas.discovery.AtlasDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.impexp.ExportImportAuditEntry;
import org.apache.atlas.repository.ogm.DataAccess;
import org.apache.atlas.repository.ogm.ExportImportAuditEntryDTO;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.ArrayList;

@AtlasService
public class ExportImportAuditService {
    private static final Logger LOG = LoggerFactory.getLogger(ExportImportAuditService.class);
    private static final String ENTITY_TYPE_NAME = "__ExportImportAuditEntry";

    private final DataAccess dataAccess;
    private AtlasDiscoveryService discoveryService;

    @Inject
    public ExportImportAuditService(DataAccess dataAccess, AtlasDiscoveryService discoveryService) {
        this.dataAccess = dataAccess;
        this.discoveryService = discoveryService;
    }

    public void save(ExportImportAuditEntry entry) throws AtlasBaseException {
        dataAccess.saveNoLoad(entry);
    }
    public ExportImportAuditEntry get(ExportImportAuditEntry entry) throws AtlasBaseException {
        if(entry.getGuid() == null) {
            throw new AtlasBaseException("entity does not have GUID set. load cannot proceed.");
        }
        return dataAccess.load(entry);
    }

    public AtlasSearchResult get(String userName, String operation, String sourceCluster, String targetCluster,
                                 String startTime, String endTime,
                                 int limit, int offset) throws AtlasBaseException {
        SearchParameters.FilterCriteria criteria = new SearchParameters.FilterCriteria();
        criteria.setCriterion(new ArrayList<SearchParameters.FilterCriteria>());

        addSearchParameters(criteria, userName, operation, sourceCluster, targetCluster, startTime, endTime);

        SearchParameters searchParameters = getSearchParameters(limit, offset, criteria);

        return discoveryService.searchWithParameters(searchParameters);
    }

    private SearchParameters getSearchParameters(int limit, int offset, SearchParameters.FilterCriteria criteria) {
        SearchParameters searchParameters = new SearchParameters();
        searchParameters.setTypeName(ENTITY_TYPE_NAME);
        searchParameters.setEntityFilters(criteria);
        searchParameters.setLimit(limit);
        searchParameters.setOffset(offset);
        return searchParameters;
    }

    private void addSearchParameters(SearchParameters.FilterCriteria criteria,
                                     String userName, String operation, String sourceCluster, String targetCluster,
                                     String startTime, String endTime) {

        addParameterIfValueNotEmpty(criteria, ExportImportAuditEntryDTO.PROPERTY_USER_NAME, userName);
        addParameterIfValueNotEmpty(criteria, ExportImportAuditEntryDTO.PROPERTY_OPERATION, operation);
        addParameterIfValueNotEmpty(criteria, ExportImportAuditEntryDTO.PROPERTY_SOURCE_CLUSTER_NAME, sourceCluster);
        addParameterIfValueNotEmpty(criteria, ExportImportAuditEntryDTO.PROPERTY_TARGET_CLUSTER_NAME, targetCluster);
        addParameterIfValueNotEmpty(criteria, ExportImportAuditEntryDTO.PROPERTY_START_TIME, startTime);
        addParameterIfValueNotEmpty(criteria, ExportImportAuditEntryDTO.PROPERTY_END_TIME, endTime);
    }

    private void addParameterIfValueNotEmpty(SearchParameters.FilterCriteria criteria,
                                             String attributeName, String value) {
        if(StringUtils.isEmpty(value)) return;

        boolean isFirstCriteria = criteria.getAttributeName() == null;
        SearchParameters.FilterCriteria cx = isFirstCriteria
                                                ? criteria
                                                : new SearchParameters.FilterCriteria();

        setCriteria(cx, attributeName, value);

        if(isFirstCriteria) {
            cx.setCondition(SearchParameters.FilterCriteria.Condition.AND);
        }

        if(!isFirstCriteria) {
            criteria.getCriterion().add(cx);
        }
    }

    private SearchParameters.FilterCriteria setCriteria(SearchParameters.FilterCriteria criteria, String attributeName, String value) {
        criteria.setAttributeName(attributeName);
        criteria.setAttributeValue(value);
        criteria.setOperator(SearchParameters.Operator.EQ);

        return criteria;
    }
}
