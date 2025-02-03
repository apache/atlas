/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.store.graph.v2.tasks.searchdownload;

import com.opencsv.CSVWriter;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.discovery.AtlasDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.tasks.AbstractTask;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasJson;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.model.discovery.AtlasSearchResult.AtlasQueryType.BASIC;
import static org.apache.atlas.model.discovery.AtlasSearchResult.AtlasQueryType.DSL;
import static org.apache.atlas.model.tasks.AtlasTask.Status.COMPLETE;
import static org.apache.atlas.model.tasks.AtlasTask.Status.FAILED;

public class SearchResultDownloadTask extends AbstractTask {
    private static final Logger LOG = LoggerFactory.getLogger(SearchResultDownloadTask.class);

    public static final String SEARCH_PARAMETERS_JSON_KEY = "search_parameters_json";
    public static final String CSV_FILE_NAME_KEY          = "csv_file_Name";
    public static final String SEARCH_TYPE_KEY            = "search_type";
    public static final String ATTRIBUTE_LABEL_MAP_KEY    = "attribute_label_map";
    public static final String QUERY_KEY                  = "query";
    public static final String TYPE_NAME_KEY              = "type_name";
    public static final String CLASSIFICATION_KEY         = "classification";
    public static final String LIMIT_KEY                  = "limit";
    public static final String OFFSET_KEY                 = "offset";
    public static final String CSV_FILE_EXTENSION         = ".csv";
    public static final String DOWNLOAD_DIR_PATH;

    private static final String EMPTY_STRING               = "";
    private static final String DOWNLOAD_DIR_PATH_KEY      = "atlas.download.search.dir.path";
    private static final String DOWNLOAD_DIR_PATH_DEFAULT  = System.getProperty("user.dir");
    private static final String CSV_DOWNLOAD_DIR           = "search_result_downloads";

    private final AtlasDiscoveryService discoveryService;
    private final AtlasTypeRegistry     typeRegistry;

    public SearchResultDownloadTask(AtlasTask task, AtlasDiscoveryService discoveryService, AtlasTypeRegistry typeRegistry) {
        super(task);

        this.discoveryService = discoveryService;
        this.typeRegistry     = typeRegistry;
    }

    @Override
    public AtlasTask.Status perform() throws Exception {
        RequestContext.clear();

        Map<String, Object> params = getTaskDef().getParameters();

        if (MapUtils.isEmpty(params)) {
            LOG.warn("Task: {}: Unable to process task: Parameters is not readable!", getTaskGuid());

            return FAILED;
        }

        String userName = getTaskDef().getCreatedBy();

        if (StringUtils.isEmpty(userName)) {
            LOG.warn("Task: {}: Unable to process task as user name is empty!", getTaskGuid());

            return FAILED;
        }

        RequestContext.get().setUser(userName, null);

        try {
            run(params);

            setStatus(COMPLETE);
        } catch (Exception e) {
            LOG.error("Task: {}: Error performing task!", getTaskGuid(), e);

            setStatus(FAILED);

            throw e;
        } finally {
            RequestContext.clear();
        }

        return getStatus();
    }

    protected void run(Map<String, Object> parameters) throws AtlasBaseException, IOException {
        Map<String, String>              attributeLabelMap;
        AtlasSearchResult                searchResult = null;
        AtlasSearchResult.AtlasQueryType queryType    = null;

        if (parameters.get(SEARCH_TYPE_KEY) == BASIC) {
            String           searchParametersJson = (String) parameters.get(SEARCH_PARAMETERS_JSON_KEY);
            SearchParameters searchParameters     = AtlasJson.fromJson(searchParametersJson, SearchParameters.class);

            searchParameters.setLimit(AtlasConfiguration.SEARCH_MAX_LIMIT.getInt());

            searchResult = discoveryService.searchWithParameters(searchParameters);
            queryType    = BASIC;
        } else if (parameters.get(SEARCH_TYPE_KEY) == DSL) {
            String query          = (String) parameters.get(QUERY_KEY);
            String typeName       = (String) parameters.get(TYPE_NAME_KEY);
            String classification = (String) parameters.get(CLASSIFICATION_KEY);
            int    offset         = (int) parameters.get(OFFSET_KEY);
            String queryStr       = discoveryService.getDslQueryUsingTypeNameClassification(query, typeName, classification);

            searchResult = discoveryService.searchUsingDslQuery(queryStr, AtlasConfiguration.SEARCH_MAX_LIMIT.getInt(), offset);
            queryType    = DSL;
        }

        String attributeLabelMapJson = (String) parameters.get(ATTRIBUTE_LABEL_MAP_KEY);

        attributeLabelMap = AtlasJson.fromJson(attributeLabelMapJson, Map.class);

        if (searchResult != null) {
            generateCSVFileFromSearchResult(searchResult, attributeLabelMap, queryType);
        }
    }

    private void generateCSVFileFromSearchResult(AtlasSearchResult searchResult, Map<String, String> attributeLabelMap, AtlasSearchResult.AtlasQueryType queryType) throws IOException {
        List<AtlasEntityHeader>                 allEntityHeaders      = searchResult.getEntities();
        AtlasSearchResult.AttributeSearchResult attributeSearchResult = searchResult.getAttributes();
        String                                  fileName              = (String) getTaskDef().getParameters().get(CSV_FILE_NAME_KEY);

        if ((queryType == BASIC && CollectionUtils.isEmpty(allEntityHeaders)) || (queryType == DSL && (CollectionUtils.isEmpty(allEntityHeaders) && attributeSearchResult == null))) {
            LOG.info("No result found. Not generating csv file: {}", fileName);

            return;
        }

        File dir = new File(DOWNLOAD_DIR_PATH, RequestContext.getCurrentUser());

        if (!dir.exists()) {
            dir.mkdirs();
        }

        File csvFile = new File(dir, fileName);

        try (FileWriter fileWriter = new FileWriter(csvFile); CSVWriter csvWriter = new CSVWriter(fileWriter)) {
            String[] defaultHeaders = new String[] {"Type name", "Name", "Classifications", "Terms"};
            String[] attributeHeaders;
            int      attrSize;

            if (attributeLabelMap == null) {
                attributeLabelMap = new HashMap<>();
            }

            attributeLabelMap.put("Owner", "owner");
            attributeLabelMap.put("Description", "description");

            Collection<String> attributeHeaderLabels = attributeLabelMap.keySet();

            if (queryType == DSL && (CollectionUtils.isEmpty(allEntityHeaders) && attributeSearchResult != null)) {
                attributeHeaderLabels = attributeSearchResult.getName();
                defaultHeaders        = new String[0];
            }

            attrSize         = (attributeHeaderLabels == null) ? 0 : attributeHeaderLabels.size();
            attributeHeaders = new String[attrSize];

            if (attributeHeaderLabels != null) {
                attributeHeaders = attributeHeaderLabels.toArray(attributeHeaders);
            }

            int      headerSize = attrSize + defaultHeaders.length;
            String[] headers    = new String[headerSize];

            System.arraycopy(defaultHeaders, 0, headers, 0, defaultHeaders.length);

            if (ArrayUtils.isNotEmpty(attributeHeaders)) {
                System.arraycopy(attributeHeaders, 0, headers, defaultHeaders.length, attrSize);
            }

            csvWriter.writeNext(headers);

            String[] entityRecords = new String[headerSize];

            if (CollectionUtils.isNotEmpty(allEntityHeaders)) {
                for (AtlasEntityHeader entityHeader : allEntityHeaders) {
                    entityRecords[0] = entityHeader.getTypeName();
                    entityRecords[1] = entityHeader.getDisplayText() != null ? entityHeader.getDisplayText() : entityHeader.getGuid();
                    entityRecords[2] = String.join(",", entityHeader.getClassificationNames());
                    entityRecords[3] = String.join(",", entityHeader.getMeaningNames());

                    if (MapUtils.isNotEmpty(entityHeader.getAttributes())) {
                        for (int i = defaultHeaders.length; i < headerSize; i++) {
                            Object attrValue = entityHeader.getAttribute(attributeLabelMap.get(headers[i]));

                            if (attrValue instanceof AtlasObjectId) {
                                entityRecords[i] = String.valueOf(((AtlasObjectId) attrValue).getUniqueAttributes().get("qualifiedName"));
                            } else if (attrValue instanceof List) {
                                if (CollectionUtils.isNotEmpty((List<?>) attrValue)) {
                                    List<String> valueList = new ArrayList<>();

                                    for (Object attrVal : (List<?>) attrValue) {
                                        if (attrVal instanceof AtlasObjectId) {
                                            String value = String.valueOf(((AtlasObjectId) attrVal).getUniqueAttributes().get("qualifiedName"));

                                            valueList.add(value);
                                        } else {
                                            valueList.add(String.valueOf(attrVal));
                                        }
                                    }

                                    entityRecords[i] = String.join(",", valueList);
                                }
                            } else {
                                entityRecords[i] = attrValue == null ? EMPTY_STRING : String.valueOf(attrValue);
                            }
                        }
                    }

                    csvWriter.writeNext(entityRecords);
                }
            }

            if (queryType == DSL && attributeSearchResult != null) {
                for (List<Object> resultSet : attributeSearchResult.getValues()) {
                    for (int i = defaultHeaders.length; i < headerSize; i++) {
                        entityRecords[i] = resultSet.get(i) == null ? EMPTY_STRING : String.valueOf(resultSet.get(i));
                    }

                    csvWriter.writeNext(entityRecords);
                }
            }
        }
    }

    static {
        Configuration configuration = null;

        try {
            configuration = ApplicationProperties.get();
        } catch (AtlasException e) {
            LOG.error("Failed to load application properties", e);
        }

        if (configuration != null) {
            DOWNLOAD_DIR_PATH = configuration.getString(DOWNLOAD_DIR_PATH_KEY, DOWNLOAD_DIR_PATH_DEFAULT) + File.separator + CSV_DOWNLOAD_DIR;
        } else {
            DOWNLOAD_DIR_PATH = DOWNLOAD_DIR_PATH_DEFAULT + File.separator + CSV_DOWNLOAD_DIR;
        }
    }
}
