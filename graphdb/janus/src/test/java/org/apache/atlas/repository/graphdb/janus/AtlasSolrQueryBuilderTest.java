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
package org.apache.atlas.repository.graphdb.janus;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.discovery.SearchParameters.FilterCriteria;
import org.apache.atlas.model.discovery.SearchParameters.Operator;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class AtlasSolrQueryBuilderTest {
    @Mock
    private AtlasEntityType hiveTableEntityTypeMock;

    @Mock
    private AtlasEntityType hiveTableEntityTypeMock2;

    @Mock
    private AtlasStructType.AtlasAttribute nameAttributeMock;

    @Mock
    private AtlasStructType.AtlasAttribute commentAttributeMock;

    @Mock
    private AtlasStructType.AtlasAttribute stateAttributeMock;

    @Mock
    private AtlasStructType.AtlasAttribute descrptionAttributeMock;

    @Mock
    private AtlasStructType.AtlasAttribute createdAttributeMock;

    @Mock
    private AtlasStructType.AtlasAttribute startedAttributeMock;

    @Mock
    private AtlasStructType.AtlasAttribute entitypeAttributeMock;

    @Mock
    private AtlasStructType.AtlasAttribute qualifiedNameAttributeMock;

    @Mock
    private AtlasStructDef.AtlasAttributeDef stringAttributeDef;

    @Mock
    private AtlasStructDef.AtlasAttributeDef textAttributeDef;

    @Mock
    private AtlasStructDef.AtlasAttributeDef nonStringAttributeDef;

    private final Map<String, String> indexFieldNamesMap = new HashMap<>();

    @BeforeTest
    public void setup() {
        AtlasTypesDef typesDef = new AtlasTypesDef();
        MockitoAnnotations.openMocks(this);
        when(hiveTableEntityTypeMock.getAttribute("name")).thenReturn(nameAttributeMock);
        when(hiveTableEntityTypeMock.getAttribute("comment")).thenReturn(commentAttributeMock);
        when(hiveTableEntityTypeMock.getAttribute("__state")).thenReturn(stateAttributeMock);
        when(hiveTableEntityTypeMock.getAttribute("description")).thenReturn(descrptionAttributeMock);
        when(hiveTableEntityTypeMock.getAttribute("created")).thenReturn(createdAttributeMock);
        when(hiveTableEntityTypeMock.getAttribute("started")).thenReturn(startedAttributeMock);
        when(hiveTableEntityTypeMock.getAttribute("Constants.ENTITY_TYPE_PROPERTY_KEY")).thenReturn(entitypeAttributeMock);
        when(hiveTableEntityTypeMock.getAttribute("qualifiedName")).thenReturn(qualifiedNameAttributeMock);

        when(hiveTableEntityTypeMock.getAttributeDef("name")).thenReturn(stringAttributeDef);
        when(hiveTableEntityTypeMock.getAttributeDef("comment")).thenReturn(stringAttributeDef);
        when(hiveTableEntityTypeMock.getAttributeDef("description")).thenReturn(stringAttributeDef);
        when(hiveTableEntityTypeMock.getAttributeDef("qualifiedName")).thenReturn(textAttributeDef);

        when(nonStringAttributeDef.getTypeName()).thenReturn(AtlasBaseTypeDef.ATLAS_TYPE_INT);
        when(stringAttributeDef.getTypeName()).thenReturn(AtlasBaseTypeDef.ATLAS_TYPE_STRING);
        when(textAttributeDef.getTypeName()).thenReturn(AtlasBaseTypeDef.ATLAS_TYPE_STRING);

        when(stringAttributeDef.getIndexType()).thenReturn(AtlasStructDef.AtlasAttributeDef.IndexType.STRING);

        indexFieldNamesMap.put("name", "name_index");
        indexFieldNamesMap.put("comment", "comment_index");
        indexFieldNamesMap.put("__state", "__state_index");
        indexFieldNamesMap.put("description", "descrption__index");
        indexFieldNamesMap.put("created", "created__index");
        indexFieldNamesMap.put("started", "started__index");
        indexFieldNamesMap.put(Constants.ENTITY_TYPE_PROPERTY_KEY, Constants.ENTITY_TYPE_PROPERTY_KEY + "__index");

        when(hiveTableEntityTypeMock.getTypeName()).thenReturn("hive_table");
        when(hiveTableEntityTypeMock2.getTypeName()).thenReturn("hive_db");

        when(nameAttributeMock.getIndexFieldName()).thenReturn("name_index");
        when(commentAttributeMock.getIndexFieldName()).thenReturn("comment_index");
        when(stateAttributeMock.getIndexFieldName()).thenReturn("__state_index");
        when(descrptionAttributeMock.getIndexFieldName()).thenReturn("descrption__index");
        when(createdAttributeMock.getIndexFieldName()).thenReturn("created__index");
        when(startedAttributeMock.getIndexFieldName()).thenReturn("started__index");
        when(entitypeAttributeMock.getIndexFieldName()).thenReturn(Constants.ENTITY_TYPE_PROPERTY_KEY + "__index");
        when(qualifiedNameAttributeMock.getIndexFieldName()).thenReturn("qualifiedName" + "__index");
    }

    @Test
    public void testGenerateSolrQueryString() throws IOException, AtlasBaseException {
        final String          fileName  = "src/test/resources/searchparameters2OR.json";
        AtlasSolrQueryBuilder underTest = new AtlasSolrQueryBuilder();

        processSearchParameters(fileName, underTest);

        assertEquals(underTest.build(), "+t  AND  -__state_index:DELETED AND  +__typeName__index:(hive_table )  AND  ( ( +name_index:t10  ) OR ( +comment_index:*t10*  ) )");
    }

    @Test
    public void testGenerateSolrQueryString2() throws IOException, AtlasBaseException {
        final String          fileName  = "src/test/resources/searchparameters1OR.json";
        AtlasSolrQueryBuilder underTest = new AtlasSolrQueryBuilder();

        processSearchParameters(fileName, underTest);

        assertEquals(underTest.build(), "+t  AND  -__state_index:DELETED AND  +__typeName__index:(hive_table )  AND  ( ( +name_index:t10  ) )");
    }

    @Test
    public void testGenerateSolrQueryString3() throws IOException, AtlasBaseException {
        final String          fileName  = "src/test/resources/searchparameters2AND.json";
        AtlasSolrQueryBuilder underTest = new AtlasSolrQueryBuilder();

        processSearchParameters(fileName, underTest);

        assertEquals(underTest.build(), "+t  AND  -__state_index:DELETED AND  +__typeName__index:(hive_table )  AND  ( ( +name_index:t10  ) AND ( +comment_index:*t10*  ) )");
    }

    @Test
    public void testGenerateSolrQueryString4() throws IOException, AtlasBaseException {
        final String          fileName  = "src/test/resources/searchparameters1AND.json";
        AtlasSolrQueryBuilder underTest = new AtlasSolrQueryBuilder();

        processSearchParameters(fileName, underTest);

        assertEquals(underTest.build(), "+t  AND  -__state_index:DELETED AND  +__typeName__index:(hive_table )  AND  ( ( +name_index:t10  ) )");
    }

    @Test
    public void testGenerateSolrQueryString5() throws IOException, AtlasBaseException {
        final String          fileName  = "src/test/resources/searchparameters0.json";
        AtlasSolrQueryBuilder underTest = new AtlasSolrQueryBuilder();

        processSearchParameters(fileName, underTest);

        assertEquals(underTest.build(), "+t  AND  -__state_index:DELETED AND  +__typeName__index:(hive_table )  AND  ( +name_index:t10  )");
    }

    @Test
    public void testGenerateSolrQueryString6() throws IOException, AtlasBaseException {
        final String          fileName  = "src/test/resources/searchparameters3.json";
        AtlasSolrQueryBuilder underTest = new AtlasSolrQueryBuilder();

        processSearchParameters(fileName, underTest);

        assertEquals(underTest.build(), "+t10  AND  -__state_index:DELETED AND  +__typeName__index:(hive_table )  AND  ( ( +comment_index:*United\\ States*  ) AND ( +descrption__index:*nothing*  ) AND ( +name_index:*t100*  ) )");
    }

    @Test
    public void testGenerateSolrQueryStringGT() throws IOException, AtlasBaseException {
        final String          fileName  = "src/test/resources/searchparametersGT.json";
        AtlasSolrQueryBuilder underTest = new AtlasSolrQueryBuilder();

        processSearchParameters(fileName, underTest);

        assertEquals(underTest.build(), "+t10  AND  -__state_index:DELETED AND  +__typeName__index:(hive_table )  AND  ( ( +created__index:{ 100 TO * ]  ) )");
    }

    @Test
    public void testGenerateSolrQueryStringGTE() throws IOException, AtlasBaseException {
        final String          fileName  = "src/test/resources/searchparametersGTE.json";
        AtlasSolrQueryBuilder underTest = new AtlasSolrQueryBuilder();

        processSearchParameters(fileName, underTest);

        assertEquals(underTest.build(), "+t10  AND  -__state_index:DELETED AND  +__typeName__index:(hive_table )  AND  ( ( +created__index:[ 100 TO * ]  ) AND ( +started__index:[ 100 TO * ]  ) )");
    }

    @Test
    public void testGenerateSolrQueryStringLT() throws IOException, AtlasBaseException {
        final String          fileName  = "src/test/resources/searchparametersLT.json";
        AtlasSolrQueryBuilder underTest = new AtlasSolrQueryBuilder();

        processSearchParameters(fileName, underTest);

        assertEquals(underTest.build(), "+t10  AND  -__state_index:DELETED AND  +__typeName__index:(hive_table )  AND  ( ( +created__index:[ * TO 100}  ) )");
    }

    @Test
    public void testGenerateSolrQueryStringLE() throws IOException, AtlasBaseException {
        final String          fileName  = "src/test/resources/searchparametersLTE.json";
        AtlasSolrQueryBuilder underTest = new AtlasSolrQueryBuilder();

        processSearchParameters(fileName, underTest);

        assertEquals(underTest.build(), "+t10  AND  -__state_index:DELETED AND  +__typeName__index:(hive_table )  AND  ( ( +created__index:[ * TO 100 ]  ) AND ( +started__index:[ * TO 100 ]  ) )");
    }

    @Test
    public void testGenerateSolrQueryStartsWith() throws IOException, AtlasBaseException {
        final String          fileName  = "src/test/resources/searchparametersStartsWith.json";
        AtlasSolrQueryBuilder underTest = new AtlasSolrQueryBuilder();

        processSearchParameters(fileName, underTest);

        assertEquals(underTest.build(), " -__state_index:DELETED AND  +__typeName__index:(hive_table )  AND  ( ( +qualifiedName__index:testdb.t1*  ) )");
    }

    @Test
    public void testGenerateSolrQueryString2TypeNames() throws IOException, AtlasBaseException {
        final String          fileName  = "src/test/resources/searchparameters2Types.json";
        AtlasSolrQueryBuilder underTest = new AtlasSolrQueryBuilder();

        processSearchParametersForMultipleTypeNames(fileName, underTest);

        assertEquals(underTest.build(), "+t  AND  -__state_index:DELETED AND  +__typeName__index:(hive_table hive_db ) ");
    }

    private void validateOrder(List<String> topTerms, int... indices) {
        assertEquals(topTerms.size(), indices.length);
        int i = 0;
        for (String term : topTerms) {
            assertEquals(Integer.toString(indices[i++]), term);
        }
        assertEquals(topTerms.size(), indices.length);
    }

    private Map<String, AtlasJanusGraphIndexClient.TermFreq> generateTerms(int... termFreqs) {
        int                                              i     = 0;
        Map<String, AtlasJanusGraphIndexClient.TermFreq> terms = new HashMap<>();
        for (int count : termFreqs) {
            AtlasJanusGraphIndexClient.TermFreq termFreq1 = new AtlasJanusGraphIndexClient.TermFreq(Integer.toString(i++), count);
            terms.put(termFreq1.getTerm(), termFreq1);
        }
        return terms;
    }

    private void processSearchParameters(String fileName, AtlasSolrQueryBuilder underTest) throws IOException {
        ObjectMapper     mapper           = new ObjectMapper();
        SearchParameters searchParameters = mapper.readValue(new FileInputStream(fileName), SearchParameters.class);

        Set<AtlasEntityType> hiveTableEntityTypeMocks = new HashSet<>();
        hiveTableEntityTypeMocks.add(hiveTableEntityTypeMock);
        underTest.withEntityTypes(hiveTableEntityTypeMocks)
                .withQueryString(searchParameters.getQuery())
                .withCriteria(searchParameters.getEntityFilters())
                .withExcludedDeletedEntities(searchParameters.getExcludeDeletedEntities())
                .withCommonIndexFieldNames(indexFieldNamesMap);
    }

    private void processSearchParametersForMultipleTypeNames(String fileName, AtlasSolrQueryBuilder underTest) throws IOException {
        ObjectMapper     mapper           = new ObjectMapper();
        SearchParameters searchParameters = mapper.readValue(new FileInputStream(fileName), SearchParameters.class);

        Set<AtlasEntityType> hiveTableEntityTypeMocks = new HashSet<>();
        hiveTableEntityTypeMocks.add(hiveTableEntityTypeMock);
        hiveTableEntityTypeMocks.add(hiveTableEntityTypeMock2);
        underTest.withEntityTypes(hiveTableEntityTypeMocks)
                .withQueryString(searchParameters.getQuery())
                .withCriteria(searchParameters.getEntityFilters())
                .withExcludedDeletedEntities(searchParameters.getExcludeDeletedEntities())
                .withCommonIndexFieldNames(indexFieldNamesMap);
    }

    @Test
    public void testBuildWithEmptyQueryString() throws AtlasBaseException {
        AtlasSolrQueryBuilder underTest = new AtlasSolrQueryBuilder();

        Set<AtlasEntityType> entityTypes = new HashSet<>();
        entityTypes.add(hiveTableEntityTypeMock);

        underTest.withEntityTypes(entityTypes)
                .withQueryString("")
                .withExcludedDeletedEntities(true)
                .withCommonIndexFieldNames(indexFieldNamesMap);

        String result = underTest.build();

        assertTrue(result.contains("-__state_index:DELETED"));
        assertTrue(result.contains("+__typeName__index:(hive_table )"));
    }

    @Test
    public void testBuildWithNullQueryString() throws AtlasBaseException {
        AtlasSolrQueryBuilder underTest = new AtlasSolrQueryBuilder();

        Set<AtlasEntityType> entityTypes = new HashSet<>();
        entityTypes.add(hiveTableEntityTypeMock);

        underTest.withEntityTypes(entityTypes)
                .withQueryString(null)
                .withExcludedDeletedEntities(true)
                .withCommonIndexFieldNames(indexFieldNamesMap);

        String result = underTest.build();

        assertTrue(result.contains("-__state_index:DELETED"));
        assertTrue(result.contains("+__typeName__index:(hive_table )"));
    }

    @Test
    public void testBuildWithNoEntityTypes() throws AtlasBaseException {
        AtlasSolrQueryBuilder underTest = new AtlasSolrQueryBuilder();

        underTest.withQueryString("test")
                .withExcludedDeletedEntities(true)
                .withCommonIndexFieldNames(indexFieldNamesMap);

        String result = underTest.build();

        assertEquals(result, "+test  AND  -__state_index:DELETED");
    }

    @Test
    public void testBuildWithIncludeSubtypes() throws AtlasBaseException {
        AtlasSolrQueryBuilder underTest = new AtlasSolrQueryBuilder();

        Set<AtlasEntityType> entityTypes = new HashSet<>();
        entityTypes.add(hiveTableEntityTypeMock);

        Set<String> allTypes = new HashSet<>();
        allTypes.add("hive_table");
        allTypes.add("hive_table_subtype");
        when(hiveTableEntityTypeMock.getTypeAndAllSubTypes()).thenReturn(allTypes);

        underTest.withEntityTypes(entityTypes)
                .withIncludeSubTypes(true)
                .withCommonIndexFieldNames(indexFieldNamesMap);

        String result = underTest.build();

        assertTrue(result.contains("hive_table"));
        assertTrue(result.contains("hive_table_subtype"));
    }

    @Test
    public void testWithCriteriaEqualOperator() throws AtlasBaseException {
        AtlasSolrQueryBuilder underTest = new AtlasSolrQueryBuilder();

        Set<AtlasEntityType> entityTypes = new HashSet<>();
        entityTypes.add(hiveTableEntityTypeMock);

        SearchParameters.FilterCriteria criteria = new SearchParameters.FilterCriteria();
        criteria.setAttributeName("name");
        criteria.setAttributeValue("testValue");
        criteria.setOperator(SearchParameters.Operator.EQ);

        underTest.withEntityTypes(entityTypes)
                .withCriteria(criteria)
                .withCommonIndexFieldNames(indexFieldNamesMap);

        String result = underTest.build();

        assertTrue(result.contains("+name_index:testValue"));
    }

    @Test
    public void testWithCriteriaNotEqualOperator() throws AtlasBaseException {
        AtlasSolrQueryBuilder underTest = new AtlasSolrQueryBuilder();

        Set<AtlasEntityType> entityTypes = new HashSet<>();
        entityTypes.add(hiveTableEntityTypeMock);

        SearchParameters.FilterCriteria criteria = new SearchParameters.FilterCriteria();
        criteria.setAttributeName("name");
        criteria.setAttributeValue("testValue");
        criteria.setOperator(SearchParameters.Operator.NEQ);

        underTest.withEntityTypes(entityTypes)
                .withCriteria(criteria)
                .withCommonIndexFieldNames(indexFieldNamesMap);

        String result = underTest.build();

        assertTrue(result.contains("*:* -name_index:testValue"));
    }

    @Test
    public void testWithCriteriaIsNullOperator() throws AtlasBaseException {
        AtlasSolrQueryBuilder underTest = new AtlasSolrQueryBuilder();

        Set<AtlasEntityType> entityTypes = new HashSet<>();
        entityTypes.add(hiveTableEntityTypeMock);

        SearchParameters.FilterCriteria criteria = new SearchParameters.FilterCriteria();
        criteria.setAttributeName("name");
        criteria.setOperator(SearchParameters.Operator.IS_NULL);

        underTest.withEntityTypes(entityTypes)
                .withCriteria(criteria)
                .withCommonIndexFieldNames(indexFieldNamesMap);

        String result = underTest.build();

        assertTrue(result.contains("-name_index:*"));
    }

    @Test
    public void testWithCriteriaIsNotNullOperator() throws AtlasBaseException {
        AtlasSolrQueryBuilder underTest = new AtlasSolrQueryBuilder();

        Set<AtlasEntityType> entityTypes = new HashSet<>();
        entityTypes.add(hiveTableEntityTypeMock);

        SearchParameters.FilterCriteria criteria = new SearchParameters.FilterCriteria();
        criteria.setAttributeName("name");
        criteria.setOperator(SearchParameters.Operator.NOT_NULL);

        underTest.withEntityTypes(entityTypes)
                .withCriteria(criteria)
                .withCommonIndexFieldNames(indexFieldNamesMap);

        String result = underTest.build();

        assertTrue(result.contains("+name_index:*"));
    }

    @Test
    public void testWithCriteriaEndsWithOperator() throws AtlasBaseException {
        AtlasSolrQueryBuilder underTest = new AtlasSolrQueryBuilder();

        Set<AtlasEntityType> entityTypes = new HashSet<>();
        entityTypes.add(hiveTableEntityTypeMock);

        SearchParameters.FilterCriteria criteria = new SearchParameters.FilterCriteria();
        criteria.setAttributeName("name");
        criteria.setAttributeValue("testValue");
        criteria.setOperator(SearchParameters.Operator.ENDS_WITH);

        underTest.withEntityTypes(entityTypes)
                .withCriteria(criteria)
                .withCommonIndexFieldNames(indexFieldNamesMap);

        String result = underTest.build();

        assertTrue(result.contains("+name_index:*testValue"));
    }

    @Test
    public void testWithCriteriaNotContainsOperator() throws AtlasBaseException {
        AtlasSolrQueryBuilder underTest = new AtlasSolrQueryBuilder();

        Set<AtlasEntityType> entityTypes = new HashSet<>();
        entityTypes.add(hiveTableEntityTypeMock);

        SearchParameters.FilterCriteria criteria = new SearchParameters.FilterCriteria();
        criteria.setAttributeName("name");
        criteria.setAttributeValue("testValue");
        criteria.setOperator(SearchParameters.Operator.NOT_CONTAINS);

        underTest.withEntityTypes(entityTypes)
                .withCriteria(criteria)
                .withCommonIndexFieldNames(indexFieldNamesMap);

        String result = underTest.build();

        assertTrue(result.contains("*:* -name_index:*testValue*"));
    }

    @Test
    public void testWithCriteriaCustomAttributesContains() throws AtlasBaseException {
        AtlasSolrQueryBuilder underTest = new AtlasSolrQueryBuilder();

        Set<AtlasEntityType> entityTypes = new HashSet<>();
        entityTypes.add(hiveTableEntityTypeMock);

        SearchParameters.FilterCriteria criteria = new SearchParameters.FilterCriteria();
        criteria.setAttributeName(Constants.CUSTOM_ATTRIBUTES_PROPERTY_KEY);
        criteria.setAttributeValue("key1=value1");
        criteria.setOperator(SearchParameters.Operator.CONTAINS);

        // Mock custom attribute
        AtlasStructType.AtlasAttribute customAttr = mock(AtlasStructType.AtlasAttribute.class);
        when(hiveTableEntityTypeMock.getAttribute(Constants.CUSTOM_ATTRIBUTES_PROPERTY_KEY)).thenReturn(customAttr);
        when(customAttr.getIndexFieldName()).thenReturn("custom_attr_index");

        underTest.withEntityTypes(entityTypes)
                .withCriteria(criteria)
                .withCommonIndexFieldNames(indexFieldNamesMap);

        String result = underTest.build();

        assertTrue(result.contains("custom_attr_index"));
    }

    @Test
    public void testWithCriteriaCustomAttributesNotContains() throws AtlasBaseException {
        AtlasSolrQueryBuilder underTest = new AtlasSolrQueryBuilder();

        Set<AtlasEntityType> entityTypes = new HashSet<>();
        entityTypes.add(hiveTableEntityTypeMock);

        SearchParameters.FilterCriteria criteria = new SearchParameters.FilterCriteria();
        criteria.setAttributeName(Constants.CUSTOM_ATTRIBUTES_PROPERTY_KEY);
        criteria.setAttributeValue("key1=value1");
        criteria.setOperator(SearchParameters.Operator.NOT_CONTAINS);

        // Mock custom attribute
        AtlasStructType.AtlasAttribute customAttr = mock(AtlasStructType.AtlasAttribute.class);
        when(hiveTableEntityTypeMock.getAttribute(Constants.CUSTOM_ATTRIBUTES_PROPERTY_KEY)).thenReturn(customAttr);
        when(customAttr.getIndexFieldName()).thenReturn("custom_attr_index");

        underTest.withEntityTypes(entityTypes)
                .withCriteria(criteria)
                .withCommonIndexFieldNames(indexFieldNamesMap);

        String result = underTest.build();

        assertTrue(result.contains("*:* -custom_attr_index"));
    }

    @Test
    public void testWithCriteriaNestedCriteria() throws AtlasBaseException {
        AtlasSolrQueryBuilder underTest = new AtlasSolrQueryBuilder();

        Set<AtlasEntityType> entityTypes = new HashSet<>();
        entityTypes.add(hiveTableEntityTypeMock);

        SearchParameters.FilterCriteria parentCriteria = new SearchParameters.FilterCriteria();
        parentCriteria.setCondition(SearchParameters.FilterCriteria.Condition.AND);

        SearchParameters.FilterCriteria childCriteria1 = new SearchParameters.FilterCriteria();
        childCriteria1.setAttributeName("name");
        childCriteria1.setAttributeValue("testValue1");
        childCriteria1.setOperator(SearchParameters.Operator.EQ);

        SearchParameters.FilterCriteria childCriteria2 = new FilterCriteria();
        childCriteria2.setAttributeName("comment");
        childCriteria2.setAttributeValue("testValue2");
        childCriteria2.setOperator(Operator.EQ);

        List<FilterCriteria> criterion = new ArrayList<>();
        criterion.add(childCriteria1);
        criterion.add(childCriteria2);
        parentCriteria.setCriterion(criterion);

        underTest.withEntityTypes(entityTypes)
                .withCriteria(parentCriteria)
                .withCommonIndexFieldNames(indexFieldNamesMap);

        String result = underTest.build();

        assertTrue(result.contains("+name_index:testValue1"));
        assertTrue(result.contains("+comment_index:testValue2"));
        assertTrue(result.contains("AND"));
    }

    @Test
    public void testWithCriteriaORCondition() throws AtlasBaseException {
        AtlasSolrQueryBuilder underTest = new AtlasSolrQueryBuilder();

        Set<AtlasEntityType> entityTypes = new HashSet<>();
        entityTypes.add(hiveTableEntityTypeMock);

        FilterCriteria parentCriteria = new FilterCriteria();
        parentCriteria.setCondition(FilterCriteria.Condition.OR);

        FilterCriteria childCriteria1 = new FilterCriteria();
        childCriteria1.setAttributeName("name");
        childCriteria1.setAttributeValue("testValue1");
        childCriteria1.setOperator(Operator.EQ);

        FilterCriteria childCriteria2 = new FilterCriteria();
        childCriteria2.setAttributeName("comment");
        childCriteria2.setAttributeValue("testValue2");
        childCriteria2.setOperator(Operator.EQ);

        List<FilterCriteria> criterion = new ArrayList<>();
        criterion.add(childCriteria1);
        criterion.add(childCriteria2);
        parentCriteria.setCriterion(criterion);

        underTest.withEntityTypes(entityTypes)
                .withCriteria(parentCriteria)
                .withCommonIndexFieldNames(indexFieldNamesMap);

        String result = underTest.build();

        assertTrue(result.contains("+name_index:testValue1"));
        assertTrue(result.contains("+comment_index:testValue2"));
        assertTrue(result.contains("OR"));
    }

    @Test(expectedExceptions = AtlasBaseException.class)
    public void testWithCriteriaUnsupportedOperator() throws AtlasBaseException {
        AtlasSolrQueryBuilder underTest = new AtlasSolrQueryBuilder();

        Set<AtlasEntityType> entityTypes = new HashSet<>();
        entityTypes.add(hiveTableEntityTypeMock);

        FilterCriteria criteria = new FilterCriteria();
        criteria.setAttributeName("name");
        criteria.setAttributeValue("testValue");
        criteria.setOperator(Operator.IN); // Unsupported operator

        underTest.withEntityTypes(entityTypes)
                .withCriteria(criteria)
                .withCommonIndexFieldNames(indexFieldNamesMap);

        underTest.build();
    }

    @Test(expectedExceptions = AtlasBaseException.class)
    public void testWithCriteriaInvalidAttribute() throws AtlasBaseException {
        AtlasSolrQueryBuilder underTest = new AtlasSolrQueryBuilder();

        Set<AtlasEntityType> entityTypes = new HashSet<>();
        entityTypes.add(hiveTableEntityTypeMock);

        FilterCriteria criteria = new FilterCriteria();
        criteria.setAttributeName("invalidAttribute");
        criteria.setAttributeValue("testValue");
        criteria.setOperator(Operator.EQ);

        when(hiveTableEntityTypeMock.getAttribute("invalidAttribute")).thenReturn(null);

        underTest.withEntityTypes(entityTypes)
                .withCriteria(criteria)
                .withCommonIndexFieldNames(indexFieldNamesMap);

        underTest.build();
    }

    @Test(expectedExceptions = AtlasBaseException.class)
    public void testWithCriteriaNonIndexedAttribute() throws AtlasBaseException {
        AtlasSolrQueryBuilder underTest = new AtlasSolrQueryBuilder();

        Set<AtlasEntityType> entityTypes = new HashSet<>();
        entityTypes.add(hiveTableEntityTypeMock);

        FilterCriteria criteria = new FilterCriteria();
        criteria.setAttributeName("nonIndexedAttr");
        criteria.setAttributeValue("testValue");
        criteria.setOperator(Operator.EQ);

        AtlasStructType.AtlasAttribute nonIndexedAttr = mock(AtlasStructType.AtlasAttribute.class);
        when(hiveTableEntityTypeMock.getAttribute("nonIndexedAttr")).thenReturn(nonIndexedAttr);
        when(nonIndexedAttr.getIndexFieldName()).thenReturn(null);

        underTest.withEntityTypes(entityTypes)
                .withCriteria(criteria)
                .withCommonIndexFieldNames(indexFieldNamesMap);

        underTest.build();
    }

    @Test
    public void testTokenizedCharacterHandling() throws AtlasBaseException {
        AtlasSolrQueryBuilder underTest = new AtlasSolrQueryBuilder();

        Set<AtlasEntityType> entityTypes = new HashSet<>();
        entityTypes.add(hiveTableEntityTypeMock);

        FilterCriteria criteria = new FilterCriteria();
        criteria.setAttributeName("qualifiedName");
        criteria.setAttributeValue("test.value");
        criteria.setOperator(Operator.CONTAINS);

        when(hiveTableEntityTypeMock.getAttributeDef("qualifiedName")).thenReturn(textAttributeDef);
        when(textAttributeDef.getIndexType()).thenReturn(null); // No index type to trigger tokenized char handling

        underTest.withEntityTypes(entityTypes)
                .withCriteria(criteria)
                .withCommonIndexFieldNames(indexFieldNamesMap);

        String result = underTest.build();

        assertTrue(result.contains("qualifiedName__index"));
    }

    @Test(expectedExceptions = AtlasBaseException.class)
    public void testDropDeletedEntitiesWithMissingStateIndex() throws AtlasBaseException {
        AtlasSolrQueryBuilder underTest = new AtlasSolrQueryBuilder();

        Map<String, String> emptyIndexFieldNamesMap = new HashMap<>();

        underTest.withExcludedDeletedEntities(true)
                .withCommonIndexFieldNames(emptyIndexFieldNamesMap);

        underTest.build();
    }
}
