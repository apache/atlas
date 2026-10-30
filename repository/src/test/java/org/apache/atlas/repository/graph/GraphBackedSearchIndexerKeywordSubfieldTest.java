/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.graph;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.repository.Constants;
import org.apache.commons.configuration2.Configuration;
import org.mockito.MockedStatic;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Regression tests for {@link GraphBackedSearchIndexer#needsKeywordSubfield} (OpenSearch text+keyword subfield
 * eligibility). Issue #7: numeric attributes such as {@code spark_process.executionId} ({@code long},
 * {@code searchWeight=10}) must not request a keyword subfield.
 */
public class GraphBackedSearchIndexerKeywordSubfieldTest {
    private MockedStatic<ApplicationProperties> applicationPropertiesMock;
    private Configuration                     configuration;

    @BeforeMethod
    public void setUp() {
        configuration = mock(Configuration.class);
        applicationPropertiesMock = mockStatic(ApplicationProperties.class);
        applicationPropertiesMock.when(ApplicationProperties::get).thenReturn(configuration);
    }

    @AfterMethod
    public void tearDown() {
        if (applicationPropertiesMock != null) {
            applicationPropertiesMock.close();
        }
    }

    private void withOpenSearchBackend(Runnable runnable) {
        when(configuration.getString(ApplicationProperties.INDEX_BACKEND_CONF))
                .thenReturn(ApplicationProperties.INDEX_BACKEND_OPENSEARCH);
        runnable.run();
    }

    private void withSolrBackend(Runnable runnable) {
        when(configuration.getString(ApplicationProperties.INDEX_BACKEND_CONF))
                .thenReturn(ApplicationProperties.INDEX_BACKEND_SOLR);
        runnable.run();
    }

    @Test
    public void sparkProcessExecutionIdLongDoesNotRequestKeywordSubfield() {
        withOpenSearchBackend(() -> {
            String propertyName = "spark_process.executionId";
            assertFalse(GraphBackedSearchIndexer.needsKeywordSubfield(propertyName, false, 10, Long.class),
                    "long executionId must not use text+keyword mapping");
        });
    }

    @DataProvider
    public Object[][] nonStringPrimitiveTypes() {
        return new Object[][] {
                {Integer.class},
                {Long.class},
                {Byte.class},
                {Short.class},
                {Double.class},
                {Float.class},
                {Boolean.class},
        };
    }

    @Test(dataProvider = "nonStringPrimitiveTypes")
    public void highSearchWeightNonStringTypesDoNotRequestKeywordSubfield(Class<?> propertyClass) {
        withOpenSearchBackend(() -> {
            assertFalse(GraphBackedSearchIndexer.needsKeywordSubfield("some_type.attr", false, 10, propertyClass),
                    propertyClass.getSimpleName() + " must not request keyword subfield");
        });
    }

    @Test
    public void highSearchWeightStringTextFieldRequestsKeywordSubfield() {
        withOpenSearchBackend(() -> {
            assertTrue(GraphBackedSearchIndexer.needsKeywordSubfield("hive_table.name", false, 10, String.class));
        });
    }

    @Test
    public void stringIndexTypeFieldDoesNotRequestKeywordSubfield() {
        withOpenSearchBackend(() -> {
            assertFalse(GraphBackedSearchIndexer.needsKeywordSubfield("hive_table.owner", true, 10, String.class),
                    "IndexType.STRING uses native string mapping, not text+keyword");
        });
    }

    @DataProvider
    public Object[][] specialOpenSearchStringProperties() {
        return new Object[][] {
                {Constants.STATE_PROPERTY_KEY},
                {Constants.ENTITY_TYPE_PROPERTY_KEY},
                {Constants.LABELS_PROPERTY_KEY},
                {Constants.CLASSIFICATION_TEXT_KEY},
        };
    }

    @Test(dataProvider = "specialOpenSearchStringProperties")
    public void specialSystemStringPropertiesRetainKeywordSubfield(String propertyName) {
        withOpenSearchBackend(() -> {
            assertTrue(GraphBackedSearchIndexer.needsKeywordSubfield(propertyName, false, 1, String.class),
                    propertyName + " must keep text+keyword subfield on OpenSearch");
        });
    }

    @Test
    public void keywordSubfieldNotRequestedWhenBackendIsSolr() {
        withSolrBackend(() -> {
            assertFalse(GraphBackedSearchIndexer.needsKeywordSubfield(Constants.STATE_PROPERTY_KEY, false, 10, String.class));
            assertFalse(GraphBackedSearchIndexer.needsKeywordSubfield("hive_table.name", false, 10, String.class));
        });
    }

    @Test
    public void searchWeightBelowThresholdDoesNotRequestKeywordSubfieldForStringTextField() {
        withOpenSearchBackend(() -> {
            assertFalse(GraphBackedSearchIndexer.needsKeywordSubfield("hive_table.comment", false, 5, String.class));
        });
    }
}
