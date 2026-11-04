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
package org.apache.atlas.repository.graphdb.janus;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * Approach A pins a unique, suffixed mapped name ({@code Type.attr__1}) for an attribute that was deleted and recreated.
 * Everything that turns an Atlas index field name into an OpenSearch field must treat that name exactly like any other:
 * the {@code .}-to-bullet conversion, the keyword-subfield registry, the native {@code __s_} keyword detection and the
 * {@code term}/aggregation field resolution. Retired and live fields must never be confused with each other.
 */
public class AtlasOpenSearchSuffixedFieldNameTest {
    private static final String BULLET = "\u2022";

    @BeforeMethod
    @AfterMethod
    public void reset() {
        AtlasOpenSearchIndexClient.clearKeywordSubfieldFieldsForTests();
    }

    @Test
    public void suffixedNameIsConvertedLikeAnyOtherFieldName() {
        assertEquals(AtlasOpenSearchIndexClient.toOpenSearchFieldName("hive_table.name__1"), "hive_table" + BULLET + "name__1");
        assertEquals(AtlasOpenSearchQueryBuilder.toOsField("hive_table.name__12"), "hive_table" + BULLET + "name__12");
        // already physical -> unchanged
        assertEquals(AtlasOpenSearchIndexClient.toOpenSearchFieldName("hive_table" + BULLET + "name__1"), "hive_table" + BULLET + "name__1");
    }

    @Test
    public void keywordSubfieldRegistryIsKeyedByTheExactSuffixedName() {
        AtlasOpenSearchIndexClient.registerKeywordSubfieldField("hive_table.name");
        AtlasOpenSearchIndexClient.registerKeywordSubfieldField("hive_table.name__1");

        assertTrue(AtlasOpenSearchIndexClient.usesKeywordSubfield("hive_table.name"));
        assertTrue(AtlasOpenSearchIndexClient.usesKeywordSubfield("hive_table.name__1"));
        assertFalse(AtlasOpenSearchIndexClient.usesKeywordSubfield("hive_table.name__2"),
                "a later generation is not keyword-capable until it is registered itself");

        assertEquals(AtlasOpenSearchIndexClient.resolveTermsAggregationFieldName("hive_table.name__1"),
                "hive_table" + BULLET + "name__1.keyword");
        assertEquals(AtlasOpenSearchQueryBuilder.toKeywordField("hive_table.name__1"),
                "hive_table" + BULLET + "name__1.keyword");
    }

    @Test
    public void retiredTextFieldDoesNotMakeRecreatedNumericFieldKeywordCapable() {
        // generation 0 was text+keyword; generation 1 was recreated as long and registers no keyword subfield
        AtlasOpenSearchIndexClient.registerKeywordSubfieldField("spark_process.executionId");

        assertNull(AtlasOpenSearchIndexClient.resolveTermsAggregationFieldName("spark_process.executionId__1"));
        assertEquals(AtlasOpenSearchQueryBuilder.toKeywordField("spark_process.executionId__1"),
                "spark_process" + BULLET + "executionId__1", "no .keyword subfield may be appended to a numeric field");
    }

    @Test
    public void suffixedNativeKeywordStringFieldIsStillDetectedAsNativeKeyword() {
        String generation0 = "hive_table.__s_comment";
        String generation1 = "hive_table.__s_comment__1";

        assertEquals(AtlasOpenSearchIndexClient.resolveTermsAggregationFieldName(generation0), "hive_table" + BULLET + "__s_comment");
        assertEquals(AtlasOpenSearchIndexClient.resolveTermsAggregationFieldName(generation1), "hive_table" + BULLET + "__s_comment__1");
        assertFalse(AtlasOpenSearchIndexClient.usesKeywordSubfield(generation1), "native keyword fields have no .keyword subfield");
    }

    @Test
    public void suffixDoesNotCreateFalseNativeKeywordMatch() {
        // "__1" must not be mistaken for the "__s_" native-keyword prefix
        assertNull(AtlasOpenSearchIndexClient.resolveTermsAggregationFieldName("hive_table.size__1"));
        assertNull(AtlasOpenSearchIndexClient.resolveTermsAggregationFieldName("hive_table.s__1"));
    }
}
