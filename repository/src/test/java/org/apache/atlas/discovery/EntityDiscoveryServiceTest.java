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
package org.apache.atlas.discovery;

import org.apache.atlas.TestModules;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.impexp.AtlasImportRequest;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.repository.impexp.ImportService;
import org.apache.atlas.repository.impexp.ZipSource;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.TestResourceFileUtils;
import org.apache.commons.lang.StringUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.io.IOException;

import static org.apache.atlas.repository.impexp.ZipFileResourceTestUtils.LOG;
import static org.apache.atlas.repository.impexp.ZipFileResourceTestUtils.loadModelFromJson;
import static org.testng.Assert.*;
import static org.testng.Assert.fail;

@Guice(modules = TestModules.TestOnlyModule.class)
public class EntityDiscoveryServiceTest {

    private final String TEST_TYPE                = "test";
    private final String TEST_TYPE1               = "test1";
    private final String TEST_TYPE2               = "test2";
    private final String TEST_TYPE3               = "test3";
    private final String TEST_TYPE_WITH_SUB_TYPES = "testTypeWithSubTypes";
    private AtlasTypeRegistry typeRegistry = new AtlasTypeRegistry();

    AtlasEntityDef typeTest         = null;
    AtlasEntityDef typeTest1        = null;
    AtlasEntityDef typeTest2        = null;
    AtlasEntityDef typeTest3        = null;
    AtlasEntityDef typeWithSubTypes = null;

    private final int maxTypesStrLengthInIdxQuery = 55;

    @Inject
    EntityDiscoveryService discoveryService;
    @Inject
    AtlasTypeDefStore typeDefStore;
    @Inject
    AtlasEntityStore entityStore;
    @Inject
    ImportService importService;


    @BeforeClass
    public void init() throws AtlasBaseException {
        typeTest         = new AtlasEntityDef(TEST_TYPE);
        typeTest1        = new AtlasEntityDef(TEST_TYPE1);
        typeTest2        = new AtlasEntityDef(TEST_TYPE2);
        typeTest3        = new AtlasEntityDef(TEST_TYPE3);
        typeWithSubTypes = new AtlasEntityDef(TEST_TYPE_WITH_SUB_TYPES);

        typeTest1.addSuperType(TEST_TYPE_WITH_SUB_TYPES);
        typeTest2.addSuperType(TEST_TYPE_WITH_SUB_TYPES);
        typeTest3.addSuperType(TEST_TYPE_WITH_SUB_TYPES);

        AtlasTypeRegistry.AtlasTransientTypeRegistry ttr = typeRegistry.lockTypeRegistryForUpdate();

        ttr.addType(typeTest);
        ttr.addType(typeWithSubTypes);
        ttr.addType(typeTest1);
        ttr.addType(typeTest2);
        ttr.addType(typeTest3);

        typeRegistry.releaseTypeRegistryForUpdate(ttr, true);
    }

    @Test
    public void getSubTypesForType_NullStringReturnsEmptyString() throws Exception {
        invokeGetSubTypesForType(null, maxTypesStrLengthInIdxQuery);
    }

    @Test
    public void getSubTypesForType_BlankStringReturnsEmptyString() throws Exception {
        invokeGetSubTypesForType(" ", maxTypesStrLengthInIdxQuery);
    }

    @Test
    public void getSubTypesForType_EmptyStringReturnsEmptyString() throws Exception {
        invokeGetSubTypesForType("", maxTypesStrLengthInIdxQuery);
    }

    @Test
    public void getSubTypeForTypeWithNoSubType_ReturnsTypeString() throws Exception {
        String s = invokeGetSubTypesForType(TEST_TYPE, 10);

        assertEquals(s, "(" + TEST_TYPE + ")");
    }

    @Test
    public void getSubTypeForTypeWithSubTypes_ReturnsOrClause() throws Exception {
        String s = invokeGetSubTypesForType(TEST_TYPE_WITH_SUB_TYPES, maxTypesStrLengthInIdxQuery);

        assertTrue(s.startsWith("("));
        assertTrue(s.contains(TEST_TYPE_WITH_SUB_TYPES));
        assertTrue(s.contains(TEST_TYPE1));
        assertTrue(s.contains(TEST_TYPE2));
        assertTrue(s.contains(TEST_TYPE3));
        assertTrue(s.endsWith(")"));
    }

    @Test
    public void getSubTypeForTypeWithSubTypes_ReturnsEmptyString() throws Exception {
        String s = invokeGetSubTypesForType(TEST_TYPE_WITH_SUB_TYPES, 20);

        assertTrue(StringUtils.isBlank(s));
    }

    private String invokeGetSubTypesForType(String inputString, int maxSubTypes) throws Exception {
        String s = EntityDiscoveryService.getTypeFilter(typeRegistry, inputString, maxSubTypes);

        assertNotNull(s);
        return s;
    }

}
