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

package org.apache.atlas.repository.graph;

import com.google.inject.Inject;
import org.apache.atlas.AtlasException;
import org.apache.atlas.TestModules;
import org.apache.atlas.TestUtils;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphIndex;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.repository.graphdb.AtlasPropertyKey;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.EnumType;
import org.apache.atlas.typesystem.types.EnumValue;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Set;

import static org.apache.atlas.typesystem.types.utils.TypesUtil.createClassTypeDef;
import static org.testng.Assert.*;

@Guice(modules = TestModules.TestOnlyModule.class)
public class GraphBackedSearchIndexerTest {
    @Inject
    private GraphBackedSearchIndexer graphBackedSearchIndexer;

    @Test
    public void verifySystemMixedIndexes() {
        AtlasGraph graph = TestUtils.getGraph();
        AtlasGraphManagement managementSystem = graph.getManagementSystem();
        try {
            AtlasGraphIndex vertexIndex = managementSystem.getGraphIndex(Constants.VERTEX_INDEX);
            assertNotNull(vertexIndex);
            assertTrue(vertexIndex.isMixedIndex());
            assertFalse(vertexIndex.isEdgeIndex());
            assertTrue(vertexIndex.isVertexIndex());
            
            AtlasGraphIndex edgeIndex = managementSystem.getGraphIndex(Constants.EDGE_INDEX);
            assertNotNull(edgeIndex);
            assertTrue(edgeIndex.isMixedIndex());
            assertTrue(edgeIndex.isEdgeIndex());
            assertFalse(edgeIndex.isVertexIndex());
           
    
            verifyVertexIndexContains(managementSystem, Constants.STATE_PROPERTY_KEY);
        }
        finally {
            managementSystem.rollback();
        }
    }

    @Test
    public void verifySystemCompositeIndexes() {
        AtlasGraph graph = TestUtils.getGraph();
        AtlasGraphManagement managementSystem = graph.getManagementSystem();
        try {
            verifySystemCompositeIndex(managementSystem, Constants.GUID_PROPERTY_KEY, true);
            verifyVertexIndexContains(managementSystem, Constants.GUID_PROPERTY_KEY);
    
            verifySystemCompositeIndex(managementSystem, Constants.ENTITY_TYPE_PROPERTY_KEY, false);
            verifyVertexIndexContains(managementSystem, Constants.ENTITY_TYPE_PROPERTY_KEY);
    
            verifySystemCompositeIndex(managementSystem, Constants.SUPER_TYPES_PROPERTY_KEY, false);
            verifyVertexIndexContains(managementSystem, Constants.SUPER_TYPES_PROPERTY_KEY);
    
            verifySystemCompositeIndex(managementSystem, Constants.TRAIT_NAMES_PROPERTY_KEY, false);
            verifyVertexIndexContains(managementSystem, Constants.TRAIT_NAMES_PROPERTY_KEY);
        }
        finally {
            managementSystem.rollback();
        }
    }

    @Test
    public void verifyFullTextIndex() {
        AtlasGraph graph = TestUtils.getGraph();
        AtlasGraphManagement managementSystem = graph.getManagementSystem();
        try {
        AtlasGraphIndex fullTextIndex = managementSystem.getGraphIndex(Constants.FULLTEXT_INDEX);
        assertTrue(fullTextIndex.isMixedIndex());

        Arrays.asList(fullTextIndex.getFieldKeys()).contains(
                managementSystem.getPropertyKey(Constants.ENTITY_TEXT_PROPERTY_KEY));
        }
        finally {
            managementSystem.rollback();
        }
    }

    @Test
    public void verifyTypeStoreIndexes() {
        AtlasGraph graph = TestUtils.getGraph();
        AtlasGraphManagement managementSystem = graph.getManagementSystem();
        try {
            verifySystemCompositeIndex(managementSystem, Constants.TYPENAME_PROPERTY_KEY, true);
            verifyVertexIndexContains(managementSystem, Constants.TYPENAME_PROPERTY_KEY);
    
            verifySystemCompositeIndex(managementSystem, Constants.VERTEX_TYPE_PROPERTY_KEY, false);
            verifyVertexIndexContains(managementSystem, Constants.VERTEX_TYPE_PROPERTY_KEY);
        }
        finally {
            managementSystem.rollback();
        }
        
    }

    @Test
    public void verifyUserDefinedTypeIndex() throws AtlasException {
        AtlasGraph graph = TestUtils.getGraph();
        AtlasGraphManagement managementSystem = graph.getManagementSystem();
        try {
            TypeSystem typeSystem = TypeSystem.getInstance();
    
            String enumName = "randomEnum" + TestUtils.randomString(10);
            EnumType managedType = typeSystem.defineEnumType(enumName, new EnumValue("randomEnumValue", 0));
    
            HierarchicalTypeDefinition<ClassType> databaseTypeDefinition =
                    createClassTypeDef("Database", "Database type description", null,
                            TypesUtil.createUniqueRequiredAttrDef("name", DataTypes.STRING_TYPE),
                            TypesUtil.createRequiredAttrDef("managedType", managedType));
                
            ClassType databaseType = typeSystem.defineClassType(databaseTypeDefinition);
            graphBackedSearchIndexer.onAdd(Arrays.asList(databaseType));
    
            verifySystemCompositeIndex(managementSystem, "Database.name" + Constants.ENTITY_TYPE_PROPERTY_KEY, false);
            verifyVertexIndexContains(managementSystem, "Database.name" + Constants.ENTITY_TYPE_PROPERTY_KEY);
            verifySystemCompositeIndex(managementSystem, "Database.name" + Constants.SUPER_TYPES_PROPERTY_KEY, false);
    
            verifyVertexIndexContains(managementSystem, "Database.managedType");
        }
        finally {
            //search indexer uses its own titan management transaction
            managementSystem.rollback();
        }
    }

    private void verifyVertexIndexContains(AtlasGraphManagement managementSystem, String indexName) {
        AtlasGraphIndex vertexIndex = managementSystem.getGraphIndex(Constants.VERTEX_INDEX);
        Set<AtlasPropertyKey> fieldKeys = vertexIndex.getFieldKeys();
        Arrays.asList(fieldKeys).contains(managementSystem.getPropertyKey(indexName));
    }

    private void verifySystemCompositeIndex(AtlasGraphManagement managementSystem, String indexName, boolean isUnique) {
        AtlasGraphIndex systemIndex = managementSystem.getGraphIndex(indexName);
        assertNotNull(systemIndex);
        assertTrue(systemIndex.isCompositeIndex());
        if (isUnique) {
            assertTrue(systemIndex.isUnique());
        } else {
            assertFalse(systemIndex.isUnique());
        }
    }
}
