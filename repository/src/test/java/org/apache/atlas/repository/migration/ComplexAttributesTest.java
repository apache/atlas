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

package org.apache.atlas.repository.migration;

import com.google.inject.Inject;
import org.apache.atlas.TestModules;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.GraphDBMigrator;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.io.IOException;

@Guice(modules = TestModules.TestOnlyModule.class)
public class ComplexAttributesTest extends MigrationBaseAsserts {
    @Inject
    public ComplexAttributesTest(AtlasGraph graph, GraphDBMigrator migrator) {
        super(graph, migrator);
    }

    @Test
    public void verify() throws IOException, AtlasBaseException {
        String structType                = "struct_type";
        String entityType                = "entity_type";
        String entityWithComplexCollType = "entity_with_complex_collection_attr";

        final int expectedTotalCount                         = 217;
        final int expectedEntityTypeCount                   = 16;
        final int expectedStructTypeCount                   = 3;
        final int expectedEntityWithComplexCollTypeCount = 1;

        runFileImporter("complex-attr_db");

        assertTypeCountNameGuid(structType, expectedStructTypeCount, "", "");
        assertTypeCountNameGuid(entityType, expectedEntityTypeCount, "", "");
        assertTypeCountNameGuid(entityWithComplexCollType, expectedEntityWithComplexCollTypeCount, "", "");

        assertEdgesWithLabel(getVertex(entityWithComplexCollType, "").getEdges(AtlasEdgeDirection.OUT).iterator(), 1, "__entity_with_complex_collection_attr.listOfEntities");
        assertEdgesWithLabel(getVertex(entityWithComplexCollType, "").getEdges(AtlasEdgeDirection.OUT).iterator(), 9, "__entity_with_complex_collection_attr.mapOfStructs");

        assertMigrationStatus(expectedTotalCount);
    }
}
