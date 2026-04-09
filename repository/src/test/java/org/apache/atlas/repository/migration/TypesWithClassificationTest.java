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
public class TypesWithClassificationTest extends MigrationBaseAsserts {
    @Inject
    public TypesWithClassificationTest(AtlasGraph graph, GraphDBMigrator migrator) {
        super(graph, migrator);
    }

    @Test
    public void verify() throws IOException, AtlasBaseException {
        int    expectedTotalCount  = 62;
        String entityType          = "ComplexTraitType";
        String legacyTypeTrait     = "legacy_traitprayivofx4";
        String legacyTypeVendorPii = "legacy_VENDOR_PII";
        String legacyTypeFinance   = "legacy_FINANCE";

        runFileImporter("classification_defs");

        assertTypeCountNameGuid(entityType, 1, "", "");
        assertTypeCountNameGuid(legacyTypeTrait, 1, "", "");
        assertTypeCountNameGuid(legacyTypeVendorPii, 3, "", "");
        assertTypeCountNameGuid(legacyTypeFinance, 2, "", "");

        assertEdgesWithLabel(getVertex(entityType, "").getEdges(AtlasEdgeDirection.OUT).iterator(), 1, "__ComplexTraitType.vendors");
        assertEdgesWithLabel(getVertex(entityType, "").getEdges(AtlasEdgeDirection.OUT).iterator(), 4, "__ComplexTraitType.finance");
        assertEdgesWithLabel(getVertex(entityType, "").getEdges(AtlasEdgeDirection.OUT).iterator(), 6, "__ComplexTraitType.complexTrait");

        assertMigrationStatus(expectedTotalCount);
    }
}
