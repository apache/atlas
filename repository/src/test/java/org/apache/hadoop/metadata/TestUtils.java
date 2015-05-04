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

package org.apache.hadoop.metadata;

import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONWriter;
import org.apache.hadoop.metadata.repository.graph.GraphHelper;
import org.apache.hadoop.metadata.typesystem.ITypedReferenceableInstance;
import org.apache.hadoop.metadata.typesystem.Referenceable;
import org.apache.hadoop.metadata.typesystem.types.AttributeDefinition;
import org.apache.hadoop.metadata.typesystem.types.ClassType;
import org.apache.hadoop.metadata.typesystem.types.DataTypes;
import org.apache.hadoop.metadata.typesystem.types.EnumType;
import org.apache.hadoop.metadata.typesystem.types.EnumTypeDefinition;
import org.apache.hadoop.metadata.typesystem.types.EnumValue;
import org.apache.hadoop.metadata.typesystem.types.HierarchicalTypeDefinition;
import org.apache.hadoop.metadata.typesystem.types.Multiplicity;
import org.apache.hadoop.metadata.typesystem.types.StructTypeDefinition;
import org.apache.hadoop.metadata.typesystem.types.TraitType;
import org.apache.hadoop.metadata.typesystem.types.TypeSystem;
import org.testng.Assert;

import java.io.File;

import static org.apache.hadoop.metadata.typesystem.types.utils.TypesUtil.createClassTypeDef;
import static org.apache.hadoop.metadata.typesystem.types.utils.TypesUtil.createOptionalAttrDef;
import static org.apache.hadoop.metadata.typesystem.types.utils.TypesUtil.createRequiredAttrDef;
import static org.apache.hadoop.metadata.typesystem.types.utils.TypesUtil.createStructTypeDef;
import static org.apache.hadoop.metadata.typesystem.types.utils.TypesUtil.createTraitTypeDef;

/**
 * Test utility class.
 */
public final class TestUtils {

    private TestUtils() {
    }

    /**
     * Dumps the graph in GSON format in the path returned.
     *
     * @param titanGraph handle to graph
     * @return path to the dump file
     * @throws Exception
     */
    public static String dumpGraph(TitanGraph titanGraph) throws Exception {
        File tempFile = File.createTempFile("graph", ".gson");
        System.out.println("tempFile.getPath() = " + tempFile.getPath());
        GraphSONWriter.outputGraph(titanGraph, tempFile.getPath());

        System.out.println("Vertices:");
        for (Vertex vertex : titanGraph.getVertices()) {
            System.out.println(GraphHelper.vertexString(vertex));
        }

        System.out.println("Edges:");
        for (Edge edge : titanGraph.getEdges()) {
            System.out.println(GraphHelper.edgeString(edge));
        }

        return tempFile.getPath();
    }

    /**
     * Class Hierarchy is:
     * Department(name : String, employees : Array[Person])
     * Person(name : String, department : Department, manager : Manager)
     * Manager(subordinates : Array[Person]) extends Person
     * <p/>
     * Persons can have SecurityClearance(level : Int) clearance.
     */
    public static void defineDeptEmployeeTypes(TypeSystem ts) throws MetadataException {

        EnumTypeDefinition orgLevelEnum =
                new EnumTypeDefinition("OrgLevel", new EnumValue("L1", 1), new EnumValue("L2", 2));
        ts.defineEnumType(orgLevelEnum);

        StructTypeDefinition addressDetails = createStructTypeDef("Address",
                createRequiredAttrDef("street", DataTypes.STRING_TYPE),
                createRequiredAttrDef("city", DataTypes.STRING_TYPE));

        HierarchicalTypeDefinition<ClassType> deptTypeDef =
                createClassTypeDef("Department", ImmutableList.<String>of(),
                        createRequiredAttrDef("name", DataTypes.STRING_TYPE),
                        new AttributeDefinition("employees",
                                String.format("array<%s>", "Person"), Multiplicity.COLLECTION, true,
                                "department")
                );

        HierarchicalTypeDefinition<ClassType> personTypeDef = createClassTypeDef("Person",
                ImmutableList.<String>of(),
                createRequiredAttrDef("name", DataTypes.STRING_TYPE),
                createOptionalAttrDef("orgLevel", ts.getDataType(EnumType.class, "OrgLevel")),
                createOptionalAttrDef("address", "Address"),
                new AttributeDefinition("department",
                        "Department", Multiplicity.REQUIRED, false, "employees"),
                new AttributeDefinition("manager",
                        "Manager", Multiplicity.OPTIONAL, false, "subordinates")
        );

        HierarchicalTypeDefinition<ClassType> managerTypeDef = createClassTypeDef("Manager",
                ImmutableList.of("Person"),
                new AttributeDefinition("subordinates",
                        String.format("array<%s>", "Person"), Multiplicity.COLLECTION, false,
                        "manager")
        );

        HierarchicalTypeDefinition<TraitType> securityClearanceTypeDef = createTraitTypeDef(
                "SecurityClearance",
                ImmutableList.<String>of(),
                createRequiredAttrDef("level", DataTypes.INT_TYPE)
        );

        ts.defineTypes(ImmutableList.of(addressDetails),
                ImmutableList.of(securityClearanceTypeDef),
                ImmutableList.of(deptTypeDef, personTypeDef, managerTypeDef));
    }

    public static Referenceable createDeptEg1(TypeSystem ts) throws MetadataException {
        Referenceable hrDept = new Referenceable("Department");
        Referenceable john = new Referenceable("Person");
        Referenceable jane = new Referenceable("Manager", "SecurityClearance");
        Referenceable johnAddr = new Referenceable("Address");
        Referenceable janeAddr = new Referenceable("Address");

        hrDept.set("name", "hr");
        john.set("name", "John");
        john.set("department", hrDept);
        johnAddr.set("street", "Stewart Drive");
        johnAddr.set("city", "Sunnyvale");
        john.set("address", johnAddr);

        jane.set("name", "Jane");
        jane.set("department", hrDept);
        janeAddr.set("street", "Great America Parkway");
        janeAddr.set("city", "Santa Clara");
        jane.set("address", janeAddr);

        john.set("manager", jane);

        hrDept.set("employees", ImmutableList.of(john, jane));

        jane.set("subordinates", ImmutableList.of(john));

        jane.getTrait("SecurityClearance").set("level", 1);

        ClassType deptType = ts.getDataType(ClassType.class, "Department");
        ITypedReferenceableInstance hrDept2 = deptType.convert(hrDept, Multiplicity.REQUIRED);
        Assert.assertNotNull(hrDept2);

        return hrDept;
    }
}
