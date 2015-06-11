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

package org.apache.atlas.repository.memory;

import com.google.common.collect.ImmutableList;
import org.apache.atlas.AtlasException;
import org.apache.atlas.repository.BaseTest;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.json.InstanceSerialization$;
import org.apache.atlas.typesystem.json.Serialization$;
import org.apache.atlas.typesystem.json.TypesSerialization$;
import org.apache.atlas.typesystem.types.AttributeDefinition;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.StructTypeDefinition;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class InstanceE2ETest extends BaseTest {

    protected List<HierarchicalTypeDefinition> createHiveTypes(TypeSystem typeSystem)
    throws AtlasException {
        ArrayList<HierarchicalTypeDefinition> typeDefinitions = new ArrayList<>();

        HierarchicalTypeDefinition<ClassType> databaseTypeDefinition =
                TypesUtil.createClassTypeDef("hive_database",
                        ImmutableList.<String>of(),
                        TypesUtil.createRequiredAttrDef("name", DataTypes.STRING_TYPE),
                        TypesUtil.createRequiredAttrDef("description", DataTypes.STRING_TYPE));
        typeDefinitions.add(databaseTypeDefinition);

        HierarchicalTypeDefinition<ClassType> tableTypeDefinition = TypesUtil.createClassTypeDef(
                "hive_table",
                ImmutableList.<String>of(),
                TypesUtil.createRequiredAttrDef("name", DataTypes.STRING_TYPE),
                TypesUtil.createRequiredAttrDef("description", DataTypes.STRING_TYPE),
                TypesUtil.createRequiredAttrDef("type", DataTypes.STRING_TYPE),
                new AttributeDefinition("hive_database",
                        "hive_database", Multiplicity.REQUIRED, false, "hive_database"));
        typeDefinitions.add(tableTypeDefinition);

        HierarchicalTypeDefinition<TraitType> fetlTypeDefinition = TypesUtil.createTraitTypeDef(
                "hive_fetl",
                ImmutableList.<String>of(),
                TypesUtil.createRequiredAttrDef("level", DataTypes.INT_TYPE));
        typeDefinitions.add(fetlTypeDefinition);

        typeSystem.defineTypes(
                ImmutableList.<StructTypeDefinition>of(),
                ImmutableList.of(fetlTypeDefinition),
                ImmutableList.of(databaseTypeDefinition, tableTypeDefinition));

        return typeDefinitions;
    }

    protected Referenceable createHiveTableReferenceable()
            throws AtlasException {
        Referenceable databaseInstance = new Referenceable("hive_database");
        databaseInstance.set("name", "hive_database");
        databaseInstance.set("description", "foo database");

        Referenceable tableInstance = new Referenceable("hive_table", "hive_fetl");
        tableInstance.set("name", "t1");
        tableInstance.set("description", "bar table");
        tableInstance.set("type", "managed");
        tableInstance.set("hive_database", databaseInstance);

        Struct traitInstance = (Struct) tableInstance.getTrait("hive_fetl");
        traitInstance.set("level", 1);

        tableInstance.set("hive_fetl", traitInstance);

        return tableInstance;
    }

    protected ITypedReferenceableInstance createHiveTableInstance(TypeSystem typeSystem)
    throws AtlasException {
        ClassType tableType = typeSystem.getDataType(ClassType.class, "hive_table");
        return tableType.convert(createHiveTableReferenceable(), Multiplicity.REQUIRED);
    }

    @Test
    public void testType() throws AtlasException {

        TypeSystem ts = getTypeSystem();

        createHiveTypes(ts);

        String jsonStr = TypesSerialization$.MODULE$
                .toJson(ts, ImmutableList.of("hive_database", "hive_table"));
        System.out.println(jsonStr);

        TypesDef typesDef1 = TypesSerialization$.MODULE$.fromJson(jsonStr);
        System.out.println(typesDef1);

        ts.reset();
        ts.defineTypes(typesDef1);
        jsonStr = TypesSerialization$.MODULE$
                .toJson(ts, ImmutableList.of("hive_database", "hive_table"));
        System.out.println(jsonStr);

    }

    @Test
    public void testInstance() throws AtlasException {

        TypeSystem ts = getTypeSystem();

        createHiveTypes(ts);

        ITypedReferenceableInstance i = createHiveTableInstance(getTypeSystem());

        String jsonStr = Serialization$.MODULE$.toJson(i);
        System.out.println(jsonStr);

        i = Serialization$.MODULE$.fromJson(jsonStr);
        System.out.println(i);
    }

    @Test
    public void testInstanceSerialization() throws AtlasException {

        TypeSystem ts = getTypeSystem();

        createHiveTypes(ts);

        Referenceable r = createHiveTableReferenceable();
        String jsonStr = InstanceSerialization$.MODULE$.toJson(r, true);
        Referenceable  r1 = InstanceSerialization$.MODULE$.fromJsonReferenceable(jsonStr, true);
        ClassType tableType = ts.getDataType(ClassType.class, "hive_table");

        ITypedReferenceableInstance i = tableType.convert(r1, Multiplicity.REQUIRED);

        jsonStr = Serialization$.MODULE$.toJson(i);
        System.out.println(jsonStr);

        i = Serialization$.MODULE$.fromJson(jsonStr);
        System.out.println(i);

    }
}
