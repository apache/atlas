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

package org.apache.hadoop.metadata.typesystem.types.store;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.metadata.BaseTest;
import org.apache.hadoop.metadata.typesystem.TypesDef;
import org.apache.hadoop.metadata.typesystem.types.ClassType;
import org.apache.hadoop.metadata.typesystem.types.DataTypes;
import org.apache.hadoop.metadata.typesystem.types.HierarchicalTypeDefinition;
import org.apache.hadoop.metadata.typesystem.types.StructTypeDefinition;
import org.apache.hadoop.metadata.typesystem.types.TraitType;
import org.apache.hadoop.metadata.typesystem.types.TypeSystem;
import org.apache.hadoop.metadata.typesystem.types.utils.TypesUtil;
import org.junit.Before;
import org.junit.Test;

import static org.apache.hadoop.metadata.typesystem.types.utils.TypesUtil.createClassTypeDef;
import static org.apache.hadoop.metadata.typesystem.types.utils.TypesUtil.createRequiredAttrDef;

public class HdfsStoreTest extends BaseTest {
    private static final String LOCATION = "target/type-store";

    @Before
    public void setup() throws Exception {
        super.setup();
        System.setProperty(HdfsStore.LOCATION_PROPERTY, LOCATION);
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path(LOCATION), true);

        //define type system
        HierarchicalTypeDefinition<TraitType> tagTypeDefinition =
                TypesUtil.createTraitTypeDef("tag",
                        ImmutableList.<String>of(),
                        createRequiredAttrDef("level", DataTypes.INT_TYPE));
        HierarchicalTypeDefinition<ClassType> databaseTypeDefinition =
                createClassTypeDef("database",
                        ImmutableList.<String>of(),
                        createRequiredAttrDef("name", DataTypes.STRING_TYPE),
                        createRequiredAttrDef("description", DataTypes.STRING_TYPE),
                        createRequiredAttrDef("tag", DataTypes.STRING_TYPE));
        TypeSystem.getInstance().defineTypes(
                ImmutableList.<StructTypeDefinition>of(),
                ImmutableList.of(tagTypeDefinition),
                ImmutableList.of(databaseTypeDefinition));

    }

    @Test
    public void testStore() throws Exception {
        TypeSystemStore store = new HdfsStore();
        TypeSystem typeSystem = TypeSystem.getInstance();
        store.store(typeSystem, "hive");

        FileSystem fs = FileSystem.get(new Configuration());
        Assert.assertTrue(fs.exists(new Path(LOCATION, "hive.json")));

        TypesDef typeDef = store.restore("hive");
        Assert.assertNotNull(typeDef);
        Assert.assertEquals(1, typeDef.classTypesAsJavaList().size());
        Assert.assertEquals("database", typeDef.classTypesAsJavaList().get(0).typeName);
    }

    @Test
    public void testRestore() throws Exception {
        TypeSystemStore store = new HdfsStore();
        TypeSystem typeSystem = TypeSystem.getInstance();
        store.store(typeSystem, "hive");
        store.store(typeSystem, "falcon");

        ImmutableMap<String, TypesDef> typeDef = store.restore();
        Assert.assertEquals(2, typeDef.size());
        Assert.assertEquals(1, typeDef.get("falcon").classTypesAsJavaList().size());
        Assert.assertEquals("database",
                typeDef.get("falcon").classTypesAsJavaList().get(0).typeName);
    }

    @Test
    public void testArchive() throws Exception {
        TypeSystemStore store = new HdfsStore();
        TypeSystem typeSystem = TypeSystem.getInstance();
        store.store(typeSystem, "hive");    //insert
        store.store(typeSystem, "hive");    //update

        FileSystem fs = FileSystem.get(new Configuration());
        Assert.assertTrue(fs.exists(new Path(LOCATION, "hive.json")));
        FileStatus[] files = fs.listStatus(new Path(LOCATION, "ARCHIVE"));
        Assert.assertEquals(1, files.length);
        Assert.assertTrue(files[0].getPath().getName().startsWith("hive.json"));
    }

    @Test
    public void testDelete() throws Exception {
        TypeSystemStore store = new HdfsStore();
        TypeSystem typeSystem = TypeSystem.getInstance();
        store.store(typeSystem, "hive");

        store.delete("hive");
        FileSystem fs = FileSystem.get(new Configuration());
        Assert.assertFalse(fs.exists(new Path(LOCATION, "hive.json")));
        FileStatus[] files = fs.listStatus(new Path(LOCATION, "ARCHIVE"));
        Assert.assertEquals(1, files.length);
        Assert.assertTrue(files[0].getPath().getName().startsWith("hive.json"));
    }
}
