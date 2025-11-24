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
package org.apache.atlas.model.instance;

import org.apache.atlas.model.instance.EntityMutations.EntityMutation;
import org.apache.atlas.model.instance.EntityMutations.EntityOperation;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestEntityMutations {
    @Test
    public void testConstructor() {
        List<EntityMutation> mutations = new ArrayList<>();
        EntityMutation mutation1 = new EntityMutation(EntityOperation.CREATE, new AtlasEntity("type1"));
        EntityMutation mutation2 = new EntityMutation(EntityOperation.UPDATE, new AtlasEntity("type2"));
        mutations.add(mutation1);
        mutations.add(mutation2);

        EntityMutations entityMutations = new EntityMutations(mutations);
        assertNotNull(entityMutations);
    }

    @Test
    public void testToString() {
        // Test toString with empty list
        List<EntityMutation> emptyMutations = new ArrayList<>();
        EntityMutations emptyEntityMutations = new EntityMutations(emptyMutations);

        String toStringEmpty = emptyEntityMutations.toString();
        assertNotNull(toStringEmpty);
        assertTrue(toStringEmpty.contains("EntityMutations"));
        assertTrue(toStringEmpty.contains("{}"));

        // Test toString with populated list
        List<EntityMutation> mutations = new ArrayList<>();
        AtlasEntity entity1 = new AtlasEntity("type1");
        entity1.setGuid("guid1");
        AtlasEntity entity2 = new AtlasEntity("type2");
        entity2.setGuid("guid2");

        EntityMutation mutation1 = new EntityMutation(EntityOperation.CREATE, entity1);
        EntityMutation mutation2 = new EntityMutation(EntityOperation.DELETE, entity2);
        mutations.add(mutation1);
        mutations.add(mutation2);

        EntityMutations entityMutations = new EntityMutations(mutations);
        String toStringWithData = entityMutations.toString();
        assertNotNull(toStringWithData);
        assertTrue(toStringWithData.contains("EntityMutations"));
        assertTrue(toStringWithData.contains("CREATE"));
        assertTrue(toStringWithData.contains("DELETE"));

        // Test toString with StringBuilder parameter
        StringBuilder sb = entityMutations.toString(null);
        assertNotNull(sb);
        assertTrue(sb.toString().contains("EntityMutations"));

        StringBuilder existingSb = new StringBuilder("Prefix: ");
        entityMutations.toString(existingSb);
        assertTrue(existingSb.toString().startsWith("Prefix: "));
    }

    @Test
    public void testEquals() {
        AtlasEntity entity1 = new AtlasEntity("type1");
        entity1.setGuid("guid1");
        AtlasEntity entity2 = new AtlasEntity("type2");
        entity2.setGuid("guid2");

        List<EntityMutation> mutations1 = new ArrayList<>();
        mutations1.add(new EntityMutation(EntityOperation.CREATE, entity1));
        mutations1.add(new EntityMutation(EntityOperation.UPDATE, entity2));

        List<EntityMutation> mutations2 = new ArrayList<>();
        mutations2.add(new EntityMutation(EntityOperation.CREATE, entity1));
        mutations2.add(new EntityMutation(EntityOperation.UPDATE, entity2));

        EntityMutations entityMutations1 = new EntityMutations(mutations1);
        EntityMutations entityMutations2 = new EntityMutations(mutations2);

        // Test equals
        assertEquals(entityMutations1, entityMutations2);
        assertEquals(entityMutations1.hashCode(), entityMutations2.hashCode());

        // Test self equality
        assertEquals(entityMutations1, entityMutations1);

        // Test null equality
        assertNotEquals(entityMutations1, null);

        // Test different class equality
        assertNotEquals(entityMutations1, "string");

        // Test with different mutations
        List<EntityMutation> differentMutations = new ArrayList<>();
        differentMutations.add(new EntityMutation(EntityOperation.DELETE, entity1));
        EntityMutations differentEntityMutations = new EntityMutations(differentMutations);
        assertNotEquals(entityMutations1, differentEntityMutations);
    }

    @Test
    public void testHashCode() {
        AtlasEntity entity = new AtlasEntity("type1");
        List<EntityMutation> mutations1 = new ArrayList<>();
        mutations1.add(new EntityMutation(EntityOperation.CREATE, entity));

        List<EntityMutation> mutations2 = new ArrayList<>();
        mutations2.add(new EntityMutation(EntityOperation.CREATE, entity));

        EntityMutations entityMutations1 = new EntityMutations(mutations1);
        EntityMutations entityMutations2 = new EntityMutations(mutations2);

        assertEquals(entityMutations1.hashCode(), entityMutations2.hashCode());

        // Test with different mutations
        List<EntityMutation> differentMutations = new ArrayList<>();
        differentMutations.add(new EntityMutation(EntityOperation.UPDATE, entity));
        EntityMutations differentEntityMutations = new EntityMutations(differentMutations);
        assertNotEquals(entityMutations1.hashCode(), differentEntityMutations.hashCode());
    }

    @Test
    public void testEntityOperationEnum() {
        assertEquals(5, EntityOperation.values().length);
        assertTrue(Arrays.asList(EntityOperation.values()).contains(EntityOperation.CREATE));
        assertTrue(Arrays.asList(EntityOperation.values()).contains(EntityOperation.UPDATE));
        assertTrue(Arrays.asList(EntityOperation.values()).contains(EntityOperation.PARTIAL_UPDATE));
        assertTrue(Arrays.asList(EntityOperation.values()).contains(EntityOperation.DELETE));
        assertTrue(Arrays.asList(EntityOperation.values()).contains(EntityOperation.PURGE));

        // Test enum names
        assertEquals("CREATE", EntityOperation.CREATE.name());
        assertEquals("UPDATE", EntityOperation.UPDATE.name());
        assertEquals("PARTIAL_UPDATE", EntityOperation.PARTIAL_UPDATE.name());
        assertEquals("DELETE", EntityOperation.DELETE.name());
        assertEquals("PURGE", EntityOperation.PURGE.name());
    }

    @Test
    public void testEntityMutationNestedClass() {
        AtlasEntity entity = new AtlasEntity("testType");
        entity.setGuid("testGuid");

        // Test constructor
        EntityMutation mutation = new EntityMutation(EntityOperation.CREATE, entity);
        assertNotNull(mutation);

        // Test toString
        String toString = mutation.toString();
        assertNotNull(toString);
        assertTrue(toString.contains("EntityMutation"));
        assertTrue(toString.contains("CREATE"));
        assertTrue(toString.contains("testType"));
        assertTrue(toString.contains("testGuid"));

        // Test toString with StringBuilder parameter
        StringBuilder sb = mutation.toString(null);
        assertNotNull(sb);
        assertTrue(sb.toString().contains("EntityMutation"));

        StringBuilder existingSb = new StringBuilder("Prefix: ");
        mutation.toString(existingSb);
        assertTrue(existingSb.toString().startsWith("Prefix: "));

        // Test with null entity
        EntityMutation mutationWithNullEntity = new EntityMutation(EntityOperation.DELETE, null);
        String toStringNullEntity = mutationWithNullEntity.toString();
        assertNotNull(toStringNullEntity);
        assertTrue(toStringNullEntity.contains("EntityMutation"));
        assertTrue(toStringNullEntity.contains("DELETE"));
    }

    @Test
    public void testEntityMutationEquals() {
        AtlasEntity entity1 = new AtlasEntity("type1");
        entity1.setGuid("guid1");
        AtlasEntity entity2 = new AtlasEntity("type2");
        entity2.setGuid("guid2");

        EntityMutation mutation1 = new EntityMutation(EntityOperation.CREATE, entity1);
        EntityMutation mutation2 = new EntityMutation(EntityOperation.CREATE, entity1);

        // Test equals
        assertEquals(mutation1, mutation2);
        assertEquals(mutation1.hashCode(), mutation2.hashCode());

        // Test self equality
        assertEquals(mutation1, mutation1);

        // Test null equality
        assertNotEquals(mutation1, null);

        // Test different class equality
        assertNotEquals(mutation1, "string");

        // Test different operation
        EntityMutation mutationDifferentOp = new EntityMutation(EntityOperation.UPDATE, entity1);
        assertNotEquals(mutation1, mutationDifferentOp);

        // Test different entity
        EntityMutation mutationDifferentEntity = new EntityMutation(EntityOperation.CREATE, entity2);
        assertNotEquals(mutation1, mutationDifferentEntity);

        // Test with null entities
        EntityMutation mutationWithNull1 = new EntityMutation(EntityOperation.CREATE, null);
        EntityMutation mutationWithNull2 = new EntityMutation(EntityOperation.CREATE, null);
        assertEquals(mutationWithNull1, mutationWithNull2);

        EntityMutation mutationWithEntity = new EntityMutation(EntityOperation.CREATE, entity1);
        assertNotEquals(mutationWithNull1, mutationWithEntity);
    }

    @Test
    public void testEntityMutationHashCode() {
        AtlasEntity entity = new AtlasEntity("type1");
        entity.setGuid("guid1");

        EntityMutation mutation1 = new EntityMutation(EntityOperation.CREATE, entity);
        EntityMutation mutation2 = new EntityMutation(EntityOperation.CREATE, entity);

        assertEquals(mutation1.hashCode(), mutation2.hashCode());

        // Test with different operation
        EntityMutation mutationDifferentOp = new EntityMutation(EntityOperation.UPDATE, entity);
        assertNotEquals(mutation1.hashCode(), mutationDifferentOp.hashCode());

        // Test with null entity
        EntityMutation mutationWithNull = new EntityMutation(EntityOperation.CREATE, null);
        assertNotEquals(mutation1.hashCode(), mutationWithNull.hashCode());
    }

    @Test
    public void testComplexScenario() {
        // Create a complex scenario with multiple operations and entities
        AtlasEntity tableEntity = new AtlasEntity("Table");
        tableEntity.setGuid("table-guid-1");

        AtlasEntity columnEntity = new AtlasEntity("Column");
        columnEntity.setGuid("column-guid-1");

        AtlasEntity databaseEntity = new AtlasEntity("Database");
        databaseEntity.setGuid("database-guid-1");

        AtlasEntity oldTableEntity = new AtlasEntity("OldTable");
        oldTableEntity.setGuid("old-table-guid-1");

        List<EntityMutation> mutations = new ArrayList<>();
        mutations.add(new EntityMutation(EntityOperation.CREATE, tableEntity));
        mutations.add(new EntityMutation(EntityOperation.CREATE, columnEntity));
        mutations.add(new EntityMutation(EntityOperation.UPDATE, databaseEntity));
        mutations.add(new EntityMutation(EntityOperation.DELETE, oldTableEntity));

        EntityMutations entityMutations = new EntityMutations(mutations);

        // Test toString contains all operations
        String toString = entityMutations.toString();
        assertTrue(toString.contains("CREATE"));
        assertTrue(toString.contains("UPDATE"));
        assertTrue(toString.contains("DELETE"));
        assertTrue(toString.contains("Table"));
        assertTrue(toString.contains("Column"));
        assertTrue(toString.contains("Database"));

        // Test equals with identical scenario - use the same entity references
        List<EntityMutation> identicalMutations = new ArrayList<>();
        identicalMutations.add(new EntityMutation(EntityOperation.CREATE, tableEntity));
        identicalMutations.add(new EntityMutation(EntityOperation.CREATE, columnEntity));
        identicalMutations.add(new EntityMutation(EntityOperation.UPDATE, databaseEntity));
        identicalMutations.add(new EntityMutation(EntityOperation.DELETE, oldTableEntity));

        EntityMutations identicalEntityMutations = new EntityMutations(identicalMutations);
        assertEquals(entityMutations, identicalEntityMutations);
    }

    @Test
    public void testNullHandling() {
        // Test with null mutations list
        EntityMutations entityMutationsWithNull = new EntityMutations(null);
        assertNotNull(entityMutationsWithNull);

        String toStringNull = entityMutationsWithNull.toString();
        assertNotNull(toStringNull);
        assertTrue(toStringNull.contains("EntityMutations"));

        // Test equality with null
        EntityMutations anotherNullMutations = new EntityMutations(null);
        assertEquals(entityMutationsWithNull, anotherNullMutations);

        // Test with non-null vs null
        List<EntityMutation> nonNullMutations = new ArrayList<>();
        nonNullMutations.add(new EntityMutation(EntityOperation.CREATE, new AtlasEntity("type1")));
        EntityMutations nonNullEntityMutations = new EntityMutations(nonNullMutations);
        assertNotEquals(entityMutationsWithNull, nonNullEntityMutations);
    }
}
