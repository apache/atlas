/*
 * Copyright 2023 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.atlas.connector.entities;

import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.model.instance.AtlasEntity;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

/**
 * Tests atlas entity loading and caching
 */
public class CouchbaseAtlasEntityTest {
    static final String QUALIFIED_NAME = "testEntityQualifiedName";
    static final String TYPE_NAME      = "testEntityTypeName";
    static final UUID   ID             = UUID.randomUUID();

    @Test
    public void testEntityLoading() throws Exception {
        final AtlasClientV2 ac = Mockito.mock(AtlasClientV2.class);
        final AtlasEntity   ae = Mockito.mock(AtlasEntity.class);

        Mockito.when(ae.getAttribute(Mockito.eq("qualifiedName"))).thenReturn(QUALIFIED_NAME);

        Mockito.when(ac.getEntityByAttribute(Mockito.eq(TYPE_NAME), Mockito.anyMap())).thenAnswer(iom -> {
            Map<String, String> query = iom.getArgument(1);
            assertTrue(query.containsKey("qualifiedName"));
            assertEquals(query.get("qualifiedName"), QUALIFIED_NAME);
            return new AtlasEntity.AtlasEntityWithExtInfo(ae);
        });

        TestEntity subject = Mockito.spy(new TestEntity());
        // exists must return false at this point as we've just created the model but it doesn't have the corresponding AtlasEntity
        // and the cache should be empty
        assertFalse(subject.exists());
        assertSame(subject, subject.get());
        assertFalse(subject.exists());
        // ditto
        assertFalse(subject.atlasEntity().isPresent());
        // Because our client mock should return the mock entity, exists with Atlas check should find the entity,
        // cache it, and return true
        assertTrue(subject.exists(ac));
        // and call the method to update our model
        Mockito.verify(subject, Mockito.times(1)).updateJavaModel(Mockito.eq(ae));
        // Let's validate that exists with Atlas check did, in fact, query our atlas mock for the entity
        Mockito.verify(ac, Mockito.times(1)).getEntityByAttribute(Mockito.eq(TYPE_NAME), Mockito.anyMap());
        // the entity should exist in cache
        assertTrue(subject.exists());
        // and exists with Atlas check should use it
        assertTrue(subject.exists(ac));
        // so, let's verify that the item was pulled not from atlas (from cache will be the only option left)
        Mockito.verify(ac, Mockito.times(1)).getEntityByAttribute(Mockito.eq(TYPE_NAME), Mockito.anyMap());

        // This method should return filled Optional with our mocked entity pulled from cache
        // And, no matter how many times we call, the result should be the same (but let's make sure that we call it at least twice)
        int timesToLoadEntity = 2 + (int) (Math.random() * 98);
        for (int i = 0; i < timesToLoadEntity; i++) {
            assertSame(ae, subject.atlasEntity().get());
        }
        // verify that atlas entity was updated every time we requested it
        Mockito.verify(subject, Mockito.times(timesToLoadEntity)).updateAtlasEntity(Mockito.eq(ae));
        // verify that the model was not updated when we requested the entity second time
        Mockito.verify(subject, Mockito.times(1)).updateJavaModel(Mockito.eq(ae));
    }

    public class TestEntity extends CouchbaseAtlasEntity<TestEntity> {
        @Override
        public String atlasTypeName() {
            return TYPE_NAME;
        }

        @Override
        public UUID id() {
            return ID;
        }

        @Override
        protected String qualifiedName() {
            return QUALIFIED_NAME;
        }
    }
}
