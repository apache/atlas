/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.catalog.projection;

import com.tinkerpop.blueprints.Vertex;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.typesystem.persistence.Id;
import org.testng.annotations.Test;

import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Unit tests for TagRelation
 */
public class TagRelationTest {
    @Test
    public void testIsDeleted() {
        Vertex v = createStrictMock(Vertex.class);
        expect(v.getProperty(Constants.STATE_PROPERTY_KEY)).andReturn(Id.EntityState.ACTIVE.name());
        replay(v);

        BaseRelation relation = new TagRelation();
        assertFalse(relation.isDeleted(v));
    }

    @Test
    public void testIsDeleted_false() {
        Vertex v = createStrictMock(Vertex.class);
        expect(v.getProperty(Constants.STATE_PROPERTY_KEY)).andReturn(Id.EntityState.DELETED.name());
        replay(v);

        BaseRelation relation = new TagRelation();
        assertTrue(relation.isDeleted(v));
    }
}
