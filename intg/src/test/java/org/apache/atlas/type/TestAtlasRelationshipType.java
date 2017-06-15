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
package org.apache.atlas.type;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.typedef.AtlasRelationshipDef;
import org.apache.atlas.model.typedef.AtlasRelationshipEndDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.testng.annotations.Test;

import static org.testng.Assert.fail;
public class TestAtlasRelationshipType {
    @Test
    public void testvalidateAtlasRelationshipDef() throws AtlasBaseException {
        AtlasRelationshipEndDef ep1 = new AtlasRelationshipEndDef("typeA", "attr1", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE);
        AtlasRelationshipEndDef ep2 = new AtlasRelationshipEndDef("typeB", "attr2", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE);
        AtlasRelationshipEndDef ep3 = new AtlasRelationshipEndDef("typeC", "attr2", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE, true);
        AtlasRelationshipEndDef ep4 = new AtlasRelationshipEndDef("typeD", "attr2", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE, true);
        AtlasRelationshipEndDef ep5 = new AtlasRelationshipEndDef("typeA", "attr1", AtlasStructDef.AtlasAttributeDef.Cardinality.SET,true);
        AtlasRelationshipEndDef ep6 = new AtlasRelationshipEndDef("typeA", "attr1", AtlasStructDef.AtlasAttributeDef.Cardinality.LIST,true);
        AtlasRelationshipDef relationshipDef1 = new AtlasRelationshipDef("emptyRelationshipDef", "desc 1", "version1",
                AtlasRelationshipDef.RelationshipCategory.ASSOCIATION, AtlasRelationshipDef.PropagateTags.ONE_TO_TWO, ep1, ep2);
        AtlasRelationshipDef relationshipDef2 = new AtlasRelationshipDef("emptyRelationshipDef", "desc 1", "version1",
                AtlasRelationshipDef.RelationshipCategory.COMPOSITION, AtlasRelationshipDef.PropagateTags.ONE_TO_TWO, ep1, ep3);
        AtlasRelationshipDef relationshipDef3 = new AtlasRelationshipDef("emptyRelationshipDef", "desc 1", "version1",
                AtlasRelationshipDef.RelationshipCategory.AGGREGATION, AtlasRelationshipDef.PropagateTags.ONE_TO_TWO, ep1, ep3);

        try {
            AtlasRelationshipDef relationshipDef = new AtlasRelationshipDef("emptyRelationshipDef", "desc 1", "version1",
                    AtlasRelationshipDef.RelationshipCategory.ASSOCIATION, AtlasRelationshipDef.PropagateTags.ONE_TO_TWO, ep3, ep2);
            AtlasRelationshipType.validateAtlasRelationshipDef(relationshipDef);
            fail("This call is expected to fail");
        } catch (AtlasBaseException abe) {
            if (!abe.getAtlasErrorCode().equals(AtlasErrorCode.RELATIONSHIPDEF_ASSOCIATION_AND_CONTAINER)) {
                fail("This call expected a different error");
            }
        }
        try {
            AtlasRelationshipDef relationshipDef = new AtlasRelationshipDef("emptyRelationshipDef", "desc 1", "version1",
                    AtlasRelationshipDef.RelationshipCategory.COMPOSITION, AtlasRelationshipDef.PropagateTags.ONE_TO_TWO, ep1, ep2);
            AtlasRelationshipType.validateAtlasRelationshipDef(relationshipDef);
            fail("This call is expected to fail");
        } catch (AtlasBaseException abe) {
            if (!abe.getAtlasErrorCode().equals(AtlasErrorCode.RELATIONSHIPDEF_COMPOSITION_NO_CONTAINER)) {
                fail("This call expected a different error");
            }
        }
        try {
            AtlasRelationshipDef relationshipDef = new AtlasRelationshipDef("emptyRelationshipDef", "desc 1", "version1",
                    AtlasRelationshipDef.RelationshipCategory.AGGREGATION, AtlasRelationshipDef.PropagateTags.ONE_TO_TWO, ep1, ep2);
            AtlasRelationshipType.validateAtlasRelationshipDef(relationshipDef);
            fail("This call is expected to fail");
        } catch (AtlasBaseException abe) {
            if (!abe.getAtlasErrorCode().equals(AtlasErrorCode.RELATIONSHIPDEF_AGGREGATION_NO_CONTAINER)) {
                fail("This call expected a different error");
            }
        }

        try {
            AtlasRelationshipDef relationshipDef = new AtlasRelationshipDef("emptyRelationshipDef", "desc 1", "version1",
                    AtlasRelationshipDef.RelationshipCategory.COMPOSITION, AtlasRelationshipDef.PropagateTags.ONE_TO_TWO, ep1, ep5);
            AtlasRelationshipType.validateAtlasRelationshipDef(relationshipDef);
            fail("This call is expected to fail");
        } catch (AtlasBaseException abe) {
            if (!abe.getAtlasErrorCode().equals(AtlasErrorCode.RELATIONSHIPDEF_COMPOSITION_SET_CONTAINER)) {
                fail("This call expected a different error");
            }
        }
        try {
            AtlasRelationshipDef relationshipDef = new AtlasRelationshipDef("emptyRelationshipDef", "desc 1", "version1",
                    AtlasRelationshipDef.RelationshipCategory.COMPOSITION, AtlasRelationshipDef.PropagateTags.ONE_TO_TWO, ep1, ep6);
            AtlasRelationshipType.validateAtlasRelationshipDef(relationshipDef);
            fail("This call is expected to fail");
        } catch (AtlasBaseException abe) {
            if (!abe.getAtlasErrorCode().equals(AtlasErrorCode.RELATIONSHIPDEF_LIST_ON_END)) {
                fail("This call expected a different error");
            }
        }
        try {
            AtlasRelationshipDef relationshipDef = new AtlasRelationshipDef("emptyRelationshipDef", "desc 1", "version1",
                    AtlasRelationshipDef.RelationshipCategory.COMPOSITION, AtlasRelationshipDef.PropagateTags.ONE_TO_TWO, ep6, ep1);
            AtlasRelationshipType.validateAtlasRelationshipDef(relationshipDef);
            fail("This call is expected to fail");
        } catch (AtlasBaseException abe) {
            if (!abe.getAtlasErrorCode().equals(AtlasErrorCode.RELATIONSHIPDEF_LIST_ON_END)) {
                fail("This call expected a different error");
            }
        }

    }
}
