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

import java.util.Iterator;
import java.util.List;

import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.testng.Assert;


/**
 * Run tests in {@link ReverseReferenceUpdateTestBase} with soft delete enabled.
 *
 */
public class ReverseReferenceUpdateSoftDeleteTest extends ReverseReferenceUpdateTestBase {

    @Override
    DeleteHandler getDeleteHandler(TypeSystem typeSystem) {

        return new SoftDeleteHandler(typeSystem);
    }

    @Override
    void assertTestOneToOneReference(Object actual, ITypedReferenceableInstance expectedValue, ITypedReferenceableInstance referencingInstance) throws Exception {
        // Verify reference was not disconnected if soft deletes are enabled.
        Assert.assertNotNull(actual);
        Assert.assertTrue(actual instanceof ITypedReferenceableInstance);
        ITypedReferenceableInstance referenceValue = (ITypedReferenceableInstance) actual;
        Assert.assertEquals(referenceValue.getId()._getId(), expectedValue.getId()._getId());

        //Verify reference edge was marked as DELETED.
        AtlasVertex vertexForGUID = GraphHelper.getInstance().getVertexForGUID(referencingInstance.getId()._getId());
        String edgeLabel = GraphHelper.getEdgeLabel(typeB, typeB.fieldMapping.fields.get("a"));
        AtlasEdge edgeForLabel = GraphHelper.getInstance().getEdgeForLabel(vertexForGUID, edgeLabel);
        String edgeState = edgeForLabel.getProperty(Constants.STATE_PROPERTY_KEY, String.class);
        Assert.assertEquals(edgeState, Id.EntityState.DELETED.name());
    }

    @Override
 void assertTestOneToManyReference(Object object, ITypedReferenceableInstance referencingInstance) throws Exception {
        // Verify reference was not disconnected if soft deletes are enabled.
        Assert.assertTrue(object instanceof List);
        List<ITypedReferenceableInstance> refValues = (List<ITypedReferenceableInstance>) object;
        Assert.assertEquals(refValues.size(), 2);

        // Verify that one of the reference edges is marked DELETED.
        AtlasVertex vertexForGUID = GraphHelper.getInstance().getVertexForGUID(referencingInstance.getId()._getId());
        String edgeLabel = GraphHelper.getEdgeLabel(typeB, typeB.fieldMapping.fields.get("manyA"));
        Iterator<AtlasEdge> outGoingEdgesByLabel = GraphHelper.getInstance().getOutGoingEdgesByLabel(vertexForGUID, edgeLabel);
        boolean found = false;
        while (outGoingEdgesByLabel.hasNext()) {
            AtlasEdge edge = outGoingEdgesByLabel.next();
            String edgeState = edge.getProperty(Constants.STATE_PROPERTY_KEY, String.class);
            if (edgeState.equals(Id.EntityState.DELETED.name())) {
                found = true;
                break;
            }
        }
        Assert.assertTrue(found, "One edge for label " + edgeLabel + " should be marked " + Id.EntityState.DELETED.name());
    }

}
