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

import java.util.List;

import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.testng.Assert;


/**
 * Run tests in {@link ReverseReferenceUpdateTestBase} with hard delete enabled.
 *
 */
public class ReverseReferenceUpdateHardDeleteTest extends ReverseReferenceUpdateTestBase {

    @Override
    DeleteHandler getDeleteHandler(TypeSystem typeSystem) {

        return new HardDeleteHandler(typeSystem);
    }

    @Override
    void assertTestOneToOneReference(Object refValue, ITypedReferenceableInstance expectedValue, ITypedReferenceableInstance referencingInstance) throws Exception {
        // Verify reference was disconnected
        Assert.assertNull(refValue);
    }

    @Override
    void assertTestOneToManyReference(Object object, ITypedReferenceableInstance referencingInstance) {
        Assert.assertTrue(object instanceof List);
        List<ITypedReferenceableInstance> refValues = (List<ITypedReferenceableInstance>) object;
        Assert.assertEquals(refValues.size(), 1);
    }

}
