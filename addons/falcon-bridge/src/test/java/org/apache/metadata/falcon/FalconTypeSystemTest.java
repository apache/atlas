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

package org.apache.metadata.falcon;

import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.types.ClassType;
import org.apache.hadoop.metadata.types.TraitType;
import org.apache.hadoop.metadata.types.TypeSystem;
import org.junit.Assert;
import org.testng.annotations.Test;

public class FalconTypeSystemTest {
    @Test
    public void testTypeSystem() throws MetadataException {
        FalconTypeSystem.getInstance();
        Assert.assertNotNull(TypeSystem.getInstance().getDataType(ClassType.class, FalconTypeSystem.DefinedTypes.CLUSTER.name()));
        Assert.assertNotNull(TypeSystem.getInstance().getDataType(TraitType.class, FalconTypeSystem.DefinedTypes.TAG.name()));
    }
}
