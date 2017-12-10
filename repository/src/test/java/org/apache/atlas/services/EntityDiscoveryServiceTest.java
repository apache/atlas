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

package org.apache.atlas.services;

import com.google.inject.Inject;
import org.apache.atlas.TestModules;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.junit.Assert;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

@Guice(modules = TestModules.TestOnlyModule.class)
public class EntityDiscoveryServiceTest {
    @Inject
    AtlasTypeRegistry typeRegistry;

    @Inject
    private AtlasTypeDefStore typeDefStore;

    @Inject
    private AtlasGraph atlasGraph;

    @Inject
    EntityDiscoveryService entityDiscoveryService;

    @Test
    public void dslTest() throws AtlasBaseException {
        //String dslQuery = "DB where name = \"Reporting\"";
        String dslQuery = "hive_table where Asset.name = \"testtable_x_0\"";

        AtlasSearchResult result = entityDiscoveryService.searchUsingDslQuery(dslQuery, 20 , 0);

        Assert.assertNotNull(result);
    }
}
