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

package org.apache.hadoop.metadata.bridge;

import org.apache.hadoop.metadata.RepositoryMetadataModule;
import org.apache.hadoop.metadata.repository.MetadataRepository;
import org.testng.Assert;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;


@Guice(modules = RepositoryMetadataModule.class)
public class BridgeManagerTest {

    @Inject
    MetadataRepository repo;

    @Test(enabled = false)
    public void testLoadPropertiesFile() throws Exception {
        BridgeManager bm = new BridgeManager(repo);
        System.out.println(bm.getActiveBridges().size());

        Assert.assertEquals(bm.activeBridges.get(0).getClass().getSimpleName(),
                "HiveLineageBridge");
    }

    @Test
    public void testBeanConvertion() {

        //Tests Conversion of Bean to Type
    }

    @Test
    public void testIRefConvertion() {

        //Tests Conversion of IRef cast to Bean
    }


}
