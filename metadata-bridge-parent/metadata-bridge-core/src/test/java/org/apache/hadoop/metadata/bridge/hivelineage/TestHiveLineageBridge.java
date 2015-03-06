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

package org.apache.hadoop.metadata.bridge.hivelineage;

import com.google.gson.Gson;
import org.apache.commons.collections.IteratorUtils;
import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.bridge.BridgeTypeBootstrapper;
import org.apache.hadoop.metadata.bridge.hivelineage.hook.HiveLineage;
import org.apache.hadoop.metadata.bridge.module.BridgeModule;
import org.apache.hadoop.metadata.repository.RepositoryException;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

@Guice(modules = {BridgeModule.class})
public class TestHiveLineageBridge {

    @Inject
    HiveLineageBridge bridge;

    @Inject
    BridgeTypeBootstrapper bootstrapper;

    HiveLineage hlb;

    // the id of one.json in the repo (test #1)
    String oneId;

    private HiveLineage loadHiveLineageBean(String path) throws IOException {
        return new Gson().fromJson(new InputStreamReader(this.getClass().getResourceAsStream(path)),
                HiveLineage.class);
    }

    @BeforeClass
    public void bootstrap() throws IOException, MetadataException {
        bootstrapper.bootstrap();
        hlb = loadHiveLineageBean("/one.json");
    }

    @Test(priority = 1, enabled = false)
    public void testCreate() throws MetadataException {
        // add the lineage bean to the repo
        oneId = bridge.create(hlb);

        // make sure this actually did worked
        Assert.assertNotNull(oneId);
    }

    @Test(priority = 2, enabled = false)
    public void testGet() throws RepositoryException, IOException {
        Object bean = bridge.get(oneId);

        Assert.assertEquals(hlb, bean);
    }

    @Test(priority = 3, enabled = false)
    public void testList() throws RepositoryException {
        List<String> list = IteratorUtils.toList(bridge.list().iterator());

        Assert.assertEquals(list.size(), 1);
        Assert.assertEquals(list.get(0), oneId);
    }
}
