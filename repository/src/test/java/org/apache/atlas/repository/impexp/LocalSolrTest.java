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
package org.apache.atlas.repository.impexp;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.repository.graph.GraphBackedSearchIndexer;
import org.apache.atlas.runner.LocalSolrRunner;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.configuration.Configuration;
import org.junit.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import javax.inject.Inject;

import static org.apache.atlas.graph.GraphSandboxUtil.useLocalSolr;

public class LocalSolrTest {
    @Inject
    AtlasTypeRegistry typeRegistry;

    @BeforeTest
    public void setUp() throws Exception {
        if (useLocalSolr()) {
            try {
                LocalSolrRunner.start();

                Configuration configuration = ApplicationProperties.get();
                configuration.setProperty("atlas.graph.index.search.solr.zookeeper-url", LocalSolrRunner.getZookeeperUrls());
            } catch (Exception e) {
                //ignore
            }
        }

        new GraphBackedSearchIndexer(typeRegistry);
    }

    @Test
    public void testLocalSolr() {
        String zookeeperUrls = LocalSolrRunner.getZookeeperUrls();
        System.out.println("Started Local Solr at: " + zookeeperUrls);

        Assert.assertNotNull(zookeeperUrls);
    }

    @AfterTest
    public void tearDown() throws Exception {
        System.out.println("Stopping Local Solr...");
        LocalSolrRunner.stop();
    }
}