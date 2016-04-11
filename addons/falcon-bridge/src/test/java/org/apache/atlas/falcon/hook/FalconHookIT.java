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

package org.apache.atlas.falcon.hook;

import com.sun.jersey.api.client.ClientResponse;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.falcon.model.FalconDataModelGenerator;
import org.apache.atlas.falcon.model.FalconDataTypes;
import org.apache.atlas.hive.bridge.HiveMetaStoreBridge;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.falcon.atlas.service.AtlasService;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.security.CurrentUser;
import org.apache.hadoop.hive.conf.HiveConf;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.xml.bind.JAXBException;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class FalconHookIT {
    public static final Logger LOG = org.slf4j.LoggerFactory.getLogger(FalconHookIT.class);

    public static final String CLUSTER_RESOURCE = "/cluster.xml";
    public static final String FEED_RESOURCE = "/feed.xml";
    public static final String FEED_HDFS_RESOURCE = "/feed-hdfs.xml";
    public static final String PROCESS_RESOURCE = "/process.xml";

    private AtlasClient atlasClient;

    private static final ConfigurationStore STORE = ConfigurationStore.get();

    @BeforeClass
    public void setUp() throws Exception {
        Configuration atlasProperties = ApplicationProperties.get();
        atlasClient = new AtlasClient(atlasProperties.getString("atlas.rest.address"));

        AtlasService service = new AtlasService();
        service.init();
        STORE.registerListener(service);
        registerFalconDataModel();
        CurrentUser.authenticate(System.getProperty("user.name"));
    }

    private void registerFalconDataModel() throws Exception {
        if (isDataModelAlreadyRegistered()) {
            LOG.info("Falcon data model is already registered!");
            return;
        }

        HiveMetaStoreBridge hiveMetaStoreBridge = new HiveMetaStoreBridge(new HiveConf(), atlasClient);
        hiveMetaStoreBridge.registerHiveDataModel();

        FalconDataModelGenerator dataModelGenerator = new FalconDataModelGenerator();
        LOG.info("Registering Falcon data model");
        atlasClient.createType(dataModelGenerator.getModelAsJson());
    }

    private boolean isDataModelAlreadyRegistered() throws Exception {
        try {
            atlasClient.getType(FalconDataTypes.FALCON_PROCESS_ENTITY.getName());
            LOG.info("Hive data model is already registered!");
            return true;
        } catch(AtlasServiceException ase) {
            if (ase.getStatus() == ClientResponse.Status.NOT_FOUND) {
                return false;
            }
            throw ase;
        }
    }

    private <T extends Entity> T loadEntity(EntityType type, String resource, String name) throws JAXBException {
        Entity entity = (Entity) type.getUnmarshaller().unmarshal(this.getClass().getResourceAsStream(resource));
        switch (entity.getEntityType()) {
            case CLUSTER:
                ((Cluster) entity).setName(name);
                break;

            case FEED:
                ((Feed) entity).setName(name);
                break;

            case PROCESS:
                ((org.apache.falcon.entity.v0.process.Process) entity).setName(name);
                break;
        }
        return (T)entity;
    }

    private String random() {
        return RandomStringUtils.randomAlphanumeric(10);
    }

    private String getTableUri(String dbName, String tableName) {
        return String.format("catalog:%s:%s#ds=${YEAR}-${MONTH}-${DAY}-${HOUR}", dbName, tableName);
    }

    @Test (enabled = true)
    public void testCreateProcess() throws Exception {
        Cluster cluster = loadEntity(EntityType.CLUSTER, CLUSTER_RESOURCE, "cluster" + random());
        STORE.publish(EntityType.CLUSTER, cluster);

        Feed infeed = getTableFeed(FEED_RESOURCE, cluster.getName());
        String inTableName = getTableName(infeed);
        String inDbName = getDBName(infeed);

        Feed outfeed = getTableFeed(FEED_RESOURCE, cluster.getName());
        String outTableName = getTableName(outfeed);
        String outDbName = getDBName(outfeed);

        Process process = loadEntity(EntityType.PROCESS, PROCESS_RESOURCE, "process" + random());
        process.getClusters().getClusters().get(0).setName(cluster.getName());
        process.getInputs().getInputs().get(0).setFeed(infeed.getName());
        process.getOutputs().getOutputs().get(0).setFeed(outfeed.getName());
        STORE.publish(EntityType.PROCESS, process);

        String pid = assertProcessIsRegistered(cluster.getName(), process.getName());
        Referenceable processEntity = atlasClient.getEntity(pid);
        assertNotNull(processEntity);
        assertEquals(processEntity.get("processName"), process.getName());

        Id inId = (Id) ((List)processEntity.get("inputs")).get(0);
        Referenceable inEntity = atlasClient.getEntity(inId._getId());
        assertEquals(inEntity.get("name"),
                HiveMetaStoreBridge.getTableQualifiedName(cluster.getName(), inDbName, inTableName));

        Id outId = (Id) ((List)processEntity.get("outputs")).get(0);
        Referenceable outEntity = atlasClient.getEntity(outId._getId());
        assertEquals(outEntity.get("name"),
                HiveMetaStoreBridge.getTableQualifiedName(cluster.getName(), outDbName, outTableName));
    }

    private Feed getTableFeed(String feedResource, String clusterName) throws Exception {
        Feed feed = loadEntity(EntityType.FEED, feedResource, "feed" + random());
        org.apache.falcon.entity.v0.feed.Cluster feedCluster = feed.getClusters().getClusters().get(0);
        feedCluster.setName(clusterName);
        feedCluster.getTable().setUri(getTableUri("db" + random(), "table" + random()));
        STORE.publish(EntityType.FEED, feed);
        return feed;
    }

    private String getDBName(Feed feed) {
        String uri = feed.getClusters().getClusters().get(0).getTable().getUri();
        String[] parts = uri.split(":");
        return parts[1];
    }

    private String getTableName(Feed feed) {
        String uri = feed.getClusters().getClusters().get(0).getTable().getUri();
        String[] parts = uri.split(":");
        parts = parts[2].split("#");
        return parts[0];
    }

    @Test (enabled = true)
    public void testCreateProcessWithHDFSFeed() throws Exception {
        Cluster cluster = loadEntity(EntityType.CLUSTER, CLUSTER_RESOURCE, "cluster" + random());
        STORE.publish(EntityType.CLUSTER, cluster);

        Feed infeed = loadEntity(EntityType.FEED, FEED_HDFS_RESOURCE, "feed" + random());
        org.apache.falcon.entity.v0.feed.Cluster feedCluster = infeed.getClusters().getClusters().get(0);
        feedCluster.setName(cluster.getName());
        STORE.publish(EntityType.FEED, infeed);

        Feed outfeed = getTableFeed(FEED_RESOURCE, cluster.getName());
        String outTableName = getTableName(outfeed);
        String outDbName = getDBName(outfeed);

        Process process = loadEntity(EntityType.PROCESS, PROCESS_RESOURCE, "process" + random());
        process.getClusters().getClusters().get(0).setName(cluster.getName());
        process.getInputs().getInputs().get(0).setFeed(infeed.getName());
        process.getOutputs().getOutputs().get(0).setFeed(outfeed.getName());
        STORE.publish(EntityType.PROCESS, process);

        String pid = assertProcessIsRegistered(cluster.getName(), process.getName());
        Referenceable processEntity = atlasClient.getEntity(pid);
        assertEquals(processEntity.get("processName"), process.getName());
        assertNull(processEntity.get("inputs"));

        Id outId = (Id) ((List)processEntity.get("outputs")).get(0);
        Referenceable outEntity = atlasClient.getEntity(outId._getId());
        assertEquals(outEntity.get("name"),
                HiveMetaStoreBridge.getTableQualifiedName(cluster.getName(), outDbName, outTableName));
    }

    //    @Test (enabled = true, dependsOnMethods = "testCreateProcess")
//    public void testUpdateProcess() throws Exception {
//        FalconEvent event = createProcessEntity(PROCESS_NAME_2, INPUT, OUTPUT);
//        FalconEventPublisher.Data data = new FalconEventPublisher.Data(event);
//        hook.publish(data);
//        String id = assertProcessIsRegistered(CLUSTER_NAME, PROCESS_NAME_2);
//        event = createProcessEntity(PROCESS_NAME_2, INPUT_2, OUTPUT_2);
//        hook.publish(data);
//        String id2 = assertProcessIsRegistered(CLUSTER_NAME, PROCESS_NAME_2);
//        if (!id.equals(id2)) {
//            throw new Exception("Id mismatch");
//        }
//    }

    private String assertProcessIsRegistered(String clusterName, String processName) throws Exception {
        String name = processName + "@" + clusterName;
        LOG.debug("Searching for process {}", name);
        String query = String.format("%s as t where name = '%s' select t",
                FalconDataTypes.FALCON_PROCESS_ENTITY.getName(), name);
        return assertEntityIsRegistered(query);
    }

    private String assertEntityIsRegistered(final String query) throws Exception {
        waitFor(2000000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                JSONArray results = atlasClient.search(query);
                System.out.println(results);
                return results.length() == 1;
            }
        });

        JSONArray results = atlasClient.search(query);
        JSONObject row = results.getJSONObject(0).getJSONObject("t");

        return row.getString("id");
    }


    public interface Predicate {

        /**
         * Perform a predicate evaluation.
         *
         * @return the boolean result of the evaluation.
         * @throws Exception thrown if the predicate evaluation could not evaluate.
         */
        boolean evaluate() throws Exception;
    }

    /**
     * Wait for a condition, expressed via a {@link Predicate} to become true.
     *
     * @param timeout maximum time in milliseconds to wait for the predicate to become true.
     * @param predicate predicate waiting on.
     */
    protected void waitFor(int timeout, Predicate predicate) throws Exception {
        long mustEnd = System.currentTimeMillis() + timeout;

        boolean eval;
        while (!(eval = predicate.evaluate()) && System.currentTimeMillis() < mustEnd) {
            LOG.info("Waiting up to {} msec", mustEnd - System.currentTimeMillis());
            Thread.sleep(1000);
        }
        if (!eval) {
            throw new Exception("Waiting timed out after " + timeout + " msec");
        }
    }

}
