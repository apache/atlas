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

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.falcon.model.FalconDataTypes;
import org.apache.atlas.hive.bridge.HiveMetaStoreBridge;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.falcon.atlas.service.AtlasService;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.security.CurrentUser;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.xml.bind.JAXBException;
import java.util.List;

import static org.testng.Assert.assertEquals;

public class FalconHookIT {
    public static final Logger LOG = org.slf4j.LoggerFactory.getLogger(FalconHookIT.class);

    public static final String CLUSTER_RESOURCE = "/cluster.xml";
    public static final String FEED_RESOURCE = "/feed.xml";
    public static final String PROCESS_RESOURCE = "/process.xml";

    private AtlasClient dgiCLient;

    private static final ConfigurationStore STORE = ConfigurationStore.get();

    @BeforeClass
    public void setUp() throws Exception {
        dgiCLient = new AtlasClient(ApplicationProperties.get().getString("atlas.rest.address"));

        AtlasService service = new AtlasService();
        service.init();
        STORE.registerListener(service);
        CurrentUser.authenticate(System.getProperty("user.name"));
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

        Feed infeed = loadEntity(EntityType.FEED, FEED_RESOURCE, "feedin" + random());
        org.apache.falcon.entity.v0.feed.Cluster feedCluster = infeed.getClusters().getClusters().get(0);
        feedCluster.setName(cluster.getName());
        String inTableName = "table" + random();
        String inDbName = "db" + random();
        feedCluster.getTable().setUri(getTableUri(inDbName, inTableName));
        STORE.publish(EntityType.FEED, infeed);

        Feed outfeed = loadEntity(EntityType.FEED, FEED_RESOURCE, "feedout" + random());
        feedCluster = outfeed.getClusters().getClusters().get(0);
        feedCluster.setName(cluster.getName());
        String outTableName = "table" + random();
        String outDbName = "db" + random();
        feedCluster.getTable().setUri(getTableUri(outDbName, outTableName));
        STORE.publish(EntityType.FEED, outfeed);

        Process process = loadEntity(EntityType.PROCESS, PROCESS_RESOURCE, "process" + random());
        process.getClusters().getClusters().get(0).setName(cluster.getName());
        process.getInputs().getInputs().get(0).setFeed(infeed.getName());
        process.getOutputs().getOutputs().get(0).setFeed(outfeed.getName());
        STORE.publish(EntityType.PROCESS, process);

        String pid = assertProcessIsRegistered(cluster.getName(), process.getName());
        Referenceable processEntity = dgiCLient.getEntity(pid);
        assertEquals(processEntity.get("processName"), process.getName());

        Id inId = (Id) ((List)processEntity.get("inputs")).get(0);
        Referenceable inEntity = dgiCLient.getEntity(inId._getId());
        assertEquals(inEntity.get("name"),
                HiveMetaStoreBridge.getTableQualifiedName(cluster.getName(), inDbName, inTableName));

        Id outId = (Id) ((List)processEntity.get("outputs")).get(0);
        Referenceable outEntity = dgiCLient.getEntity(outId._getId());
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
        waitFor(20000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                JSONArray results = dgiCLient.search(query);
                System.out.println(results);
                return results.length() == 1;
            }
        });

        JSONArray results = dgiCLient.search(query);
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
