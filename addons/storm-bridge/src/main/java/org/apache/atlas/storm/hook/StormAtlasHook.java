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

package org.apache.atlas.storm.hook;

import org.apache.storm.ISubmitterHook;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.utils.Utils;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasConstants;
import org.apache.atlas.fs.model.FSDataTypes;
import org.apache.atlas.hive.bridge.HiveMetaStoreBridge;
import org.apache.atlas.hive.model.HiveDataModelGenerator;
import org.apache.atlas.hook.AtlasHook;
import org.apache.atlas.storm.model.StormDataTypes;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * StormAtlasHook sends storm topology metadata information to Atlas
 * via a Kafka Broker for durability.
 * <p/>
 * This is based on the assumption that the same topology name is used
 * for the various lifecycle stages.
 */
public class StormAtlasHook extends AtlasHook implements ISubmitterHook {

    public static final Logger LOG = org.slf4j.LoggerFactory.getLogger(StormAtlasHook.class);

    private static final String CONF_PREFIX = "atlas.hook.storm.";
    private static final String HOOK_NUM_RETRIES = CONF_PREFIX + "numRetries";
    // will be used for owner if Storm topology does not contain the owner instance
    // possible if Storm is running in unsecure mode.
    public static final String ANONYMOUS_OWNER = "anonymous";

    public static final String HBASE_NAMESPACE_DEFAULT = "default";

    @Override
    protected String getNumberOfRetriesPropertyKey() {
        return HOOK_NUM_RETRIES;
    }

    /**
     * This is the client-side hook that storm fires when a topology is added.
     *
     * @param topologyInfo topology info
     * @param stormConf configuration
     * @param stormTopology a storm topology
     * @throws IllegalAccessException
     */
    @Override
    public void notify(TopologyInfo topologyInfo, Map stormConf,
                       StormTopology stormTopology) throws IllegalAccessException {

        LOG.info("Collecting metadata for a new storm topology: {}", topologyInfo.get_name());
        try {
            ArrayList<Referenceable> entities = new ArrayList<>();
            Referenceable topologyReferenceable = createTopologyInstance(topologyInfo, stormConf);
            List<Referenceable> dependentEntities = addTopologyDataSets(stormTopology, topologyReferenceable,
                    topologyInfo.get_owner(), stormConf);
            if (dependentEntities.size()>0) {
                entities.addAll(dependentEntities);
            }
            // create the graph for the topology
            ArrayList<Referenceable> graphNodes = createTopologyGraph(
                    stormTopology, stormTopology.get_spouts(), stormTopology.get_bolts());
            // add the connection from topology to the graph
            topologyReferenceable.set("nodes", graphNodes);
            entities.add(topologyReferenceable);

            LOG.debug("notifying entities, size = {}", entities.size());
            String user = getUser(topologyInfo.get_owner(), null);
            notifyEntities(user, entities);
        } catch (Exception e) {
            throw new RuntimeException("Atlas hook is unable to process the topology.", e);
        }
    }

    private Referenceable createTopologyInstance(TopologyInfo topologyInfo, Map stormConf) throws Exception {
        Referenceable topologyReferenceable = new Referenceable(
                StormDataTypes.STORM_TOPOLOGY.getName());
        topologyReferenceable.set("id", topologyInfo.get_id());
        topologyReferenceable.set(AtlasClient.NAME, topologyInfo.get_name());
        topologyReferenceable.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, topologyInfo.get_name());
        String owner = topologyInfo.get_owner();
        if (StringUtils.isEmpty(owner)) {
            owner = ANONYMOUS_OWNER;
        }
        topologyReferenceable.set("owner", owner);
        topologyReferenceable.set("startTime", System.currentTimeMillis());
        topologyReferenceable.set(AtlasConstants.CLUSTER_NAME_ATTRIBUTE, getClusterName(stormConf));

        return topologyReferenceable;
    }

    private List<Referenceable> addTopologyDataSets(StormTopology stormTopology,
                                                    Referenceable topologyReferenceable,
                                                    String topologyOwner,
                                                    Map stormConf) throws Exception {
        List<Referenceable> dependentEntities = new ArrayList<>();
        // add each spout as an input data set
        addTopologyInputs(topologyReferenceable,
                stormTopology.get_spouts(), stormConf, topologyOwner, dependentEntities);
        // add the appropriate bolts as output data sets
        addTopologyOutputs(topologyReferenceable, stormTopology, topologyOwner, stormConf, dependentEntities);
        return dependentEntities;
    }

    private void addTopologyInputs(Referenceable topologyReferenceable,
                                   Map<String, SpoutSpec> spouts,
                                   Map stormConf,
                                   String topologyOwner, List<Referenceable> dependentEntities) throws IllegalAccessException {
        final ArrayList<Referenceable> inputDataSets = new ArrayList<>();
        for (Map.Entry<String, SpoutSpec> entry : spouts.entrySet()) {
            Serializable instance = Utils.javaDeserialize(
                    entry.getValue().get_spout_object().get_serialized_java(), Serializable.class);

            String simpleName = instance.getClass().getSimpleName();
            final Referenceable datasetRef = createDataSet(simpleName, topologyOwner, instance, stormConf, dependentEntities);
            if (datasetRef != null) {
                inputDataSets.add(datasetRef);
            }
        }

        topologyReferenceable.set("inputs", inputDataSets);
    }

    private void addTopologyOutputs(Referenceable topologyReferenceable,
                                    StormTopology stormTopology, String topologyOwner,
                                    Map stormConf, List<Referenceable> dependentEntities) throws Exception {
        final ArrayList<Referenceable> outputDataSets = new ArrayList<>();

        Map<String, Bolt> bolts = stormTopology.get_bolts();
        Set<String> terminalBoltNames = StormTopologyUtil.getTerminalUserBoltNames(stormTopology);
        for (String terminalBoltName : terminalBoltNames) {
            Serializable instance = Utils.javaDeserialize(bolts.get(terminalBoltName)
                    .get_bolt_object().get_serialized_java(), Serializable.class);

            String dataSetType = instance.getClass().getSimpleName();
            final Referenceable datasetRef = createDataSet(dataSetType, topologyOwner, instance, stormConf, dependentEntities);
            if (datasetRef != null) {
                outputDataSets.add(datasetRef);
            }
        }

        topologyReferenceable.set("outputs", outputDataSets);
    }

    private Referenceable createDataSet(String name, String topologyOwner,
                                              Serializable instance,
                                              Map stormConf, List<Referenceable> dependentEntities) throws IllegalAccessException {
        Map<String, String> config = StormTopologyUtil.getFieldValues(instance, true);

        String clusterName = null;
        Referenceable dataSetReferenceable;
        // todo: need to redo this with a config driven approach
        switch (name) {
            case "KafkaSpout":
                dataSetReferenceable = new Referenceable(StormDataTypes.KAFKA_TOPIC.getName());
                final String topicName = config.get("KafkaSpout._spoutConfig.topic");
                dataSetReferenceable.set("topic", topicName);
                dataSetReferenceable.set("uri",
                        config.get("KafkaSpout._spoutConfig.hosts.brokerZkStr"));
                if (StringUtils.isEmpty(topologyOwner)) {
                    topologyOwner = ANONYMOUS_OWNER;
                }
                dataSetReferenceable.set("owner", topologyOwner);
                dataSetReferenceable.set("name", getKafkaTopicQualifiedName(getClusterName(stormConf), topicName));
                break;

            case "HBaseBolt":
                dataSetReferenceable = new Referenceable(StormDataTypes.HBASE_TABLE.getName());
                final String hbaseTableName = config.get("HBaseBolt.tableName");
                dataSetReferenceable.set("uri", stormConf.get("hbase.rootdir"));
                dataSetReferenceable.set("tableName", hbaseTableName);
                dataSetReferenceable.set("owner", stormConf.get("storm.kerberos.principal"));
                clusterName = extractComponentClusterName(HBaseConfiguration.create(), stormConf);
                //TODO - Hbase Namespace is hardcoded to 'default'. need to check how to get this or is it already part of tableName
                dataSetReferenceable.set("name", getHbaseTableQualifiedName(clusterName, HBASE_NAMESPACE_DEFAULT,
                        hbaseTableName));
                break;

            case "HdfsBolt":
                dataSetReferenceable = new Referenceable(FSDataTypes.HDFS_PATH().toString());
                String hdfsUri = config.get("HdfsBolt.rotationActions") == null
                        ? config.get("HdfsBolt.fileNameFormat.path")
                        : config.get("HdfsBolt.rotationActions");
                final String hdfsPathStr = config.get("HdfsBolt.fsUrl") + hdfsUri;
                dataSetReferenceable.set(AtlasConstants.CLUSTER_NAME_ATTRIBUTE, getClusterName(stormConf));
                dataSetReferenceable.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, hdfsPathStr);
                dataSetReferenceable.set("path", hdfsPathStr);
                dataSetReferenceable.set("owner", stormConf.get("hdfs.kerberos.principal"));
                //Fix after ATLAS-542
//                final Path hdfsPath = new Path(hdfsPathStr);
//                dataSetReferenceable.set(AtlasClient.NAME, hdfsPath.getName());
                dataSetReferenceable.set(AtlasClient.NAME, hdfsPathStr);
                break;

            case "HiveBolt":
                // todo: verify if hive table has everything needed to retrieve existing table
                Referenceable dbReferenceable = new Referenceable("hive_db");
                String databaseName = config.get("HiveBolt.options.databaseName");
                dbReferenceable.set(HiveDataModelGenerator.NAME, databaseName);
                dbReferenceable.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
                        HiveMetaStoreBridge.getDBQualifiedName(getClusterName(stormConf), databaseName));
                dbReferenceable.set(AtlasConstants.CLUSTER_NAME_ATTRIBUTE, getClusterName(stormConf));
                dependentEntities.add(dbReferenceable);
                clusterName = extractComponentClusterName(new HiveConf(), stormConf);
                final String hiveTableName = config.get("HiveBolt.options.tableName");
                dataSetReferenceable = new Referenceable("hive_table");
                final String tableQualifiedName = HiveMetaStoreBridge.getTableQualifiedName(clusterName,
                        databaseName, hiveTableName);
                dataSetReferenceable.set(HiveDataModelGenerator.NAME, tableQualifiedName);
                dataSetReferenceable.set(HiveDataModelGenerator.DB, dbReferenceable);
                dataSetReferenceable.set(HiveDataModelGenerator.TABLE_NAME, hiveTableName);
                break;

            default:
                // custom node - create a base dataset class with name attribute
                //TODO - What should we do for custom data sets. Not sure what name we can set here?
                return null;
        }
        dependentEntities.add(dataSetReferenceable);


        return dataSetReferenceable;
    }

    private String extractComponentClusterName(Configuration configuration, Map stormConf) {
        String clusterName = configuration.get(AtlasConstants.CLUSTER_NAME_KEY, null);
        if (clusterName == null) {
            clusterName = getClusterName(stormConf);
        }
        return clusterName;
    }


    private ArrayList<Referenceable> createTopologyGraph(StormTopology stormTopology,
                                                         Map<String, SpoutSpec> spouts,
                                                         Map<String, Bolt> bolts) throws Exception {
        // Add graph of nodes in the topology
        final Map<String, Referenceable> nodeEntities = new HashMap<>();
        addSpouts(spouts, nodeEntities);
        addBolts(bolts, nodeEntities);

        addGraphConnections(stormTopology, nodeEntities);

        ArrayList<Referenceable> nodes = new ArrayList<>();
        nodes.addAll(nodeEntities.values());
        return nodes;
    }

    private void addSpouts(Map<String, SpoutSpec> spouts,
                           Map<String, Referenceable> nodeEntities) throws IllegalAccessException {
        for (Map.Entry<String, SpoutSpec> entry : spouts.entrySet()) {
            final String spoutName = entry.getKey();
            Referenceable spoutReferenceable = createSpoutInstance(
                    spoutName, entry.getValue());
            nodeEntities.put(spoutName, spoutReferenceable);
        }
    }

    private Referenceable createSpoutInstance(String spoutName,
                                              SpoutSpec stormSpout) throws IllegalAccessException {
        Referenceable spoutReferenceable = new Referenceable(
                StormDataTypes.STORM_SPOUT.getName(), "DataProducer");
        spoutReferenceable.set("name", spoutName);

        Serializable instance = Utils.javaDeserialize(
                stormSpout.get_spout_object().get_serialized_java(), Serializable.class);
        spoutReferenceable.set("driverClass", instance.getClass().getName());

        Map<String, String> flatConfigMap = StormTopologyUtil.getFieldValues(instance, true);
        spoutReferenceable.set("conf", flatConfigMap);

        return spoutReferenceable;
    }

    private void addBolts(Map<String, Bolt> bolts,
                          Map<String, Referenceable> nodeEntities) throws IllegalAccessException {
        for (Map.Entry<String, Bolt> entry : bolts.entrySet()) {
            Referenceable boltInstance = createBoltInstance(entry.getKey(), entry.getValue());
            nodeEntities.put(entry.getKey(), boltInstance);
        }
    }

    private Referenceable createBoltInstance(String boltName,
                                             Bolt stormBolt) throws IllegalAccessException {
        Referenceable boltReferenceable = new Referenceable(
                StormDataTypes.STORM_BOLT.getName(), "DataProcessor");

        boltReferenceable.set("name", boltName);

        Serializable instance = Utils.javaDeserialize(
                stormBolt.get_bolt_object().get_serialized_java(), Serializable.class);
        boltReferenceable.set("driverClass", instance.getClass().getName());

        Map<String, String> flatConfigMap = StormTopologyUtil.getFieldValues(instance, true);
        boltReferenceable.set("conf", flatConfigMap);

        return boltReferenceable;
    }

    private void addGraphConnections(StormTopology stormTopology,
                                     Map<String, Referenceable> nodeEntities) throws Exception {
        // adds connections between spouts and bolts
        Map<String, Set<String>> adjacencyMap =
                StormTopologyUtil.getAdjacencyMap(stormTopology, true);

        for (Map.Entry<String, Set<String>> entry : adjacencyMap.entrySet()) {
            String nodeName = entry.getKey();
            Set<String> adjacencyList = adjacencyMap.get(nodeName);
            if (adjacencyList == null || adjacencyList.isEmpty()) {
                continue;
            }

            // add outgoing links
            Referenceable node = nodeEntities.get(nodeName);
            ArrayList<String> outputs = new ArrayList<>(adjacencyList.size());
            outputs.addAll(adjacencyList);
            node.set("outputs", outputs);

            // add incoming links
            for (String adjacentNodeName : adjacencyList) {
                Referenceable adjacentNode = nodeEntities.get(adjacentNodeName);
                @SuppressWarnings("unchecked")
                ArrayList<String> inputs = (ArrayList<String>) adjacentNode.get("inputs");
                if (inputs == null) {
                    inputs = new ArrayList<>();
                }
                inputs.add(nodeName);
                adjacentNode.set("inputs", inputs);
            }
        }
    }

    public static String getKafkaTopicQualifiedName(String clusterName, String topicName) {
        return String.format("%s@%s", topicName, clusterName);
    }

    public static String getHbaseTableQualifiedName(String clusterName, String nameSpace, String tableName) {
        return String.format("%s.%s@%s", nameSpace, tableName, clusterName);
    }

    private String getClusterName(Map stormConf) {
        String clusterName = AtlasConstants.DEFAULT_CLUSTER_NAME;
        if (stormConf.containsKey(AtlasConstants.CLUSTER_NAME_KEY)) {
            clusterName = (String)stormConf.get(AtlasConstants.CLUSTER_NAME_KEY);
        }
        return clusterName;
    }
}
