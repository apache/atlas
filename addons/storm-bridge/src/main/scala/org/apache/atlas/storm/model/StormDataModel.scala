/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.storm.model

import org.apache.atlas.AtlasClient
import org.apache.atlas.typesystem.TypesDef
import org.apache.atlas.typesystem.builders.TypesBuilder
import org.apache.atlas.typesystem.json.TypesSerialization


/**
 * This represents the data model for a storm topology.
 */
object StormDataModel extends App {

    var typesDef : TypesDef = null

    val typesBuilder = new TypesBuilder
    import typesBuilder._

    typesDef = types {

        /**
         * Model is represented as:
         * Topology is a Process Super Type inheriting inputs/outputs
         * Input DataSet(s) => Topology => Output DataSet(s)
         * Also, Topology contains the Graph of Nodes
         * Topology => Node(s) -> Spouts/Bolts
         */
        _class(StormDataTypes.STORM_TOPOLOGY.getName, List(AtlasClient.PROCESS_SUPER_TYPE)) {
            "id" ~ (string, required, indexed, unique)
            "startTime" ~ date
            "endTime" ~ date
            "conf" ~ (map(string, string), optional)
            "clusterName" ~ (string, optional, indexed)

            // Nodes in the Graph
            "nodes" ~ (array(StormDataTypes.STORM_NODE.getName), collection, composite)
        }

        // Base class for DataProducer aka Spouts and
        // DataProcessor aka Bolts, also links from Topology
        _class(StormDataTypes.STORM_NODE.getName) {
            "name" ~ (string, required, indexed)
            "description" ~ (string, optional, indexed)
            // fully qualified driver java class name
            "driverClass" ~ (string, required, indexed)
            // spout or bolt configuration NVPs
            "conf" ~ (map(string, string), optional)
        }

        // Data Producer and hence only outputs
        _class(StormDataTypes.STORM_SPOUT.getName, List(StormDataTypes.STORM_NODE.getName)) {
            // "outputs" ~ (array(StormDataTypes.STORM_NODE.getName), collection, composite)
            "outputs" ~ (array(string), collection)
        }

        // Data Processor and hence both inputs and outputs (inherited from Spout)
        _class(StormDataTypes.STORM_BOLT.getName, List(StormDataTypes.STORM_NODE.getName)) {
            // "inputs" ~ (array(StormDataTypes.STORM_NODE.getName), collection, composite)
            "inputs" ~ (array(string), collection)
            "outputs" ~ (array(string), collection, optional)
        }

        // Kafka Data Set
        _class(StormDataTypes.KAFKA_TOPIC.getName, List(AtlasClient.DATA_SET_SUPER_TYPE)) {
            "topic" ~ (string, required, unique, indexed)
            "uri" ~ (string, required)
        }

        // JMS Data Set
        _class(StormDataTypes.JMS_TOPIC.getName, List(AtlasClient.DATA_SET_SUPER_TYPE)) {
            "topic" ~ (string, required, unique, indexed)
            "uri" ~ (string, required)
        }

        // HBase Data Set
        _class(StormDataTypes.HBASE_TABLE.getName, List(AtlasClient.DATA_SET_SUPER_TYPE)) {
            "uri" ~ (string, required)
        }
        // Hive table data set already exists in atlas.
    }

    // add the types to atlas
    val typesAsJSON = TypesSerialization.toJson(typesDef)
    println("Storm Data Model as JSON: ")
    println(typesAsJSON)
}
