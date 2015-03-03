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

package org.apache.metadata.falcon;

import com.google.inject.Inject;
import org.apache.falcon.client.FalconCLIException;
import org.apache.falcon.client.FalconClient;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.Interface;
import org.apache.falcon.entity.v0.cluster.Location;
import org.apache.falcon.entity.v0.cluster.Properties;
import org.apache.falcon.entity.v0.cluster.Property;
import org.apache.falcon.resource.EntityList;
import org.apache.hadoop.metadata.ITypedInstance;
import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.Referenceable;
import org.apache.hadoop.metadata.Struct;
import org.apache.hadoop.metadata.repository.MetadataRepository;
import org.apache.hadoop.metadata.types.Multiplicity;
import org.apache.hadoop.metadata.types.StructType;
import org.parboiled.common.StringUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FalconImporter {

    private final FalconTypeSystem typeSystem;
    private final FalconClient client;
    private final MetadataRepository repository;

    @Inject
    public FalconImporter(FalconTypeSystem typeSystem, FalconClient client, MetadataRepository repo) {
        this.typeSystem = typeSystem;
        this.client = client;
        this.repository = repo;
    }

    public void importClusters() throws FalconCLIException, MetadataException {
        EntityList clusters = client.getEntityList(EntityType.CLUSTER.name(), null, null, null, null, null, null, null, null);
        for (EntityList.EntityElement element : clusters.getElements()) {
            Cluster cluster = (Cluster) client.getDefinition(EntityType.CLUSTER.name(), element.name);

            Referenceable clusterRef = new Referenceable(FalconTypeSystem.DefinedTypes.CLUSTER.name());
            clusterRef.set("name", cluster.getName());

            if (cluster.getACL() != null) {
                Struct acl = new Struct(FalconTypeSystem.DefinedTypes.ACL.name());
                acl.set("owner", cluster.getACL().getOwner());
                acl.set("group", cluster.getACL().getGroup());
                acl.set("permission", cluster.getACL().getPermission());
                StructType aclType = (StructType) typeSystem.getDataType(FalconTypeSystem.DefinedTypes.ACL.name());
                clusterRef.set("acl", aclType.convert(acl, Multiplicity.REQUIRED));
            }

            if (StringUtils.isNotEmpty(cluster.getTags())) {
                clusterRef.set("tags", getMap(cluster.getTags()));
            }

            if (cluster.getProperties() != null) {
                clusterRef.set("properties", getMap(cluster.getProperties()));
            }

            if (cluster.getLocations() != null) {
                List<ITypedInstance> locations = new ArrayList<>();
                for (Location loc : cluster.getLocations().getLocations()) {
                    Struct location = new Struct(FalconTypeSystem.DefinedTypes.CLUSTER_LOCATION.name());
                    location.set("type", loc.getName());
                    location.set("path", loc.getPath());
                    StructType type = (StructType) typeSystem.getDataType(FalconTypeSystem.DefinedTypes.CLUSTER_LOCATION.name());
                    locations.add(type.convert(location, Multiplicity.REQUIRED));
                }
                clusterRef.set("locations", locations);
            }

            if (cluster.getInterfaces() != null) {
                List<ITypedInstance> interfaces = new ArrayList<>();
                for (Interface interfaceFld : cluster.getInterfaces().getInterfaces()) {
                    Struct interfaceStruct = new Struct(FalconTypeSystem.DefinedTypes.CLUSTER_INTERFACE.name());
                    interfaceStruct.set("type", interfaceFld.getType().name());
                    interfaceStruct.set("endpoint", interfaceFld.getEndpoint());
                    interfaceStruct.set("version", interfaceFld.getVersion());
                    StructType type = (StructType) typeSystem.getDataType(FalconTypeSystem.DefinedTypes.CLUSTER_INTERFACE.name());
                    interfaces.add(type.convert(interfaceStruct, Multiplicity.REQUIRED));
                }
                clusterRef.set("interfaces", interfaces);
            }
            repository.createEntity(clusterRef, clusterRef.getTypeName());
        }
    }

    private Map<String, String> getMap(Properties properties) {
        Map<String, String> map = new HashMap();
        for (Property property : properties.getProperties()) {
            map.put(property.getName().trim(), property.getValue().trim());
        }
        return map;
    }

    private Map<String, String> getMap(String tags) {
        Map<String, String> map = new HashMap();
        String[] parts = tags.split(",");
        for (String part : parts) {
            String[] kv = part.trim().split("=");
            map.put(kv[0].trim(), kv[1].trim());
        }
        return map;
    }
}
