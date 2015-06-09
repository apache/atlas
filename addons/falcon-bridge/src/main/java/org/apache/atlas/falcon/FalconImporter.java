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

package org.apache.atlas.falcon;

import com.google.inject.Inject;
import org.apache.atlas.MetadataException;
import org.apache.atlas.repository.MetadataRepository;
import org.apache.atlas.typesystem.ITypedInstance;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
import org.apache.atlas.typesystem.types.EnumType;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.StructType;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.commons.lang.StringUtils;
import org.apache.falcon.client.FalconCLIException;
import org.apache.falcon.client.FalconClient;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.Interface;
import org.apache.falcon.entity.v0.cluster.Location;
import org.apache.falcon.entity.v0.cluster.Properties;
import org.apache.falcon.entity.v0.cluster.Property;
import org.apache.falcon.resource.EntityList;

import javax.xml.bind.JAXBException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FalconImporter {

    private static final TypeSystem typeSystem = TypeSystem.getInstance();

    private final FalconClient client;
    private final MetadataRepository repository;

    @Inject
    public FalconImporter(FalconClient client, MetadataRepository repo) {
        this.client = client;
        this.repository = repo;
    }

    private Entity getEntity(FalconClient client, EntityType type, String name) throws FalconCLIException, JAXBException {
        String entityStr = client.getDefinition(type.name(), name);
        return (Entity) type.getUnmarshaller().unmarshal(new StringReader(entityStr));
    }

    public void importClusters() throws MetadataException {
        try {
            EntityList clusters = client.getEntityList(EntityType.CLUSTER.name(), null, null, null, null, null, null, null);
            for (EntityList.EntityElement element : clusters.getElements()) {
                Cluster cluster = (Cluster) getEntity(client, EntityType.CLUSTER, element.name);

                Referenceable clusterRef = new Referenceable(FalconTypeSystem.DefinedTypes.CLUSTER.name());
                clusterRef.set("name", cluster.getName());

                if (cluster.getACL() != null) {
                    Struct acl = new Struct(FalconTypeSystem.DefinedTypes.ACL.name());
                    acl.set("owner", cluster.getACL().getOwner());
                    acl.set("group", cluster.getACL().getGroup());
                    acl.set("permission", cluster.getACL().getPermission());
                    StructType aclType = typeSystem.getDataType(StructType.class, FalconTypeSystem.DefinedTypes.ACL.name());
                    clusterRef.set("acl", aclType.convert(acl, Multiplicity.REQUIRED));
                }

                if (StringUtils.isNotEmpty(cluster.getTags())) {
                    String[] parts = cluster.getTags().split(",");
                    List<ITypedInstance> tags = new ArrayList<>();
                    for (String part : parts) {
                        TraitType tagType = typeSystem.getDataType(TraitType.class, FalconTypeSystem.DefinedTypes.TAG.name());
                        String[] kv = part.trim().split("=");
                        Struct tag = new Struct(FalconTypeSystem.DefinedTypes.TAG.name());
                        tag.set("name", kv[0]);
                        tag.set("value", kv[0]);
                        tags.add(tagType.convert(tag, Multiplicity.REQUIRED));
                    }
                    clusterRef.set("tags", tags);
                }

                if (cluster.getProperties() != null) {
                    clusterRef.set("properties", getMap(cluster.getProperties()));
                }

                if (cluster.getLocations() != null) {
                    List<ITypedInstance> locations = new ArrayList<>();
                    for (Location loc : cluster.getLocations().getLocations()) {
                        Struct location = new Struct(FalconTypeSystem.DefinedTypes.CLUSTER_LOCATION.name());
                        EnumType locationType = typeSystem.getDataType(EnumType.class, FalconTypeSystem.DefinedTypes.CLUSTER_LOCATION_TYPE.name());
                        location.set("type", locationType.fromValue(loc.getName().toUpperCase()));
                        location.set("path", loc.getPath());
                        StructType type = typeSystem.getDataType(StructType.class, FalconTypeSystem.DefinedTypes.CLUSTER_LOCATION.name());
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
                        StructType type = typeSystem.getDataType(StructType.class, FalconTypeSystem.DefinedTypes.CLUSTER_INTERFACE.name());
                        interfaces.add(type.convert(interfaceStruct, Multiplicity.REQUIRED));
                    }
                    clusterRef.set("interfaces", interfaces);
                }
                repository.createEntity(clusterRef);
            }
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    private Map<String, String> getMap(Properties properties) {
        Map<String, String> map = new HashMap();
        for (Property property : properties.getProperties()) {
            map.put(property.getName().trim(), property.getValue().trim());
        }
        return map;
    }
}
