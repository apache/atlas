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

package org.apache.metadata.falcon;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.falcon.client.FalconCLIException;
import org.apache.falcon.client.FalconClient;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.Interface;
import org.apache.falcon.entity.v0.cluster.Interfaces;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.entity.v0.cluster.Location;
import org.apache.falcon.entity.v0.cluster.Locations;
import org.apache.falcon.resource.EntityList;
import org.apache.hadoop.metadata.IReferenceableInstance;
import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.repository.MetadataRepository;
import org.testng.annotations.Test;

import java.io.StringWriter;
import java.util.UUID;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FalconImporterTest {
    @Test
    public void testImport() throws Exception {
        MetadataRepository repo = mock(MetadataRepository.class);
        FalconClient client = mock(FalconClient.class);
        FalconTypeSystem.getInstance();

        FalconImporter importer = new FalconImporter(client, repo);
        when(client.getEntityList(EntityType.CLUSTER.name(), null, null, null, null, null, null, null)).thenReturn(getEntityList());
        //TODO Set other fields in cluster
        when(client.getDefinition(anyString(), anyString())).thenReturn(getCluster());
        when(repo.createEntity(any(IReferenceableInstance.class), anyString())).thenReturn(UUID.randomUUID().toString());

        importer.importClusters();
    }

    public EntityList getEntityList() {
        EntityList.EntityElement[] entities = new EntityList.EntityElement[2];
        entities[0] = new EntityList.EntityElement();
        entities[0].name = "c1";
        entities[1] = new EntityList.EntityElement();
        entities[1].name = "c2";
        return new EntityList(entities);
    }

    private Interface getInterface(Interfacetype type, String endpoint) {
        Interface clusterInterface = new Interface();
        clusterInterface.setEndpoint(endpoint);
        clusterInterface.setType(type);
        clusterInterface.setVersion("2.2");
        return clusterInterface;
    }

    public String getCluster() throws Exception {
        Cluster cluster = new Cluster();
        cluster.setName(RandomStringUtils.randomAlphabetic(10));
        cluster.setColo(RandomStringUtils.randomAlphabetic(5));

        cluster.setTags("owner=xyz,team=abc");
        Interfaces interfaces = new Interfaces();
        Interface clusterInterface = new Interface();
        clusterInterface.setEndpoint("hdfs://localhost:8030");
        clusterInterface.setType(Interfacetype.WRITE);
        clusterInterface.setVersion("2.2");
        interfaces.getInterfaces().add(getInterface(Interfacetype.WRITE, "hdfs://localhost:8030"));
        interfaces.getInterfaces().add(getInterface(Interfacetype.READONLY, "hdfs://localhost:8030"));
        interfaces.getInterfaces().add(getInterface(Interfacetype.EXECUTE, "http://localhost:8040"));
        cluster.setInterfaces(interfaces);

        Locations locations = new Locations();
        locations.getLocations().add(getLocation());
        cluster.setLocations(locations);

        StringWriter writer = new StringWriter();
        EntityType.CLUSTER.getMarshaller().marshal(cluster, writer);
        return writer.toString();
    }

    public Location getLocation() {
        Location location = new Location();
        location.setName("staging");
        location.setPath("/staging");
        return location;
    }
}
