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

import org.apache.falcon.client.FalconCLIException;
import org.apache.falcon.client.FalconClient;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.resource.EntityList;
import org.apache.hadoop.metadata.IReferenceableInstance;
import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.repository.MetadataRepository;
import org.testng.annotations.Test;

import java.util.UUID;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FalconImporterTest {
    @Test
    public void testImport() throws MetadataException, FalconCLIException {
        MetadataRepository repo = mock(MetadataRepository.class);
        FalconClient client = mock(FalconClient.class);

        FalconImporter importer = new FalconImporter(FalconTypeSystem.getInstance(), client, repo);
        when(client.getEntityList(EntityType.CLUSTER.name(), null, null, null, null, null, null, null, null)).thenReturn(getEntityList());
        Cluster cluster = new Cluster();
        //TODO Set other fields in cluster
        when(client.getDefinition(anyString(), anyString())).thenReturn(cluster);
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
}
