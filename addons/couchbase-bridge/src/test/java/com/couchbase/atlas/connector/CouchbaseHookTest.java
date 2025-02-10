/*
 * Copyright 2023 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.atlas.connector;

import com.couchbase.atlas.connector.entities.CouchbaseAtlasEntity;
import com.couchbase.atlas.connector.entities.CouchbaseBucket;
import com.couchbase.atlas.connector.entities.CouchbaseCluster;
import com.couchbase.atlas.connector.entities.CouchbaseScope;
import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.StreamFrom;
import com.couchbase.client.dcp.StreamTo;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.model.instance.AtlasEntity;
import org.mockito.Mockito;
import org.testng.annotations.Test;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class CouchbaseHookTest {
    @Test
    public void testMain() throws Exception {
        Client mockDcpClient = mockDcpClient();
        CBConfig.dcpClient(mockDcpClient);
        AtlasClientV2 mockAtlasClient = mockAtlasClient(false);
        AtlasConfig.client(mockAtlasClient);

        AtomicInteger createCalled = new AtomicInteger();
        Consumer<List<AtlasEntity>> createEntitiesInterceptor = ents -> {
            createCalled.getAndIncrement();
            assertEquals(ents.size(), 2);
        };
        Consumer<List<AtlasEntity>> updateEntitiesInterceptor = ents -> fail();

        CouchbaseHook.setEntityInterceptors(createEntitiesInterceptor, updateEntitiesInterceptor);
        CouchbaseHook.loop(false);
        // AAAAAND, ACTION (missing entities)
        CouchbaseHook.main(new String[0]);

        Mockito.verify(mockDcpClient, Mockito.times(1)).connect();
        assertEquals(createCalled.get(), 1);
        // 2 times: 1 time when we call exists(ATLAS) and second time when we request the entity
        validateAtlasInvocations(mockAtlasClient, 3, 2, 0);

        // simulate existing entities situation
        mockAtlasClient = mockAtlasClient(true);
        AtlasConfig.client(mockAtlasClient);
        CouchbaseAtlasEntity.dropCache();

        // ACTION AGAIN, this time with mock entities in mock Atlas
        CouchbaseHook.main(new String[0]);

        Mockito.verify(mockDcpClient, Mockito.times(2)).connect();
        assertEquals(createCalled.get(), 1);
        // 1 time and then it should be cached
        validateAtlasInvocations(mockAtlasClient, 1, 1, 0);

        testEvents(CouchbaseHook.instance);
    }

    public void testEvents(CouchbaseHook listener) {
    }

    private Client mockDcpClient() {
        Client mockDcpClient = Mockito.mock(Client.class);
        Mockito.when(mockDcpClient.connect()).thenReturn(Mono.empty());
        Mockito.when(mockDcpClient.initializeState(StreamFrom.NOW, StreamTo.INFINITY)).thenReturn(Mono.empty());
        Mockito.when(mockDcpClient.startStreaming()).thenReturn(Mono.empty());
        Mockito.when(mockDcpClient.disconnect()).thenReturn(Mono.empty());
        return mockDcpClient;
    }

    private AtlasClientV2 mockAtlasClient(boolean returnEntities) throws Exception {
        AtlasClientV2 mockAtlasClient = Mockito.mock(AtlasClientV2.class);
        final String  clusterName     = "couchbase://localhost";
        final String  bucketName      = String.format("%s/%s", clusterName, "default");
        final String  scopeName       = String.format("%s/%s", bucketName, "_default");

        Mockito.when(mockAtlasClient.getEntityByAttribute(Mockito.eq(CouchbaseCluster.TYPE_NAME), Mockito.anyMap())).thenAnswer(iom -> {
            Map<String, String> query = iom.getArgument(1);
            assertEquals(query.get("qualifiedName"), clusterName);
            return new AtlasEntity.AtlasEntityWithExtInfo(returnEntities ? Mockito.mock(AtlasEntity.class) : null);
        });

        Mockito.when(mockAtlasClient.getEntityByAttribute(Mockito.eq(CouchbaseBucket.TYPE_NAME), Mockito.anyMap())).thenAnswer(iom -> {
            Map<String, String> query = iom.getArgument(1);
            assertEquals(bucketName, query.get("qualifiedName"));
            return new AtlasEntity.AtlasEntityWithExtInfo(returnEntities ? Mockito.mock(AtlasEntity.class) : null);
        });

        Mockito.when(mockAtlasClient.getEntityByAttribute(Mockito.eq(CouchbaseScope.TYPE_NAME), Mockito.anyMap())).thenAnswer(iom -> {
            Map<String, String> query = iom.getArgument(1);
            assertEquals(scopeName, query.get("qualifiedName"));
            return new AtlasEntity.AtlasEntityWithExtInfo(returnEntities ? Mockito.mock(AtlasEntity.class) : null);
        });

        return mockAtlasClient;
    }

    private void validateAtlasInvocations(AtlasClientV2 mockAtlasClient, int cluster, int bucket, int scope) throws Exception {
        Mockito.verify(mockAtlasClient, Mockito.times(cluster)).getEntityByAttribute(Mockito.eq(CouchbaseCluster.TYPE_NAME), Mockito.anyMap());
        Mockito.verify(mockAtlasClient, Mockito.times(bucket)).getEntityByAttribute(Mockito.eq(CouchbaseBucket.TYPE_NAME), Mockito.anyMap());
        Mockito.verify(mockAtlasClient, Mockito.times(scope)).getEntityByAttribute(Mockito.eq(CouchbaseScope.TYPE_NAME), Mockito.anyMap());
    }
}
