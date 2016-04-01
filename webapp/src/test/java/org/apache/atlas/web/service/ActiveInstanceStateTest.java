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

package org.apache.atlas.web.service;

import org.apache.atlas.ha.HAConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CreateBuilder;
import org.apache.curator.framework.api.ExistsBuilder;
import org.apache.curator.framework.api.GetDataBuilder;
import org.apache.curator.framework.api.SetDataBuilder;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.nio.charset.Charset;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;
import static org.testng.Assert.assertNull;

public class ActiveInstanceStateTest {

    private static final String HOST_PORT = "127.0.0.1:21000";
    public static final String SERVER_ADDRESS = "http://" + HOST_PORT;
    @Mock
    private Configuration configuration;

    @Mock
    private CuratorFactory curatorFactory;

    @Mock
    private CuratorFramework curatorFramework;

    @BeforeTest
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testSharedPathIsCreatedIfNotExists() throws Exception {

        when(configuration.getString(HAConfiguration.ATLAS_SERVER_ADDRESS_PREFIX +"id1")).thenReturn(HOST_PORT);

        when(curatorFactory.clientInstance()).thenReturn(curatorFramework);

        ExistsBuilder existsBuilder = mock(ExistsBuilder.class);
        when(curatorFramework.checkExists()).thenReturn(existsBuilder);
        when(existsBuilder.forPath(ActiveInstanceState.APACHE_ATLAS_ACTIVE_SERVER_INFO)).thenReturn(null);

        CreateBuilder createBuilder = mock(CreateBuilder.class);
        when(curatorFramework.create()).thenReturn(createBuilder);
        when(createBuilder.withMode(CreateMode.EPHEMERAL)).thenReturn(createBuilder);

        SetDataBuilder setDataBuilder = mock(SetDataBuilder.class);
        when(curatorFramework.setData()).thenReturn(setDataBuilder);

        ActiveInstanceState activeInstanceState = new ActiveInstanceState(configuration, curatorFactory);
        activeInstanceState.update("id1");

        verify(createBuilder).forPath(ActiveInstanceState.APACHE_ATLAS_ACTIVE_SERVER_INFO);
    }

    @Test
    public void testDataIsUpdatedWithAtlasServerAddress() throws Exception {
        when(configuration.getString(HAConfiguration.ATLAS_SERVER_ADDRESS_PREFIX +"id1")).thenReturn(HOST_PORT);

        when(curatorFactory.clientInstance()).thenReturn(curatorFramework);
        ExistsBuilder existsBuilder = mock(ExistsBuilder.class);
        when(curatorFramework.checkExists()).thenReturn(existsBuilder);
        when(existsBuilder.forPath(ActiveInstanceState.APACHE_ATLAS_ACTIVE_SERVER_INFO)).thenReturn(new Stat());

        SetDataBuilder setDataBuilder = mock(SetDataBuilder.class);
        when(curatorFramework.setData()).thenReturn(setDataBuilder);

        ActiveInstanceState activeInstanceState = new ActiveInstanceState(configuration, curatorFactory);
        activeInstanceState.update("id1");

        verify(setDataBuilder).forPath(
                ActiveInstanceState.APACHE_ATLAS_ACTIVE_SERVER_INFO,
                SERVER_ADDRESS.getBytes(Charset.forName("UTF-8")));
    }

    @Test
    public void testShouldReturnActiveServerAddress() throws Exception {
        when(curatorFactory.clientInstance()).thenReturn(curatorFramework);

        GetDataBuilder getDataBuilder = mock(GetDataBuilder.class);
        when(curatorFramework.getData()).thenReturn(getDataBuilder);
        when(getDataBuilder.forPath(ActiveInstanceState.APACHE_ATLAS_ACTIVE_SERVER_INFO)).
                thenReturn(SERVER_ADDRESS.getBytes(Charset.forName("UTF-8")));

        ActiveInstanceState activeInstanceState = new ActiveInstanceState(configuration, curatorFactory);
        String actualServerAddress = activeInstanceState.getActiveServerAddress();

        assertEquals(SERVER_ADDRESS, actualServerAddress);
    }

    @Test
    public void testShouldHandleExceptionsInFetchingServerAddress() throws Exception {
        when(curatorFactory.clientInstance()).thenReturn(curatorFramework);

        GetDataBuilder getDataBuilder = mock(GetDataBuilder.class);
        when(curatorFramework.getData()).thenReturn(getDataBuilder);
        when(getDataBuilder.forPath(ActiveInstanceState.APACHE_ATLAS_ACTIVE_SERVER_INFO)).
                thenThrow(new Exception());

        ActiveInstanceState activeInstanceState = new ActiveInstanceState(configuration, curatorFactory);
        assertNull(activeInstanceState.getActiveServerAddress());
    }
}
