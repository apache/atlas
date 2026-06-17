/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.web.service;

import org.apache.atlas.AtlasException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.ha.HAConfiguration;
import org.apache.atlas.listener.ActiveStateChangeHandler;
import org.apache.atlas.server.common.service.ActiveInstanceElectorService;
import org.apache.atlas.server.common.service.ActiveInstanceState;
import org.apache.atlas.server.common.service.CuratorFactory;
import org.apache.atlas.server.common.service.HighAvailability;
import org.apache.atlas.server.common.service.HighAvailabilityProperties;
import org.apache.atlas.server.common.service.ServiceState;
import org.apache.atlas.server.common.service.ServiceStateChangeHandler;
import org.apache.commons.configuration.Configuration;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class ActiveInstanceElectorServiceTest {
    private static final String DEFAULT_ZK_ROOT = HAConfiguration.ATLAS_SERVER_ZK_ROOT_DEFAULT;

    @Mock
    private Configuration configuration;

    @Mock
    private CuratorFactory curatorFactory;

    @Mock
    private ActiveInstanceState activeInstanceState;

    @Mock
    private ServiceState serviceState;

    @Mock
    private HighAvailability highAvailability;

    @Mock
    private HighAvailabilityProperties haProperties;

    @Mock
    private ServiceStateChangeHandler serviceStateChangeHandler;

    @BeforeMethod
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        when(highAvailability.isHAEnabled(any(Configuration.class))).thenReturn(true);
        when(highAvailability.selectServerId(any(Configuration.class))).thenReturn("id1");
        when(highAvailability.getZookeeperProperties(any(Configuration.class))).thenReturn(haProperties);
        when(haProperties.getZkRoot()).thenReturn(DEFAULT_ZK_ROOT);
    }

    private ActiveInstanceElectorService newElector(Set<ActiveStateChangeHandler> handlers) {
        return new ActiveInstanceElectorService(
                configuration,
                handlers,
                Collections.singleton(serviceStateChangeHandler),
                curatorFactory,
                activeInstanceState,
                serviceState,
                highAvailability);
    }

    @Test
    public void testLeaderElectionIsJoinedOnStart() throws Exception {
        LeaderLatch leaderLatch = mock(LeaderLatch.class);
        when(curatorFactory.leaderLatchInstance("id1", DEFAULT_ZK_ROOT)).thenReturn(leaderLatch);

        ActiveInstanceElectorService service = newElector(new HashSet<ActiveStateChangeHandler>());
        service.start();

        verify(leaderLatch).start();
    }

    @Test
    public void testListenerIsAddedForActiveInstanceCallbacks() throws Exception {
        LeaderLatch leaderLatch = mock(LeaderLatch.class);
        when(curatorFactory.leaderLatchInstance("id1", DEFAULT_ZK_ROOT)).thenReturn(leaderLatch);

        ActiveInstanceElectorService service = newElector(new HashSet<ActiveStateChangeHandler>());
        service.start();

        verify(leaderLatch).addListener(service);
    }

    @Test
    public void testLeaderElectionIsNotStartedIfNotInHAMode() throws AtlasException {
        reset(highAvailability);
        when(highAvailability.isHAEnabled(any(Configuration.class))).thenReturn(false);
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY, false)).thenReturn(false);

        ActiveInstanceElectorService service = newElector(new HashSet<ActiveStateChangeHandler>());
        service.start();

        verify(serviceStateChangeHandler).onServerStart();
        verify(serviceStateChangeHandler).onServerActivation();
        verifyZeroInteractions(curatorFactory);
    }

    @Test
    public void testLeaderElectionIsLeftOnStop() throws IOException, AtlasException {
        LeaderLatch leaderLatch = mock(LeaderLatch.class);
        when(curatorFactory.leaderLatchInstance("id1", DEFAULT_ZK_ROOT)).thenReturn(leaderLatch);

        ActiveInstanceElectorService service = newElector(new HashSet<ActiveStateChangeHandler>());
        service.start();
        service.stop();

        verify(leaderLatch).close();
    }

    @Test
    public void testCuratorFactoryIsClosedOnStop() throws AtlasException {
        LeaderLatch leaderLatch = mock(LeaderLatch.class);
        when(curatorFactory.leaderLatchInstance("id1", DEFAULT_ZK_ROOT)).thenReturn(leaderLatch);

        ActiveInstanceElectorService service = newElector(new HashSet<ActiveStateChangeHandler>());
        service.start();
        service.stop();

        verify(curatorFactory).close();
    }

    @Test
    public void testNoActionOnStopIfHAModeIsDisabled() {
        reset(highAvailability);
        when(highAvailability.isHAEnabled(any(Configuration.class))).thenReturn(false);
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY, false)).thenReturn(false);

        ActiveInstanceElectorService service = newElector(new HashSet<ActiveStateChangeHandler>());
        service.stop();

        verifyZeroInteractions(curatorFactory);
    }

    @Test
    public void testRegisteredHandlersAreNotifiedWhenInstanceIsActive() throws AtlasException {
        LeaderLatch leaderLatch = mock(LeaderLatch.class);
        when(curatorFactory.leaderLatchInstance("id1", DEFAULT_ZK_ROOT)).thenReturn(leaderLatch);

        Set<ActiveStateChangeHandler> changeHandlers  = new HashSet<ActiveStateChangeHandler>();
        ActiveStateChangeHandler        handler1      = mock(ActiveStateChangeHandler.class);
        ActiveStateChangeHandler        handler2      = mock(ActiveStateChangeHandler.class);
        changeHandlers.add(handler1);
        changeHandlers.add(handler2);

        ActiveInstanceElectorService service = newElector(changeHandlers);
        service.start();
        service.isLeader();

        verify(handler1).instanceIsActive();
        verify(handler2).instanceIsActive();
    }

    @Test
    public void testSharedStateIsUpdatedWhenInstanceIsActive() throws Exception {
        LeaderLatch leaderLatch = mock(LeaderLatch.class);
        when(curatorFactory.leaderLatchInstance("id1", DEFAULT_ZK_ROOT)).thenReturn(leaderLatch);

        ActiveInstanceElectorService service = newElector(new HashSet<ActiveStateChangeHandler>());
        service.start();
        service.isLeader();

        verify(activeInstanceState).update("id1");
    }

    @Test
    public void testRegisteredHandlersAreNotifiedOfPassiveWhenStateUpdateFails() throws Exception {
        LeaderLatch leaderLatch = mock(LeaderLatch.class);
        when(curatorFactory.leaderLatchInstance("id1", DEFAULT_ZK_ROOT)).thenReturn(leaderLatch);

        Set<ActiveStateChangeHandler> changeHandlers  = new HashSet<ActiveStateChangeHandler>();
        ActiveStateChangeHandler        handler1      = mock(ActiveStateChangeHandler.class);
        ActiveStateChangeHandler        handler2      = mock(ActiveStateChangeHandler.class);
        changeHandlers.add(handler1);
        changeHandlers.add(handler2);
        doThrow(new AtlasBaseException()).when(activeInstanceState).update("id1");

        ActiveInstanceElectorService service = newElector(changeHandlers);
        service.start();
        service.isLeader();

        verify(handler1).instanceIsPassive();
        verify(handler2).instanceIsPassive();
    }

    @Test
    public void testElectionIsRejoinedWhenStateUpdateFails() throws Exception {
        LeaderLatch leaderLatch = mock(LeaderLatch.class);
        when(curatorFactory.leaderLatchInstance("id1", DEFAULT_ZK_ROOT)).thenReturn(leaderLatch);
        doThrow(new AtlasBaseException()).when(activeInstanceState).update("id1");

        ActiveInstanceElectorService service = newElector(new HashSet<ActiveStateChangeHandler>());
        service.start();
        service.isLeader();

        InOrder inOrder = inOrder(leaderLatch, curatorFactory);
        inOrder.verify(leaderLatch).close();
        inOrder.verify(curatorFactory).leaderLatchInstance("id1", DEFAULT_ZK_ROOT);
        inOrder.verify(leaderLatch).addListener(service);
        inOrder.verify(leaderLatch).start();
    }

    @Test
    public void testRegisteredHandlersAreNotifiedOfPassiveWhenInstanceIsPassive() throws AtlasException {
        LeaderLatch leaderLatch = mock(LeaderLatch.class);
        when(curatorFactory.leaderLatchInstance("id1", DEFAULT_ZK_ROOT)).thenReturn(leaderLatch);

        Set<ActiveStateChangeHandler> changeHandlers  = new HashSet<ActiveStateChangeHandler>();
        ActiveStateChangeHandler        handler1      = mock(ActiveStateChangeHandler.class);
        ActiveStateChangeHandler        handler2      = mock(ActiveStateChangeHandler.class);
        changeHandlers.add(handler1);
        changeHandlers.add(handler2);

        ActiveInstanceElectorService service = newElector(changeHandlers);
        service.start();
        service.notLeader();

        verify(handler1).instanceIsPassive();
        verify(handler2).instanceIsPassive();
    }

    @Test
    public void testActiveStateSetOnBecomingLeader() {
        ActiveInstanceElectorService service = newElector(new HashSet<ActiveStateChangeHandler>());
        service.isLeader();

        InOrder inOrder = inOrder(serviceState);
        inOrder.verify(serviceState).becomingActive();
        inOrder.verify(serviceState).setActive();
    }

    @Test
    public void testPassiveStateSetOnLoosingLeadership() {
        ActiveInstanceElectorService service = newElector(new HashSet<ActiveStateChangeHandler>());
        service.notLeader();

        InOrder inOrder = inOrder(serviceState);
        inOrder.verify(serviceState).becomingPassive();
        inOrder.verify(serviceState).setPassive();
    }

    @Test
    public void testPassiveStateSetIfActivationFails() throws Exception {
        LeaderLatch leaderLatch = mock(LeaderLatch.class);
        when(curatorFactory.leaderLatchInstance("id1", DEFAULT_ZK_ROOT)).thenReturn(leaderLatch);
        doThrow(new AtlasBaseException()).when(activeInstanceState).update("id1");

        ActiveInstanceElectorService service = newElector(new HashSet<ActiveStateChangeHandler>());
        service.start();
        service.isLeader();

        InOrder inOrder = inOrder(serviceState);
        inOrder.verify(serviceState).becomingActive();
        inOrder.verify(serviceState).becomingPassive();
        inOrder.verify(serviceState).setPassive();
    }

    @Test
    public void testLeaderLatchUsesCustomZkRootFromHaProperties() throws Exception {
        String customRoot = "/atlas-custom-zk";
        when(haProperties.getZkRoot()).thenReturn(customRoot);
        LeaderLatch leaderLatch = mock(LeaderLatch.class);
        when(curatorFactory.leaderLatchInstance("id1", customRoot)).thenReturn(leaderLatch);

        ActiveInstanceElectorService service = newElector(new HashSet<ActiveStateChangeHandler>());
        service.start();

        verify(curatorFactory).leaderLatchInstance("id1", customRoot);
        verify(leaderLatch).start();
    }
}
