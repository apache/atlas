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

import com.google.inject.Singleton;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.ha.HAConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * A factory to create objects related to Curator.
 *
 * Allows for stubbing in tests.
 */
@Singleton
public class CuratorFactory {
    public static final String APACHE_ATLAS_LEADER_ELECTOR_PATH = "/apache_atlas_leader_elector_path";

    private final Configuration configuration;
    private CuratorFramework curatorFramework;

    /**
     * Initializes the {@link CuratorFramework} that is used for all interaction with Zookeeper.
     * @throws AtlasException
     */
    public CuratorFactory() throws AtlasException {
        configuration = ApplicationProperties.get();
        initializeCuratorFramework();
    }

    private void initializeCuratorFramework() {
        HAConfiguration.ZookeeperProperties zookeeperProperties =
                HAConfiguration.getZookeeperProperties(configuration);
        curatorFramework = CuratorFrameworkFactory.builder().
                connectString(zookeeperProperties.getConnectString()).
                sessionTimeoutMs(zookeeperProperties.getSessionTimeout()).
                retryPolicy(new ExponentialBackoffRetry(
                        zookeeperProperties.getRetriesSleepTimeMillis(), zookeeperProperties.getNumRetries())).build();
        curatorFramework.start();
    }

    /**
     * Cleanup resources related to {@link CuratorFramework}.
     *
     * After this call, no further calls to any curator objects should be done.
     */
    public void close() {
        curatorFramework.close();
    }

    /**
     * Returns a pre-created instance of {@link CuratorFramework}.
     *
     * This method can be called any number of times to access the {@link CuratorFramework} used in the
     * application.
     * @return
     */
    public CuratorFramework clientInstance() {
        return curatorFramework;
    }

    /**
     * Create a new instance {@link LeaderLatch}
     * @param serverId the ID used to register this instance with curator.
     *                 This ID should typically be obtained using
     *                 {@link org.apache.atlas.ha.AtlasServerIdSelector#selectServerId(Configuration)}
     * @return
     */
    public LeaderLatch leaderLatchInstance(String serverId) {
        return new LeaderLatch(curatorFramework, APACHE_ATLAS_LEADER_ELECTOR_PATH, serverId);
    }
}
