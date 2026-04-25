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
package org.apache.atlas.server.common.service;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.server.common.filters.spi.ActiveInstanceStateProvider;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * An object that encapsulates storing and retrieving state related to an Active Atlas server.
 *
 * The current implementation uses Zookeeper to store and read this state from. It does this
 * under a read-write lock implemented using Curator's {@link org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock} to
 * provide for safety across multiple processes.
 */
@Component
public class ActiveInstanceState implements ActiveInstanceStateProvider {
    private static final Logger LOG = LoggerFactory.getLogger(ActiveInstanceState.class);

    public static final String APACHE_ATLAS_ACTIVE_SERVER_INFO = "/active_server_info";

    private final Configuration           configuration;
    private final CuratorFactory            curatorFactory;
    private final HighAvailabilitySupport haSupport;

    @Inject
    public ActiveInstanceState(Configuration configuration, CuratorFactory curatorFactory, HighAvailabilitySupport haSupport) {
        this.configuration  = configuration;
        this.curatorFactory   = curatorFactory;
        this.haSupport        = haSupport;
    }

    public void update(String serverId) throws AtlasBaseException {
        try {
            CuratorFramework            client                 = curatorFactory.clientInstance();
            HighAvailabilityProperties zookeeperProperties    = haSupport.getZookeeperProperties(configuration);
            String                     atlasServerAddress     = haSupport.getBoundAddressForId(configuration, serverId);

            List<ACL> acls = new ArrayList<>();

            ACL parsedACL = AtlasZookeeperSecurityProperties.parseAcl(zookeeperProperties.getAcl(), ZooDefs.Ids.OPEN_ACL_UNSAFE.get(0));

            acls.add(parsedACL);

            //adding world read permission
            if (StringUtils.isNotEmpty(zookeeperProperties.getAcl())) {
                ACL worldReadPermissionACL = new ACL(ZooDefs.Perms.READ, new Id("world", "anyone"));

                acls.add(worldReadPermissionACL);
            }

            Stat serverInfo = client.checkExists().forPath(getZnodePath(zookeeperProperties));

            if (serverInfo == null) {
                client.create()
                        .withMode(CreateMode.EPHEMERAL)
                        .withACL(acls)
                        .forPath(getZnodePath(zookeeperProperties));
            }

            client.setData().forPath(getZnodePath(zookeeperProperties), atlasServerAddress.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new AtlasBaseException(AtlasErrorCode.CURATOR_FRAMEWORK_UPDATE, e, "forPath: getZnodePath");
        }
    }

    @Override
    public String getActiveServerAddress() {
        CuratorFramework            client         = curatorFactory.clientInstance();
        String                      serverAddress  = null;

        try {
            HighAvailabilityProperties zookeeperProperties = haSupport.getZookeeperProperties(configuration);
            byte[]                    bytes            = client.getData().forPath(getZnodePath(zookeeperProperties));

            serverAddress = new String(bytes, StandardCharsets.UTF_8);
        } catch (Exception e) {
            LOG.error("Error getting active server address", e);
        }

        return serverAddress;
    }

    private String getZnodePath(HighAvailabilityProperties zookeeperProperties) {
        return zookeeperProperties.getZkRoot() + APACHE_ATLAS_ACTIVE_SERVER_INFO;
    }
}
