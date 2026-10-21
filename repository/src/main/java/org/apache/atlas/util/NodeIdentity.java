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
package org.apache.atlas.util;

import org.apache.atlas.AtlasRunMode;
import org.apache.commons.lang3.StringUtils;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Identifies this JVM in the claims and leases that Atlas nodes share through the graph.
 */
public final class NodeIdentity {
    private static final String UNKNOWN_HOST = "unknown-host";

    private NodeIdentity() {
    }

    /**
     * Returns {@code <RUN_MODE>@<host>#<pid>@<host>}.
     */
    public static String nodeId() {
        return AtlasRunMode.current().name() + "@" + hostName() + "#" + ManagementFactory.getRuntimeMXBean().getName();
    }

    /**
     * {@code HOSTNAME} is usually set only in containers; on a host started by a service manager it
     * is empty, so the resolved local host name is used instead.
     */
    static String hostName() {
        String hostName = System.getenv("HOSTNAME");

        if (StringUtils.isNotBlank(hostName)) {
            return hostName;
        }

        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return UNKNOWN_HOST;
        }
    }
}
