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
     * Whether {@code otherNodeId} is an earlier process of this server: same run mode and same host, different
     * process.  A host runs one Atlas server per run mode, so that process has exited.
     */
    public static boolean isEarlierProcessOfThisServer(String otherNodeId) {
        String thisNodeId = nodeId();
        String server     = serverOf(otherNodeId);

        return server != null && !StringUtils.equals(otherNodeId, thisNodeId) && server.equals(serverOf(thisNodeId));
    }

    /**
     * Reduces a node id to {@code <RUN_MODE>@<host>}.  The host is taken from the JVM name after {@code #}, which
     * stays the same across restarts even where the host part before it was recorded as {@value #UNKNOWN_HOST}.
     */
    static String serverOf(String nodeId) {
        int runModeEnd = StringUtils.indexOf(nodeId, '@');
        int jvmStart   = StringUtils.indexOf(nodeId, '#');
        int hostStart  = StringUtils.lastIndexOf(nodeId, '@');

        if (runModeEnd <= 0 || jvmStart < 0 || hostStart < jvmStart || hostStart == nodeId.length() - 1) {
            return null;
        }

        return nodeId.substring(0, runModeEnd) + "@" + nodeId.substring(hostStart + 1);
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
