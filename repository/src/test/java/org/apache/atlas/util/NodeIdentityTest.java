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
import org.testng.annotations.Test;

import java.lang.management.ManagementFactory;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class NodeIdentityTest {
    @Test
    public void nodeIdNamesTheRunModeHostAndJvm() {
        String expected = AtlasRunMode.current().name() + "@" + NodeIdentity.hostName() + "#" + ManagementFactory.getRuntimeMXBean().getName();

        assertEquals(NodeIdentity.nodeId(), expected);
    }

    @Test
    public void hostNameIsNeverBlank() {
        assertFalse(StringUtils.isBlank(NodeIdentity.hostName()));
    }

    @Test
    public void serverOfKeepsTheRunModeAndTheJvmHost() {
        assertEquals(NodeIdentity.serverOf("MONOLITHIC@host-1#123@host-1"), "MONOLITHIC@host-1");
        assertEquals(NodeIdentity.serverOf("MONOLITHIC@unknown-host#3166151@host-1"), "MONOLITHIC@host-1");
        assertNull(NodeIdentity.serverOf("a-peer"));
        assertNull(NodeIdentity.serverOf(null));
    }

    @Test
    public void anEarlierProcessOnThisHostIsThisServer() {
        String jvmHost = jvmHost();

        assertTrue(NodeIdentity.isEarlierProcessOfThisServer(AtlasRunMode.current().name() + "@unknown-host#1@" + jvmHost));
    }

    @Test
    public void thisProcessIsNotAnEarlierProcess() {
        assertFalse(NodeIdentity.isEarlierProcessOfThisServer(NodeIdentity.nodeId()));
    }

    @Test
    public void anotherHostOrRunModeIsNotThisServer() {
        String jvmHost = jvmHost();

        assertFalse(NodeIdentity.isEarlierProcessOfThisServer(AtlasRunMode.current().name() + "@other#1@other-host"));
        assertFalse(NodeIdentity.isEarlierProcessOfThisServer("NOT_A_RUN_MODE@" + jvmHost + "#1@" + jvmHost));
        assertFalse(NodeIdentity.isEarlierProcessOfThisServer(null));
    }

    private static String jvmHost() {
        String jvmName = ManagementFactory.getRuntimeMXBean().getName();

        return jvmName.substring(jvmName.lastIndexOf('@') + 1);
    }
}
