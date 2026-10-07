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
package org.apache.atlas.semantic;

import org.apache.http.HttpHost;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;

public class OpenSearchClientFactoryTest {
    @Test
    public void parseHostsSupportsListsAndHostPort() throws Exception {
        List<HttpHost> hosts = OpenSearchClientFactory.parseHosts(
                new String[] {"os1", "os2:9201, os3 "}, 9200, "https");

        assertEquals(hosts.size(), 3);
        assertEquals(hosts.get(0).toURI(), "https://os1:9200");
        assertEquals(hosts.get(1).toURI(), "https://os2:9201");
        assertEquals(hosts.get(2).toURI(), "https://os3:9200");
    }

    @Test(expectedExceptions = SemanticSearchException.class)
    public void parseHostsRejectsEmpty() throws Exception {
        OpenSearchClientFactory.parseHosts(new String[] {" "}, 9200, "http");
    }

    @Test(expectedExceptions = SemanticSearchException.class, expectedExceptionsMessageRegExp = ".*IPv6.*")
    public void parseHostsRejectsIpv6() throws Exception {
        OpenSearchClientFactory.parseHosts(new String[] {"[::1]:9200"}, 9200, "http");
    }

    @Test(expectedExceptions = SemanticSearchException.class, expectedExceptionsMessageRegExp = ".*invalid port in 'os1:abc'.*")
    public void parseHostsRejectsNonNumericPort() throws Exception {
        OpenSearchClientFactory.parseHosts(new String[] {"os1:abc"}, 9200, "http");
    }

    @Test(expectedExceptions = SemanticSearchException.class, expectedExceptionsMessageRegExp = ".*invalid port.*")
    public void parseHostsRejectsOutOfRangePort() throws Exception {
        OpenSearchClientFactory.parseHosts(new String[] {"os1:70000"}, 9200, "http");
    }

    @Test(expectedExceptions = SemanticSearchException.class)
    public void parseHostsRejectsMissingHost() throws Exception {
        OpenSearchClientFactory.parseHosts(new String[] {":9200"}, 9200, "http");
    }
}
