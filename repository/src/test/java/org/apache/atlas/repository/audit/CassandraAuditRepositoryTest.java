/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.repository.audit;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.testng.annotations.BeforeClass;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CassandraAuditRepositoryTest extends AuditRepositoryTestBase {

  @BeforeClass
  public void setup() throws InterruptedException, TTransportException, ConfigurationException, IOException,
      AtlasException {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra_test.yml");
    eventRepository = new CassandraBasedAuditRepository();
    Map<String, Object> props = new HashMap<>();
    props.put(CassandraBasedAuditRepository.MANAGE_EMBEDDED_CASSANDRA, Boolean.TRUE);
    props.put(CassandraBasedAuditRepository.CASSANDRA_CLUSTERNAME_PROPERTY, "Test Cluster");
    props.put(CassandraBasedAuditRepository.CASSANDRA_HOSTNAME_PROPERTY, "localhost");
    props.put(CassandraBasedAuditRepository.CASSANDRA_PORT_PROPERTY, 9042);
    Configuration atlasConf = new MapConfiguration(props);
    ((CassandraBasedAuditRepository)eventRepository).setApplicationProperties(atlasConf);
    ((CassandraBasedAuditRepository)eventRepository).start();
    // Pause for a second to ensure that the embedded cluster has started
    Thread.sleep(1000);
  }

}
