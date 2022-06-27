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

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.JdkSSLOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.Session;
import com.google.common.annotations.VisibleForTesting;
import org.apache.atlas.AtlasException;
import org.apache.atlas.EntityAuditEvent;
import org.apache.atlas.annotation.ConditionalOnAtlasProperty;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.audit.EntityAuditEventV2;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Singleton;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/**
 * This class provides cassandra support as the backend for audit storage support.
 */
@Singleton
@Component
@ConditionalOnAtlasProperty(property = "atlas.EntityAuditRepository.impl")
public class CassandraBasedAuditRepository extends AbstractStorageBasedAuditRepository {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraBasedAuditRepository.class);

  // Default keyspace to store the audit entries
  private static final String DEFAULT_KEYSPACE = "atlas_audit";
  // When running in embedded cassandra mode, this is the default cluster name used
  private static final String DEFAULT_CLUSTER_NAME = "JanusGraph";
  // Default cassandra port
  private static final int DEFAULT_PORT = 9042;
  private static final int DEFAULT_REPLICATION_FACTOR = 3;
  // The environment variable that tells us we are running in embedded mode
  public static final String MANAGE_EMBEDDED_CASSANDRA = "MANAGE_EMBEDDED_CASSANDRA";

  // Application properties
  public static final String CASSANDRA_HOSTNAME_PROPERTY = "atlas.graph.storage.hostname";
  public static final String CASSANDRA_CLUSTERNAME_PROPERTY = "atlas.graph.storage.clustername";
  public static final String CASSANDRA_PORT_PROPERTY = "atlas.graph.storage.port";
  public static final String CASSANDRA_REPLICATION_FACTOR_PROPERTY = "atlas.EntityAuditRepository.replicationFactor";
  public static final String CASSANDRA_AUDIT_KEYSPACE_PROPERTY = "atlas.EntityAuditRepository.keyspace";

  //flag to indicate if SSL is enabled for cassandra (server side SSL)
  public static final String CASSANDRA_SSL_ENABLED = "atlas.graph.storage.sslEnabled";
  public static final String CASSANDRA_SSL_TRUSTSTORE_LOCATION = "atlas.graph.storage.cassandra.ssl.truststore.location";
  public static final String CASSANDRA_SSL_TRUSTSTORE_PASSWORD = "atlas.graph.storage.cassandra.ssl.truststore.password";

  // cassandra login credentials
  public static final String CASSANDRA_USERNAME = "atlas.audit.cassandra.username";
  public static final String CASSANDRA_PASSWORD = "atlas.audit.cassandra.password";
  public static final String KEYSTORE_TYPE = "JKS";
  public static final String SSL_PROTOCOL = "TLS";

  private static final String  AUDIT_TABLE_SCHEMA =
          "CREATE TABLE audit(entityid text, "
                  + "created bigint, "
                  + "action text, "
                  + "user text, "
                  + "detail text, "
                  + "entity text, "
                  + "PRIMARY KEY (entityid, created)"
                  + ") WITH CLUSTERING ORDER BY (created DESC);";

  private static final String ENTITYID = "entityid";
  private static final String CREATED = "created";
  private static final String ACTION = "action";
  private static final String USER = "user";
  private static final String DETAIL = "detail";
  private static final String ENTITY = "entity";

  private static final String INSERT_STATEMENT_TEMPLATE = "INSERT INTO audit (entityid,created,action,user,detail,entity) VALUES (?,?,?,?,?,?)";
  private static final String SELECT_STATEMENT_TEMPLATE = "select * from audit where entityid=? order by created desc limit 10;";
  private static final String SELECT_DATE_STATEMENT_TEMPLATE = "select * from audit where entityid=? and created<=? order by created desc limit 10;";


  private String keyspace;
  private int replicationFactor;
  private Session cassSession;
  private String clusterName;
  private int port;

  private boolean isSSLEnabled;
  private String username;
  private String password;
  private String truststoreLocation;
  private String truststorePassword;

  private Map<String, List<String>> auditExcludedAttributesCache = new HashMap<>();
  private PreparedStatement insertStatement;
  private PreparedStatement selectStatement;
  private PreparedStatement selectDateStatement;

  @Override
  public void putEventsV1(List<EntityAuditEvent> events) throws AtlasException {
    BoundStatement stmt = new BoundStatement(insertStatement);
    BatchStatement batch = new BatchStatement();
    events.forEach(event -> batch.add(stmt.bind(event.getEntityId(), event.getTimestamp(),
            event.getAction().toString(), event.getUser(), event.getDetails(),
            (persistEntityDefinition ? event.getEntityDefinitionString() : null))));
    cassSession.execute(batch);
  }

  @Override
  public void putEventsV2(List<EntityAuditEventV2> events) throws AtlasBaseException {
    BoundStatement stmt = new BoundStatement(insertStatement);
    BatchStatement batch = new BatchStatement();
    events.forEach(event -> batch.add(stmt.bind(event.getEntityId(), event.getTimestamp(),
            event.getAction().toString(), event.getUser(), event.getDetails(),
            (persistEntityDefinition ? event.getEntityDefinitionString() : null))));
    cassSession.execute(batch);
  }

  private BoundStatement getSelectStatement(String entityId, String startKey) {
    BoundStatement stmt;
    if (StringUtils.isEmpty(startKey)) {
      stmt = new BoundStatement(selectStatement).bind(entityId);
    } else {
      stmt = new BoundStatement(selectDateStatement).bind(entityId, Long.valueOf(startKey.split(FIELD_SEPARATOR)[1]));
    }
    return stmt;
  }

  @Override
  public List<EntityAuditEvent> listEventsV1(String entityId, String startKey, short maxResults) throws AtlasException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Listing events for entity id {}, starting timestamp {}, #records {}", entityId, startKey, maxResults);
    }

    ResultSet rs = cassSession.execute(getSelectStatement(entityId, startKey));
    List<EntityAuditEvent> entityResults = new ArrayList<>();
    for (Row row : rs) {
      String rowEntityId = row.getString(ENTITYID);
      if (!entityId.equals(rowEntityId)) {
        continue;
      }
      EntityAuditEvent event = new EntityAuditEvent();
      event.setEntityId(rowEntityId);
      event.setAction(EntityAuditEvent.EntityAuditAction.fromString(row.getString(ACTION)));
      event.setDetails(row.getString(DETAIL));
      event.setUser(row.getString(USER));
      event.setTimestamp(row.getLong(CREATED));
      event.setEventKey(rowEntityId + ":" + event.getTimestamp());
      if (persistEntityDefinition) {
        event.setEntityDefinition(row.getString(ENTITY));
      }
      entityResults.add(event);
    }
    return entityResults;
  }

  @Override
  public List<EntityAuditEventV2> listEventsV2(String entityId, EntityAuditEventV2.EntityAuditActionV2 auditAction, String startKey, short maxResults) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Listing events for entity id {}, starting timestamp {}, #records {}", entityId, startKey, maxResults);
    }

    ResultSet rs = cassSession.execute(getSelectStatement(entityId, startKey));
    List<EntityAuditEventV2> entityResults = new ArrayList<>();
    for (Row row : rs) {
      String rowEntityId = row.getString(ENTITYID);
      if (!entityId.equals(rowEntityId)) {
        continue;
      }
      EntityAuditEventV2 event = new EntityAuditEventV2();
      event.setEntityId(rowEntityId);
      event.setAction(EntityAuditEventV2.EntityAuditActionV2.fromString(row.getString(ACTION)));
      event.setDetails(row.getString(DETAIL));
      event.setUser(row.getString(USER));
      event.setTimestamp(row.getLong(CREATED));
      event.setEventKey(rowEntityId + ":" + event.getTimestamp());
      if (persistEntityDefinition) {
        event.setEntityDefinition(row.getString(ENTITY));
      }
      entityResults.add(event);
    }
    return entityResults;
  }

  @Override
  public List<EntityAuditEventV2> listEventsV2(String entityId, EntityAuditEventV2.EntityAuditActionV2 auditAction, String sortByColumn, boolean sortOrderDesc, int offset, short limit) {
    return listEventsV2(entityId, auditAction, null, limit);
  }

  @Override
  public Set<String> getEntitiesWithTagChanges(long fromTimestamp, long toTimestamp) throws AtlasBaseException {
    throw new NotImplementedException();
  }

  @Override
  public void start() throws AtlasException {
    initApplicationProperties();
    initializeSettings();
    startInternal();
  }

  void initializeSettings() {
    keyspace = APPLICATION_PROPERTIES.getString(CASSANDRA_AUDIT_KEYSPACE_PROPERTY, DEFAULT_KEYSPACE);
    replicationFactor = APPLICATION_PROPERTIES.getInt(CASSANDRA_REPLICATION_FACTOR_PROPERTY, DEFAULT_REPLICATION_FACTOR);
    clusterName = APPLICATION_PROPERTIES.getString(CASSANDRA_CLUSTERNAME_PROPERTY, DEFAULT_CLUSTER_NAME);
    port = APPLICATION_PROPERTIES.getInt(CASSANDRA_PORT_PROPERTY, DEFAULT_PORT);
    isSSLEnabled = APPLICATION_PROPERTIES.getBoolean(CASSANDRA_SSL_ENABLED, false);
    username = APPLICATION_PROPERTIES.getString(CASSANDRA_USERNAME, null);
    password = APPLICATION_PROPERTIES.getString(CASSANDRA_PASSWORD, null);
    truststoreLocation = APPLICATION_PROPERTIES.getString(CASSANDRA_SSL_TRUSTSTORE_LOCATION, null);
    truststorePassword = APPLICATION_PROPERTIES.getString(CASSANDRA_SSL_TRUSTSTORE_PASSWORD, null);
  }

  @VisibleForTesting
  void startInternal() throws AtlasException {
    createSession();
  }

  void createSession() throws AtlasException {
    Cluster.Builder cassandraClusterBuilder = Cluster.builder();

    String hostname = APPLICATION_PROPERTIES.getString(CASSANDRA_HOSTNAME_PROPERTY, "localhost");

    Cluster cluster = null;
    if (isSSLEnabled) {
      if (StringUtils.isNotEmpty(username) && StringUtils.isNotEmpty(password)) {
        try {
          cluster = cassandraClusterBuilder.addContactPoint(hostname).withClusterName(clusterName).withPort(port)
                  .withCredentials(username, password).withSSL(createSSLOptions()).build();
        } catch (KeyManagementException | KeyStoreException | NoSuchAlgorithmException | CertificateException
                | IOException e) {
          throw new AtlasException(e);
        }
      }
    } else {
      if (username != null && password != null){
        cluster = cassandraClusterBuilder.addContactPoint(hostname).withClusterName(clusterName).withPort(port).withCredentials(username,password).build();
      } else {
        cluster = cassandraClusterBuilder.addContactPoint(hostname).withClusterName(clusterName).withPort(port).build();
      }

    }

    try {
      cassSession = cluster.connect();
      if (cluster.getMetadata().getKeyspace(keyspace) == null) {
        String query = "CREATE KEYSPACE " + keyspace + " WITH replication "
                + "= {'class':'SimpleStrategy', 'replication_factor':" + replicationFactor + "}; ";
        cassSession.execute(query);
        cassSession.close();
        cassSession = cluster.connect(keyspace);
        // create the audit table
        cassSession.execute(AUDIT_TABLE_SCHEMA);
      } else {
        cassSession.close();
        cassSession = cluster.connect(keyspace);
      }

      insertStatement = cassSession.prepare(INSERT_STATEMENT_TEMPLATE.replace("KEYSPACE", keyspace));
      selectStatement = cassSession.prepare(SELECT_STATEMENT_TEMPLATE.replace("KEYSPACE", keyspace));
      selectDateStatement = cassSession.prepare(SELECT_DATE_STATEMENT_TEMPLATE.replace("KEYSPACE", keyspace));
    } catch (Exception e) {
      throw new AtlasException(e);
    }
  }

  private SSLOptions createSSLOptions() throws KeyStoreException, NoSuchAlgorithmException, CertificateException,
          FileNotFoundException, IOException, KeyManagementException {
    TrustManagerFactory tmf = null;

    KeyStore tks = KeyStore.getInstance(KEYSTORE_TYPE);
    tks.load((InputStream) new FileInputStream(new File(truststoreLocation)), truststorePassword.toCharArray());
    tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init(tks);

    SSLContext sslContext = SSLContext.getInstance(SSL_PROTOCOL);
    sslContext.init(null, tmf != null ? tmf.getTrustManagers() : null, null);

    return JdkSSLOptions.builder().withSSLContext(sslContext).build();
  }

  @Override
  public void stop() throws AtlasException {
    cassSession.close();
  }

}