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

import com.google.common.annotations.VisibleForTesting;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.EntityAuditEvent;
import org.apache.atlas.annotation.ConditionalOnAtlasProperty;
import org.apache.atlas.ha.HAConfiguration;
import org.apache.atlas.listener.ActiveStateChangeHandler;
import org.apache.atlas.service.Service;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Singleton;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * HBase based repository for entity audit events
 * Table -> 1, ATLAS_ENTITY_EVENTS
 * Key -> entity id + timestamp
 * Column Family -> 1,dt
 * Columns -> action, user, detail
 * versions -> 1
 *
 * Note: The timestamp in the key is assumed to be timestamp in milli seconds. Since the key is entity id + timestamp,
 * and only 1 version is kept, there can be just 1 audit event per entity id + timestamp. This is ok for one atlas server.
 * But if there are more than one atlas servers, we should use server id in the key
 */
@Singleton
@Component
@ConditionalOnAtlasProperty(property = "atlas.EntityAuditRepository.impl", isDefault = true)
public class HBaseBasedAuditRepository implements Service, EntityAuditRepository, ActiveStateChangeHandler {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseBasedAuditRepository.class);

    public static final String CONFIG_PREFIX = "atlas.audit";
    public static final String CONFIG_TABLE_NAME = CONFIG_PREFIX + ".hbase.tablename";
    public static final String DEFAULT_TABLE_NAME = "ATLAS_ENTITY_AUDIT_EVENTS";
    public static final String CONFIG_PERSIST_ENTITY_DEFINITION = CONFIG_PREFIX + ".persistEntityDefinition";

    public static final byte[] COLUMN_FAMILY = Bytes.toBytes("dt");
    public static final byte[] COLUMN_ACTION = Bytes.toBytes("a");
    public static final byte[] COLUMN_DETAIL = Bytes.toBytes("d");
    public static final byte[] COLUMN_USER = Bytes.toBytes("u");
    public static final byte[] COLUMN_DEFINITION = Bytes.toBytes("f");

    private static final String  AUDIT_REPOSITORY_MAX_SIZE_PROPERTY = "atlas.hbase.client.keyvalue.maxsize";
    private static final String  AUDIT_EXCLUDE_ATTRIBUTE_PROPERTY   = "atlas.audit.hbase.entity";
    private static final String  FIELD_SEPARATOR = ":";
    private static final long    ATLAS_HBASE_KEYVALUE_DEFAULT_SIZE = 1024 * 1024;
    private static Configuration APPLICATION_PROPERTIES = null;

    private static boolean       persistEntityDefinition;

    private Map<String, List<String>> auditExcludedAttributesCache = new HashMap<>();

    static {
        try {
            persistEntityDefinition = ApplicationProperties.get().getBoolean(CONFIG_PERSIST_ENTITY_DEFINITION, false);
        } catch (AtlasException e) {
            throw new RuntimeException(e);
        }
    }
    private TableName tableName;
    private Connection connection;

    /**
     * Add events to the event repository
     * @param events events to be added
     * @throws AtlasException
     */
    @Override
    public void putEvents(EntityAuditEvent... events) throws AtlasException {
        putEvents(Arrays.asList(events));
    }

    @Override
    /**
     * Add events to the event repository
     * @param events events to be added
     * @throws AtlasException
     */
    public void putEvents(List<EntityAuditEvent> events) throws AtlasException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Putting {} events", events.size());
        }

        Table table = null;
        try {
            table = connection.getTable(tableName);
            List<Put> puts = new ArrayList<>(events.size());
            for (EntityAuditEvent event : events) {
                LOG.debug("Adding entity audit event {}", event);
                Put put = new Put(getKey(event.getEntityId(), event.getTimestamp()));
                addColumn(put, COLUMN_ACTION, event.getAction());
                addColumn(put, COLUMN_USER, event.getUser());
                addColumn(put, COLUMN_DETAIL, event.getDetails());
                if (persistEntityDefinition) {
                    addColumn(put, COLUMN_DEFINITION, event.getEntityDefinitionString());
                }
                puts.add(put);
            }
            table.put(puts);
        } catch (IOException e) {
            throw new AtlasException(e);
        } finally {
            close(table);
        }
    }

    private <T> void addColumn(Put put, byte[] columnName, T columnValue) {
        if (columnValue != null && !columnValue.toString().isEmpty()) {
            put.addColumn(COLUMN_FAMILY, columnName, Bytes.toBytes(columnValue.toString()));
        }
    }

    private byte[] getKey(String id, Long ts) {
        assert id != null : "entity id can't be null";
        assert ts != null : "timestamp can't be null";
        String keyStr = id + FIELD_SEPARATOR + ts;
        return Bytes.toBytes(keyStr);
    }

    /**
     * List events for the given entity id in decreasing order of timestamp, from the given startKey. Returns n results
     * @param entityId entity id
     * @param startKey key for the first event to be returned, used for pagination
     * @param n number of events to be returned
     * @return list of events
     * @throws AtlasException
     */
    public List<EntityAuditEvent> listEvents(String entityId, String startKey, short n)
            throws AtlasException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Listing events for entity id {}, starting timestamp {}, #records {}", entityId, startKey, n);
        }

        Table table = null;
        ResultScanner scanner = null;
        try {
            table = connection.getTable(tableName);

            /**
             * Scan Details:
             * In hbase, the events are stored in increasing order of timestamp. So, doing reverse scan to get the latest event first
             * Page filter is set to limit the number of results returned.
             * Stop row is set to the entity id to avoid going past the current entity while scanning
             * small is set to true to optimise RPC calls as the scanner is created per request
             */
            Scan scan = new Scan().setReversed(true).setFilter(new PageFilter(n))
                                  .setStopRow(Bytes.toBytes(entityId))
                                  .setCaching(n)
                                  .setSmall(true);
            if (StringUtils.isEmpty(startKey)) {
                //Set start row to entity id + max long value
                byte[] entityBytes = getKey(entityId, Long.MAX_VALUE);
                scan = scan.setStartRow(entityBytes);
            } else {
                scan = scan.setStartRow(Bytes.toBytes(startKey));
            }
            scanner = table.getScanner(scan);
            Result result;
            List<EntityAuditEvent> events = new ArrayList<>();

            //PageFilter doesn't ensure n results are returned. The filter is per region server.
            //So, adding extra check on n here
            while ((result = scanner.next()) != null && events.size() < n) {
                EntityAuditEvent event = fromKey(result.getRow());

                //In case the user sets random start key, guarding against random events
                if (!event.getEntityId().equals(entityId)) {
                    continue;
                }
                event.setUser(getResultString(result, COLUMN_USER));
                event.setAction(EntityAuditEvent.EntityAuditAction.valueOf(getResultString(result, COLUMN_ACTION)));
                event.setDetails(getResultString(result, COLUMN_DETAIL));
                if (persistEntityDefinition) {
                    String colDef = getResultString(result, COLUMN_DEFINITION);
                    if (colDef != null) {
                        event.setEntityDefinition(colDef);
                    }
                }
                events.add(event);
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Got events for entity id {}, starting timestamp {}, #records {}", entityId, startKey, events.size());
            }

            return events;
        } catch (IOException e) {
            throw new AtlasException(e);
        } finally {
            close(scanner);
            close(table);
        }
    }

    @Override
    public long repositoryMaxSize() throws AtlasException {
        long ret;
        initApplicationProperties();

        if (APPLICATION_PROPERTIES == null) {
            ret = ATLAS_HBASE_KEYVALUE_DEFAULT_SIZE;
        } else {
            ret = APPLICATION_PROPERTIES.getLong(AUDIT_REPOSITORY_MAX_SIZE_PROPERTY, ATLAS_HBASE_KEYVALUE_DEFAULT_SIZE);
        }

        return ret;
    }

    @Override
    public List<String> getAuditExcludeAttributes(String entityType) throws AtlasException {
        List<String> ret = null;

        initApplicationProperties();

        if (auditExcludedAttributesCache.containsKey(entityType)) {
            ret = auditExcludedAttributesCache.get(entityType);
        } else if (APPLICATION_PROPERTIES != null) {
            String[] excludeAttributes = APPLICATION_PROPERTIES.getStringArray(AUDIT_EXCLUDE_ATTRIBUTE_PROPERTY + "." +
                    entityType + "." +  "attributes.exclude");

            if (excludeAttributes != null) {
                ret = Arrays.asList(excludeAttributes);
            }

            auditExcludedAttributesCache.put(entityType, ret);
        }

        return ret;
    }

    private void initApplicationProperties() {
        if (APPLICATION_PROPERTIES == null) {
            try {
                APPLICATION_PROPERTIES = ApplicationProperties.get();
            } catch (AtlasException ex) {
                // ignore
            }
        }
    }

    private String getResultString(Result result, byte[] columnName) {
        byte[] rawValue = result.getValue(COLUMN_FAMILY, columnName);
        if ( rawValue != null) {
            return Bytes.toString(rawValue);
        }
        return null;
    }

    private EntityAuditEvent fromKey(byte[] keyBytes) {
        String key = Bytes.toString(keyBytes);
        EntityAuditEvent event = new EntityAuditEvent();
        if (StringUtils.isNotEmpty(key)) {
            String[] parts = key.split(FIELD_SEPARATOR);
            event.setEntityId(parts[0]);
            event.setTimestamp(Long.valueOf(parts[1]));
            event.setEventKey(key);
        }
        return event;
    }

    private void close(Closeable closeable) throws AtlasException {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException e) {
                throw new AtlasException(e);
            }
        }
    }

    /**
     * Converts atlas' application properties to hadoop conf
     * @return
     * @throws AtlasException
     * @param atlasConf
     */
    public static org.apache.hadoop.conf.Configuration getHBaseConfiguration(Configuration atlasConf) throws AtlasException {
        Configuration subsetAtlasConf =
                ApplicationProperties.getSubsetConfiguration(atlasConf, CONFIG_PREFIX);
        org.apache.hadoop.conf.Configuration hbaseConf = HBaseConfiguration.create();
        Iterator<String> keys = subsetAtlasConf.getKeys();
        while (keys.hasNext()) {
            String key = keys.next();
            hbaseConf.set(key, subsetAtlasConf.getString(key));
        }
        return hbaseConf;
    }

    private void createTableIfNotExists() throws AtlasException {
        Admin admin = null;
        try {
            admin = connection.getAdmin();
            LOG.info("Checking if table {} exists", tableName.getNameAsString());
            if (!admin.tableExists(tableName)) {
                LOG.info("Creating table {}", tableName.getNameAsString());
                HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
                HColumnDescriptor columnFamily = new HColumnDescriptor(COLUMN_FAMILY);
                columnFamily.setMaxVersions(1);
                columnFamily.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
                columnFamily.setCompressionType(Compression.Algorithm.GZ);
                columnFamily.setBloomFilterType(BloomType.ROW);
                tableDescriptor.addFamily(columnFamily);
                admin.createTable(tableDescriptor);
            } else {
                LOG.info("Table {} exists", tableName.getNameAsString());
            }
        } catch (IOException e) {
            throw new AtlasException(e);
        } finally {
            close(admin);
        }
    }

    @Override
    public void start() throws AtlasException {
        Configuration configuration = ApplicationProperties.get();
        startInternal(configuration, getHBaseConfiguration(configuration));
    }

    @VisibleForTesting
    void startInternal(Configuration atlasConf,
                                 org.apache.hadoop.conf.Configuration hbaseConf) throws AtlasException {

        String tableNameStr = atlasConf.getString(CONFIG_TABLE_NAME, DEFAULT_TABLE_NAME);
        tableName = TableName.valueOf(tableNameStr);

        try {
            connection = createConnection(hbaseConf);
        } catch (IOException e) {
            throw new AtlasException(e);
        }

        if (!HAConfiguration.isHAEnabled(atlasConf)) {
            LOG.info("HA is disabled. Hence creating table on startup.");
            createTableIfNotExists();
        }
    }

    @VisibleForTesting
    protected Connection createConnection(org.apache.hadoop.conf.Configuration hbaseConf) throws IOException {
        return ConnectionFactory.createConnection(hbaseConf);
    }

    @Override
    public void stop() throws AtlasException {
        close(connection);
    }

    @Override
    public void instanceIsActive() throws AtlasException {
        LOG.info("Reacting to active: Creating HBase table for Audit if required.");
        createTableIfNotExists();
    }

    @Override
    public void instanceIsPassive() {
        LOG.info("Reacting to passive: No action for now.");
    }
}
