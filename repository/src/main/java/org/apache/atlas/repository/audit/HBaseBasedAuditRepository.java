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
import com.google.inject.Singleton;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
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
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * HBase based repository for entity audit events
 * Table -> 1, ATLAS_ENTITY_EVENTS
 * Key -> entity id + timestamp
 * Column Family -> 1,dt
 * Columns -> action, user, detail
 * versions -> 1
 *
 * Note: The timestamp in the key is assumed to be timestamp in nano seconds. Since the key is entity id + timestamp,
 * and only 1 version is kept, there can be just 1 audit event per entity id + timestamp. This is ok for one atlas server.
 * But if there are more than one atlas servers, we should use server id in the key
 */
@Singleton
public class HBaseBasedAuditRepository implements Service, EntityAuditRepository, ActiveStateChangeHandler {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseBasedAuditRepository.class);

    public static final String CONFIG_PREFIX = "atlas.audit";
    public static final String CONFIG_TABLE_NAME = CONFIG_PREFIX + ".hbase.tablename";
    public static final String DEFAULT_TABLE_NAME = "ATLAS_ENTITY_AUDIT_EVENTS";

    private static final String FIELD_SEPARATOR = ":";

    public static final byte[] COLUMN_FAMILY = Bytes.toBytes("dt");
    public static final byte[] COLUMN_ACTION = Bytes.toBytes("action");
    public static final byte[] COLUMN_DETAIL = Bytes.toBytes("detail");
    public static final byte[] COLUMN_USER = Bytes.toBytes("user");

    private TableName tableName;
    private Connection connection;

    /**
     * Add events to the event repository
     * @param events events to be added
     * @throws AtlasException
     */
    @Override
    public void putEvents(EntityAuditRepository.EntityAuditEvent... events) throws AtlasException {
        putEvents(Arrays.asList(events));
    }

    @Override
    /**
     * Add events to the event repository
     * @param events events to be added
     * @throws AtlasException
     */
    public void putEvents(List<EntityAuditEvent> events) throws AtlasException {
        LOG.info("Putting {} events", events.size());
        Table table = null;
        try {
            table = connection.getTable(tableName);
            List<Put> puts = new ArrayList<>(events.size());
            for (EntityAuditRepository.EntityAuditEvent event : events) {
                LOG.debug("Adding entity audit event {}", event);
                Put put = new Put(getKey(event.entityId, event.timestamp));
                if (event.action != null) {
                    put.addColumn(COLUMN_FAMILY, COLUMN_ACTION, Bytes.toBytes((short)event.action.ordinal()));
                }
                addColumn(put, COLUMN_USER, event.user);
                addColumn(put, COLUMN_DETAIL, event.details);
                puts.add(put);
            }
            table.put(puts);
        } catch (IOException e) {
            throw new AtlasException(e);
        } finally {
            close(table);
        }
    }

    private void addColumn(Put put, byte[] columnName, String columnValue) {
        if (StringUtils.isNotEmpty(columnValue)) {
            put.addColumn(COLUMN_FAMILY, columnName, Bytes.toBytes(columnValue));
        }
    }

    private byte[] getKey(String id, Long ts) {
        assert id != null : "entity id can't be null";
        assert ts != null : "timestamp can't be null";
        String keyStr = id + FIELD_SEPARATOR + ts;
        return Bytes.toBytes(keyStr);
    }

    /**
     * List events for the given entity id in decreasing order of timestamp, from the given timestamp. Returns n results
     * @param entityId entity id
     * @param ts starting timestamp for events
     * @param n number of events to be returned
     * @return list of events
     * @throws AtlasException
     */
    public List<EntityAuditRepository.EntityAuditEvent> listEvents(String entityId, Long ts, short n)
            throws AtlasException {
        LOG.info("Listing events for entity id {}, starting timestamp {}, #records {}", entityId, ts, n);
        Table table = null;
        ResultScanner scanner = null;
        try {
            table = connection.getTable(tableName);
            Scan scan = new Scan().setReversed(true).setFilter(new PageFilter(n))
                                  .setStartRow(getKey(entityId, ts))
                                  .setStopRow(Bytes.toBytes(entityId))
                                  .setCaching(n)
                                  .setSmall(true);
            scanner = table.getScanner(scan);
            Result result;
            List<EntityAuditRepository.EntityAuditEvent> events = new ArrayList<>();

            //PageFilter doesn't ensure n results are returned. The filter is per region server.
            //So, adding extra check on n here
            while ((result = scanner.next()) != null && events.size() < n) {
                String key = Bytes.toString(result.getRow());
                EntityAuditRepository.EntityAuditEvent event = fromKey(key);
                event.user = getResultString(result, COLUMN_USER);
                event.action =
                        EntityAuditAction.values()[(Bytes.toShort(result.getValue(COLUMN_FAMILY, COLUMN_ACTION)))];
                event.details = getResultString(result, COLUMN_DETAIL);
                events.add(event);
            }
            LOG.info("Got events for entity id {}, starting timestamp {}, #records {}", entityId, ts, events.size());
            return events;
        } catch (IOException e) {
            throw new AtlasException(e);
        } finally {
            close(scanner);
            close(table);
        }
    }

    private String getResultString(Result result, byte[] columnName) {
        return Bytes.toString(result.getValue(COLUMN_FAMILY, columnName));
    }

    private EntityAuditEvent fromKey(String key) {
        EntityAuditEvent event = new EntityAuditEvent();
        if (StringUtils.isNotEmpty(key)) {
            String[] parts = key.split(FIELD_SEPARATOR);
            event.entityId = parts[0];
            event.timestamp = Long.valueOf(parts[1]);
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
        try {
            Admin admin = connection.getAdmin();
            LOG.info("Checking if table {} exists", tableName.getNameAsString());
            if (!admin.tableExists(tableName)) {
                LOG.info("Creating table {}", tableName.getNameAsString());
                HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
                HColumnDescriptor columnFamily = new HColumnDescriptor(COLUMN_FAMILY);
                columnFamily.setMaxVersions(1);
                tableDescriptor.addFamily(columnFamily);
                admin.createTable(tableDescriptor);
            } else {
                LOG.info("Table {} exists", tableName.getNameAsString());
            }
        } catch (IOException e) {
            throw new AtlasException(e);
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
