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
import org.apache.atlas.EntityAuditEvent.EntityAuditAction;
import org.apache.atlas.annotation.ConditionalOnAtlasProperty;
import org.apache.atlas.model.audit.EntityAuditEventV2;
import org.apache.atlas.model.audit.EntityAuditEventV2.EntityAuditActionV2;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.ha.HAConfiguration;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.repository.converters.AtlasInstanceConverter;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.v1.model.instance.Referenceable;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.ArrayUtils;
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
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.apache.atlas.EntityAuditEvent.EntityAuditAction.TAG_ADD;
import static org.apache.atlas.EntityAuditEvent.EntityAuditAction.TAG_DELETE;
import static org.apache.atlas.EntityAuditEvent.EntityAuditAction.TAG_UPDATE;
import static org.apache.atlas.EntityAuditEvent.EntityAuditAction.TERM_ADD;
import static org.apache.atlas.EntityAuditEvent.EntityAuditAction.TERM_DELETE;
import static org.apache.atlas.model.audit.EntityAuditEventV2.EntityAuditType;
import static org.apache.atlas.model.audit.EntityAuditEventV2.EntityAuditType.ENTITY_AUDIT_V1;
import static org.apache.atlas.model.audit.EntityAuditEventV2.EntityAuditType.ENTITY_AUDIT_V2;
import static org.apache.atlas.repository.audit.EntityAuditListener.getV2AuditPrefix;

/**
 * HBase based repository for entity audit events
 * <p>
 * Table -> 1, ATLAS_ENTITY_EVENTS <br>
 * Key -> entity id + timestamp <br>
 * Column Family -> 1,dt <br>
 * Columns -> action, user, detail <br>
 * versions -> 1 <br>
 * <p>
 * Note: The timestamp in the key is assumed to be timestamp in milli seconds. Since the key is
 * entity id + timestamp, and only 1 version is kept, there can be just 1 audit event per entity
 * id + timestamp. This is ok for one atlas server. But if there are more than one atlas servers,
 * we should use server id in the key
 */
@Singleton
@Component
@ConditionalOnAtlasProperty(property = "atlas.EntityAuditRepository.impl", isDefault = true)
public class HBaseBasedAuditRepository extends AbstractStorageBasedAuditRepository {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseBasedAuditRepository.class);

    public static final String CONFIG_TABLE_NAME  = CONFIG_PREFIX + ".hbase.tablename";
    public static final String DEFAULT_TABLE_NAME = "ATLAS_ENTITY_AUDIT_EVENTS";
    public static final byte[] COLUMN_FAMILY      = Bytes.toBytes("dt");
    public static final byte[] COLUMN_ACTION      = Bytes.toBytes("a");
    public static final byte[] COLUMN_DETAIL      = Bytes.toBytes("d");
    public static final byte[] COLUMN_USER        = Bytes.toBytes("u");
    public static final byte[] COLUMN_DEFINITION  = Bytes.toBytes("f");
    public static final byte[] COLUMN_TYPE        = Bytes.toBytes("t");

    private TableName tableName;
    private Connection connection;
    private final AtlasInstanceConverter instanceConverter;

    @Inject
    public HBaseBasedAuditRepository(AtlasInstanceConverter instanceConverter) {
        this.instanceConverter = instanceConverter;
    }

    /**
     * Add events to the event repository
     * @param events events to be added
     * @throws AtlasException
     */
    @Override
    public void putEventsV1(List<EntityAuditEvent> events) throws AtlasException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Putting {} events", events.size());
        }

        Table table = null;

        try {
            table          = connection.getTable(tableName);
            List<Put> puts = new ArrayList<>(events.size());

            for (int index = 0; index < events.size(); index++) {
                EntityAuditEvent event = events.get(index);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Adding entity audit event {}", event);
                }

                Put put = new Put(getKey(event.getEntityId(), event.getTimestamp(), index));

                addColumn(put, COLUMN_ACTION, event.getAction());
                addColumn(put, COLUMN_USER, event.getUser());
                addColumn(put, COLUMN_DETAIL, event.getDetails());
                addColumn(put, COLUMN_TYPE, ENTITY_AUDIT_V1);

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

    @Override
    public void putEventsV2(List<EntityAuditEventV2> events) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Putting {} events", events.size());
        }

        Table table = null;

        try {
            table          = connection.getTable(tableName);
            List<Put> puts = new ArrayList<>(events.size());

            for (int index = 0; index < events.size(); index++) {
                EntityAuditEventV2 event = events.get(index);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Adding entity audit event {}", event);
                }

                Put put = new Put(getKey(event.getEntityId(), event.getTimestamp(), index));

                addColumn(put, COLUMN_ACTION, event.getAction());
                addColumn(put, COLUMN_USER, event.getUser());
                addColumn(put, COLUMN_DETAIL, event.getDetails());
                addColumn(put, COLUMN_TYPE, ENTITY_AUDIT_V2);

                if (persistEntityDefinition) {
                    addColumn(put, COLUMN_DEFINITION, event.getEntityDefinitionString());
                }

                puts.add(put);
            }

            table.put(puts);
        } catch (IOException e) {
            throw new AtlasBaseException(e);
        } finally {
            try {
                close(table);
            } catch (AtlasException e) {
                throw new AtlasBaseException(e);
            }
        }
    }

    @Override
    public List<EntityAuditEventV2> listEventsV2(String entityId, String startKey, short n) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Listing events for entity id {}, starting timestamp {}, #records {}", entityId, startKey, n);
        }

        Table         table   = null;
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
                byte[] entityBytes = getKey(entityId, Long.MAX_VALUE, Integer.MAX_VALUE);
                scan = scan.setStartRow(entityBytes);
            } else {
                scan = scan.setStartRow(Bytes.toBytes(startKey));
            }

            scanner = table.getScanner(scan);
            List<EntityAuditEventV2> events = new ArrayList<>();

            Result result;

            //PageFilter doesn't ensure n results are returned. The filter is per region server.
            //So, adding extra check on n here
            while ((result = scanner.next()) != null && events.size() < n) {
                EntityAuditEventV2 event = fromKeyV2(result.getRow());

                //In case the user sets random start key, guarding against random events
                if (!event.getEntityId().equals(entityId)) {
                    continue;
                }

                event.setUser(getResultString(result, COLUMN_USER));
                event.setAction(EntityAuditActionV2.fromString(getResultString(result, COLUMN_ACTION)));
                event.setDetails(getEntityDetails(result));
                event.setType(getAuditType(result));

                if (persistEntityDefinition) {
                    event.setEntityDefinition(getEntityDefinition(result));
                }

                events.add(event);
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Got events for entity id {}, starting timestamp {}, #records {}", entityId, startKey, events.size());
            }

            return events;
        } catch (IOException e) {
            throw new AtlasBaseException(e);
        } finally {
            try {
                close(scanner);
                close(table);
            } catch (AtlasException e) {
                throw new AtlasBaseException(e);
            }
        }
    }

    private String getEntityDefinition(Result result) throws AtlasBaseException {
        String ret = getResultString(result, COLUMN_DEFINITION);

        if (getAuditType(result) != ENTITY_AUDIT_V2) {
            Referenceable referenceable = AtlasType.fromV1Json(ret, Referenceable.class);
            AtlasEntity   entity        = toAtlasEntity(referenceable);

            ret = AtlasType.toJson(entity);
        }

        return ret;
    }

    private String getEntityDetails(Result result) throws AtlasBaseException {
        String ret;

        if (getAuditType(result) == ENTITY_AUDIT_V2) {
            ret = getResultString(result, COLUMN_DETAIL);
        } else {
            // convert v1 audit detail to v2
            ret = getV2Details(result);
        }

        return ret;
    }

    private EntityAuditType getAuditType(Result result) {
        String          typeString = getResultString(result, COLUMN_TYPE);
        EntityAuditType ret        = (typeString != null) ? EntityAuditType.valueOf(typeString) : ENTITY_AUDIT_V1;

        return ret;
    }

    private String  getV2Details(Result result) throws AtlasBaseException {
        String ret                 = null;
        String v1DetailsWithPrefix = getResultString(result, COLUMN_DETAIL);

        if (StringUtils.isNotEmpty(v1DetailsWithPrefix)) {
            EntityAuditAction v1AuditAction = EntityAuditAction.fromString(getResultString(result, COLUMN_ACTION));

            if (v1AuditAction == TERM_ADD || v1AuditAction == TERM_DELETE) {
                // for terms audit v1 and v2 structure is same
                ret = v1DetailsWithPrefix;
            } else {
                String            v1AuditPrefix = EntityAuditListener.getV1AuditPrefix(v1AuditAction);
                String[]          split         = v1DetailsWithPrefix.split(v1AuditPrefix);

                if (ArrayUtils.isNotEmpty(split) && split.length == 2) {
                    String        v1AuditDetails = split[1];
                    Referenceable referenceable  = AtlasType.fromV1Json(v1AuditDetails, Referenceable.class);
                    String        v2Json         = (referenceable != null) ? toV2Json(referenceable, v1AuditAction) : v1AuditDetails;

                    if (v2Json != null) {
                        ret = getV2AuditPrefix(v1AuditAction) + v2Json;
                    }
                } else {
                    ret = v1DetailsWithPrefix;
                }
            }
        }

        return ret;
    }

    private String toV2Json(Referenceable referenceable, EntityAuditAction action) throws AtlasBaseException {
        String ret;

        if (action == TAG_ADD || action == TAG_UPDATE || action == TAG_DELETE) {
            AtlasClassification classification = instanceConverter.toAtlasClassification(referenceable);

            ret = AtlasType.toJson(classification);
        } else {
            AtlasEntity entity = toAtlasEntity(referenceable);

            ret = AtlasType.toJson(entity);
        }

        return ret;
    }

    private AtlasEntity toAtlasEntity(Referenceable referenceable) throws AtlasBaseException {
        AtlasEntity              ret                 = null;
        AtlasEntitiesWithExtInfo entitiesWithExtInfo = instanceConverter.toAtlasEntity(referenceable);

        if (entitiesWithExtInfo != null && CollectionUtils.isNotEmpty(entitiesWithExtInfo.getEntities())) {
            ret = entitiesWithExtInfo.getEntities().get(0);
        }

        return ret;
    }

    private <T> void addColumn(Put put, byte[] columnName, T columnValue) {
        if (columnValue != null && !columnValue.toString().isEmpty()) {
            put.addColumn(COLUMN_FAMILY, columnName, Bytes.toBytes(columnValue.toString()));
        }
    }

    /**
     * List events for the given entity id in decreasing order of timestamp, from the given startKey. Returns n results
     * @param entityId entity id
     * @param startKey key for the first event to be returned, used for pagination
     * @param n number of events to be returned
     * @return list of events
     * @throws AtlasException
     */
    public List<EntityAuditEvent> listEventsV1(String entityId, String startKey, short n)
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
                byte[] entityBytes = getKey(entityId, Long.MAX_VALUE, Integer.MAX_VALUE);
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
                event.setAction(EntityAuditEvent.EntityAuditAction.fromString(getResultString(result, COLUMN_ACTION)));
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

    private EntityAuditEventV2 fromKeyV2(byte[] keyBytes) {
        String             key   = Bytes.toString(keyBytes);
        EntityAuditEventV2 event = new EntityAuditEventV2();

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
        Properties                           properties = ApplicationProperties.getSubsetAsProperties(atlasConf, CONFIG_PREFIX);
        org.apache.hadoop.conf.Configuration hbaseConf  = HBaseConfiguration.create();

        for (String key : properties.stringPropertyNames()) {
            String value = properties.getProperty(key);

            LOG.info("adding HBase configuration: {}={}", key, value);

            hbaseConf.set(key, value);
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
    public Set<String> getEntitiesWithTagChanges(long fromTimestamp, long toTimestamp) throws AtlasBaseException {
        final String classificationUpdatesAction = "CLASSIFICATION_";

        if (LOG.isDebugEnabled()) {
            LOG.debug("Listing events for fromTimestamp {}, toTimestamp {}, action {}", fromTimestamp, toTimestamp);
        }

        Table table = null;
        ResultScanner scanner = null;

        try {
            Set<String> guids = new HashSet<>();

            table = connection.getTable(tableName);

            byte[] filterValue = Bytes.toBytes(classificationUpdatesAction);
            BinaryPrefixComparator binaryPrefixComparator = new BinaryPrefixComparator(filterValue);
            SingleColumnValueFilter filter = new SingleColumnValueFilter(COLUMN_FAMILY, COLUMN_ACTION, CompareFilter.CompareOp.EQUAL, binaryPrefixComparator);
            Scan scan = new Scan().setFilter(filter).setTimeRange(fromTimestamp, toTimestamp);

            Result result;
            scanner = table.getScanner(scan);
            while ((result = scanner.next()) != null) {
                EntityAuditEvent event = fromKey(result.getRow());

                if (event == null) {
                    continue;
                }

                guids.add(event.getEntityId());
            }

            return guids;
        } catch (IOException e) {
            throw new AtlasBaseException(e);
        } finally {
            try {
                close(scanner);
                close(table);
            } catch (AtlasException e) {
                throw new AtlasBaseException(e);
            }
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

}
