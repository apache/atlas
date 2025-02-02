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
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasException;
import org.apache.atlas.EntityAuditEvent;
import org.apache.atlas.RequestContext;
import org.apache.atlas.annotation.ConditionalOnAtlasProperty;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.ha.HAConfiguration;
import org.apache.atlas.model.audit.EntityAuditEventV2;
import org.apache.atlas.model.audit.EntityAuditEventV2.EntityAuditActionV2;
import org.apache.atlas.repository.Constants.AtlasAuditAgingType;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.inject.Singleton;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

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
@Order(0)
public class HBaseBasedAuditRepository extends AbstractStorageBasedAuditRepository {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseBasedAuditRepository.class);

    public static final String CONFIG_PREFIX                = "atlas.audit";
    public static final String CONFIG_TABLE_NAME            = CONFIG_PREFIX + ".hbase.tablename";
    public static final String DEFAULT_TABLE_NAME           = "ATLAS_ENTITY_AUDIT_EVENTS";
    public static final String CONFIG_COMPRESSION_ALGORITHM = CONFIG_PREFIX + ".hbase.compression-algorithm";
    public static final byte[] COLUMN_FAMILY                = Bytes.toBytes("dt");
    public static final byte[] COLUMN_ACTION                = Bytes.toBytes("a");
    public static final byte[] COLUMN_DETAIL                = Bytes.toBytes("d");
    public static final byte[] COLUMN_USER                  = Bytes.toBytes("u");
    public static final byte[] COLUMN_DEFINITION            = Bytes.toBytes("f");

    private static final String  HBASE_STORE_COMPRESSION_PROPERTY   = "atlas.graph.storage.hbase.compression-algorithm";
    private static final String  AUDIT_REPOSITORY_MAX_SIZE_PROPERTY = "atlas.hbase.client.keyvalue.maxsize";
    private static final String  AUDIT_EXCLUDE_ATTRIBUTE_PROPERTY   = "atlas.audit.hbase.entity";
    private static final String  FIELD_SEPARATOR                    = ":";
    private static final long    ATLAS_HBASE_KEYVALUE_DEFAULT_SIZE  = 1024 * 1024;
    private static final int     DEFAULT_CACHING                    = 200;
    private static final String  SORT_BY_COLUMN                     = EntityAuditEventV2.SORT_COLUMN_TIMESTAMP;
    private static final boolean SORT_ORDER_DESC                    = true;

    private final Map<String, List<String>> auditExcludedAttributesCache = new HashMap<>();
    private       TableName                 tableName;
    private       String                    compressionType;
    private       Connection                connection;

    /**
     * Converts atlas' application properties to hadoop conf
     *
     * @param atlasConf
     * @return
     */
    public static org.apache.hadoop.conf.Configuration getHBaseConfiguration(Configuration atlasConf) {
        Properties                           properties = ApplicationProperties.getSubsetAsProperties(atlasConf, CONFIG_PREFIX);
        org.apache.hadoop.conf.Configuration hbaseConf  = HBaseConfiguration.create();

        for (String key : properties.stringPropertyNames()) {
            String value = properties.getProperty(key);

            LOG.info("adding HBase configuration: {}={}", key, value);

            hbaseConf.set(key, value);
        }

        return hbaseConf;
    }

    /**
     * Add events to the event repository
     *
     * @param events events to be added
     * @throws AtlasException
     */
    @Override
    public void putEventsV1(List<EntityAuditEvent> events) throws AtlasException {
        LOG.debug("Putting {} events", events.size());

        Table table = null;

        try {
            table = connection.getTable(tableName);

            List<Put> puts = new ArrayList<>(events.size());

            for (int index = 0; index < events.size(); index++) {
                EntityAuditEvent event = events.get(index);

                LOG.debug("Adding entity audit event {}", event);

                Put put = new Put(getKey(event.getEntityId(), event.getTimestamp(), index));

                addColumn(put, COLUMN_ACTION, event.getAction());
                addColumn(put, COLUMN_USER, event.getUser());
                addColumn(put, COLUMN_DETAIL, event.getDetails());

                if (PERSIST_ENTITY_DEFINITION) {
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

    /**
     * List events for the given entity id in decreasing order of timestamp, from the given startKey. Returns n results
     *
     * @param entityId entity id
     * @param startKey key for the first event to be returned, used for pagination
     * @param n number of events to be returned
     * @return list of events
     * @throws AtlasException
     */
    public List<EntityAuditEvent> listEventsV1(String entityId, String startKey, short n) throws AtlasException {
        LOG.debug("Listing events for entity id {}, starting timestamp {}, #records {}", entityId, startKey, n);

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

            Result                 result;
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

                if (PERSIST_ENTITY_DEFINITION) {
                    String colDef = getResultString(result, COLUMN_DEFINITION);

                    if (colDef != null) {
                        event.setEntityDefinition(colDef);
                    }
                }

                events.add(event);
            }

            LOG.debug("Got events for entity id {}, starting timestamp {}, #records {}", entityId, startKey, events.size());

            return events;
        } catch (IOException e) {
            throw new AtlasException(e);
        } finally {
            close(scanner);
            close(table);
        }
    }

    @Override
    public void putEventsV2(List<EntityAuditEventV2> events) throws AtlasBaseException {
        LOG.debug("Putting {} events", events.size());

        Table table = null;

        try {
            table = connection.getTable(tableName);

            List<Put> puts = new ArrayList<>(events.size());

            for (int index = 0; index < events.size(); index++) {
                EntityAuditEventV2 event = events.get(index);

                LOG.debug("Adding entity audit event {}", event);

                Put put = new Put(getKey(event.getEntityId(), event.getTimestamp(), index));

                addColumn(put, COLUMN_ACTION, event.getAction());
                addColumn(put, COLUMN_USER, event.getUser());
                addColumn(put, COLUMN_DETAIL, event.getDetails());

                if (PERSIST_ENTITY_DEFINITION) {
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
    public List<EntityAuditEventV2> listEventsV2(String entityId, EntityAuditActionV2 auditAction, String startKey, short maxResultCount) throws AtlasBaseException {
        LOG.debug("Listing events for entity id {}, operation {}, starting key{}, maximum result count {}", entityId, auditAction, startKey, maxResultCount);

        AtlasPerfMetrics.MetricRecorder metric  = RequestContext.get().startMetricRecord("listSortedEventsV2");
        Table                           table   = null;
        ResultScanner                   scanner = null;

        try {
            table = connection.getTable(tableName);

            /**
             * Scan Details:
             * In hbase, the events are stored in increasing order of timestamp. So, doing reverse scan to get the latest event first
             * Page filter is set to limit the number of results returned if needed
             * Stop row is set to the entity id to avoid going past the current entity while scanning
             * SingleColumnValueFilter is been used to match the operation at COLUMN_FAMILY->COLUMN_ACTION
             * Small is set to true to optimise RPC calls as the scanner is created per request
             * setCaching(DEFAULT_CACHING) will increase the payload size to DEFAULT_CACHING rows per remote call and
             *  both types of next() take these settings into account.
             */
            Scan scan = new Scan().setReversed(true)
                    .setCaching(DEFAULT_CACHING)
                    .setSmall(true);

            if (maxResultCount > -1) {
                scan.setFilter(new PageFilter(maxResultCount));
            }

            if (auditAction != null) {
                Filter filterAction = new SingleColumnValueFilter(COLUMN_FAMILY, COLUMN_ACTION, CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(auditAction.toString())));

                scan.setFilter(filterAction);
            }

            if (StringUtils.isNotBlank(entityId)) {
                scan.setStopRow(Bytes.toBytes(entityId));
            }

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

            //PageFilter doesn't ensure maxResultCount results are returned. The filter is per region server.
            //So, adding extra check on maxResultCount
            while ((result = scanner.next()) != null && (maxResultCount == -1 || events.size() < maxResultCount)) {
                EntityAuditEventV2 event = fromKeyV2(result.getRow());

                //In case the user sets random start key, guarding against random events if entityId is provided
                if (StringUtils.isNotBlank(entityId) && !event.getEntityId().equals(entityId)) {
                    continue;
                }

                event.setUser(getResultString(result, COLUMN_USER));
                event.setAction(EntityAuditActionV2.fromString(getResultString(result, COLUMN_ACTION)));
                event.setDetails(getResultString(result, COLUMN_DETAIL));

                if (PERSIST_ENTITY_DEFINITION) {
                    String colDef = getResultString(result, COLUMN_DEFINITION);

                    if (colDef != null) {
                        event.setEntityDefinition(colDef);
                    }
                }

                events.add(event);
            }

            LOG.debug("Got events for entity id {}, operation {}, starting key{}, maximum result count {}, #records returned {}", entityId, auditAction, startKey, maxResultCount, events.size());

            return events;
        } catch (IOException e) {
            throw new AtlasBaseException(e);
        } finally {
            try {
                close(scanner);
                close(table);
                RequestContext.get().endMetricRecord(metric);
            } catch (AtlasException e) {
                throw new AtlasBaseException(e);
            }
        }
    }

    @Override
    public List<EntityAuditEventV2> listEventsV2(String entityId, EntityAuditEventV2.EntityAuditActionV2 auditAction, String sortByColumn, boolean sortOrderDesc, int offset, short limit) throws AtlasBaseException {
        return listEventsV2(entityId, auditAction, sortByColumn, sortOrderDesc, offset, limit, false, true, false, null);
    }

    @Override
    public Set<String> getEntitiesWithTagChanges(long fromTimestamp, long toTimestamp) throws AtlasBaseException {
        final String classificationUpdatesAction = "CLASSIFICATION_";

        LOG.debug("Listing events for fromTimestamp {}, toTimestamp {}", fromTimestamp, toTimestamp);

        Table         table   = null;
        ResultScanner scanner = null;

        try {
            Set<String> guids = new HashSet<>();

            table = connection.getTable(tableName);

            byte[]                  filterValue            = Bytes.toBytes(classificationUpdatesAction);
            BinaryPrefixComparator  binaryPrefixComparator = new BinaryPrefixComparator(filterValue);
            SingleColumnValueFilter filter                 = new SingleColumnValueFilter(COLUMN_FAMILY, COLUMN_ACTION, CompareFilter.CompareOp.EQUAL, binaryPrefixComparator);
            Scan                    scan                   = new Scan().setFilter(filter).setTimeRange(fromTimestamp, toTimestamp);

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

    @Override
    public int getHandlerOrder() {
        return HandlerOrder.AUDIT_REPOSITORY.getOrder();
    }

    /**
     * Add events to the event repository
     *
     * @param events events to be added
     * @throws AtlasException
     */
    @Override
    public void putEventsV1(EntityAuditEvent... events) throws AtlasException {
        putEventsV1(Arrays.asList(events));
    }

    @Override
    public void putEventsV2(EntityAuditEventV2... events) throws AtlasBaseException {
        putEventsV2(Arrays.asList(events));
    }

    @Override
    public List<Object> listEvents(String entityId, String startKey, short maxResults) throws AtlasBaseException {
        List ret = listEventsV2(entityId, null, startKey, maxResults);

        try {
            if (CollectionUtils.isEmpty(ret)) {
                ret = listEventsV1(entityId, startKey, maxResults);
            }
        } catch (AtlasException e) {
            throw new AtlasBaseException(e);
        }

        return ret;
    }

    @Override
    public long repositoryMaxSize() {
        long ret;

        initApplicationProperties();

        if (applicationProperties == null) {
            ret = ATLAS_HBASE_KEYVALUE_DEFAULT_SIZE;
        } else {
            ret = applicationProperties.getLong(AUDIT_REPOSITORY_MAX_SIZE_PROPERTY, ATLAS_HBASE_KEYVALUE_DEFAULT_SIZE);
        }

        return ret;
    }

    @Override
    public List<String> getAuditExcludeAttributes(String entityType) {
        List<String> ret = null;

        initApplicationProperties();

        if (auditExcludedAttributesCache.containsKey(entityType)) {
            ret = auditExcludedAttributesCache.get(entityType);
        } else if (applicationProperties != null) {
            String[] excludeAttributes = applicationProperties.getStringArray(AUDIT_EXCLUDE_ATTRIBUTE_PROPERTY + "." + entityType + "." + "attributes.exclude");

            if (excludeAttributes != null) {
                ret = Arrays.asList(excludeAttributes);
            }

            auditExcludedAttributesCache.put(entityType, ret);
        }

        return ret;
    }

    @Override
    public List<EntityAuditEventV2> deleteEventsV2(String entityId, Set<EntityAuditEventV2.EntityAuditActionV2> entityAuditActions, short allowedAuditCount, int ttlInDays, boolean createEventsAgeoutAllowed, AtlasAuditAgingType auditAgingType) throws AtlasBaseException, AtlasException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseBasedAuditRepository.deleteEventsV2(entityId={}, auditActions={}, sortByColumn={}, sortOrderDesc={}, offset={}, limit={})",
                    entityId, Arrays.toString(entityAuditActions.toArray()), SORT_BY_COLUMN, SORT_ORDER_DESC, allowedAuditCount, -1);
        }

        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("deleteEventsV2");

        Table                    table                   = null;
        List<EntityAuditEventV2> eventsEligibleForAgeout = new ArrayList<>();

        try {
            table = connection.getTable(tableName);

            List<EntityAuditEventV2> eventsToKeep            = new ArrayList<>();
            boolean                  allowAgeoutByAuditCount = allowedAuditCount > 0 || (auditAgingType == AtlasAuditAgingType.SWEEP);

            if (CollectionUtils.isEmpty(entityAuditActions)) {
                eventsEligibleForAgeout.addAll(listEventsV2(entityId, null, SORT_BY_COLUMN, SORT_ORDER_DESC, allowedAuditCount, (short) -1, true, createEventsAgeoutAllowed, allowAgeoutByAuditCount, eventsToKeep));
            } else {
                for (EntityAuditActionV2 eachAuditAction : entityAuditActions) {
                    List<EntityAuditEventV2> eventsByEachAuditAction = listEventsV2(entityId, eachAuditAction, SORT_BY_COLUMN, SORT_ORDER_DESC, allowedAuditCount, (short) -1, true, createEventsAgeoutAllowed, allowAgeoutByAuditCount, eventsToKeep);

                    if (CollectionUtils.isNotEmpty(eventsByEachAuditAction)) {
                        eventsEligibleForAgeout.addAll(eventsByEachAuditAction);
                    }
                }
            }

            if (CollectionUtils.isNotEmpty(eventsToKeep)) {
                //Limit events based on configured audit count by grouping events of all action types
                if (allowAgeoutByAuditCount && (auditAgingType == AtlasAuditAgingType.DEFAULT || CollectionUtils.isEmpty(entityAuditActions))) {
                    LOG.debug("Aging out audit events by audit count for entity: {}", entityId);

                    EntityAuditEventV2.sortEvents(eventsToKeep, SORT_BY_COLUMN, SORT_ORDER_DESC);

                    if (allowedAuditCount < eventsToKeep.size()) {
                        eventsEligibleForAgeout.addAll(eventsToKeep.subList(allowedAuditCount, eventsToKeep.size()));

                        eventsToKeep = eventsToKeep.subList(0, allowedAuditCount);
                    }
                }
                //TTL based aging
                LocalDateTime now                 = LocalDateTime.now();
                boolean       isTTLTestAutomation = AtlasConfiguration.ATLAS_AUDIT_AGING_TTL_TEST_AUTOMATION.getBoolean();

                if (ttlInDays > 0) {
                    LOG.debug("Aging out audit events by TTL for entity: {}", entityId);

                    long ttlTimestamp = Timestamp.valueOf(isTTLTestAutomation ? now.minusMinutes(ttlInDays) : now.minusDays(ttlInDays)).getTime();

                    eventsToKeep.forEach(e -> {
                        if (e.getTimestamp() < ttlTimestamp) {
                            eventsEligibleForAgeout.add(e);
                        }
                    });
                }
            }

            List<Delete> eventsToDelete = new ArrayList<>();

            for (EntityAuditEventV2 event : eventsEligibleForAgeout) {
                Delete delete = new Delete(Bytes.toBytes(event.getEventKey()));

                eventsToDelete.add(delete);
            }

            if (CollectionUtils.isNotEmpty(eventsToDelete)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Deleting events from table:{} are {}", tableName, Arrays.toString(eventsToDelete.toArray()));
                }

                table.delete(eventsToDelete);
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== HBaseBasedAuditRepository.deleteEventsV2(entityId={}, auditAction={}, sortByColumn={}, sortOrderDesc={}, offset={}, limit={}): ",
                        entityId, Arrays.toString(entityAuditActions.toArray()), SORT_BY_COLUMN, SORT_ORDER_DESC, allowedAuditCount, -1);
            }
        } catch (IOException e) {
            LOG.error("Failed deleting audit events for guid:{}", entityId);
        } finally {
            RequestContext.get().endMetricRecord(metric);
            close(table);
        }

        return eventsEligibleForAgeout;
    }

    @VisibleForTesting
    protected Connection createConnection(org.apache.hadoop.conf.Configuration hbaseConf) throws IOException {
        return ConnectionFactory.createConnection(hbaseConf);
    }

    @VisibleForTesting
    void startInternal(Configuration atlasConf, org.apache.hadoop.conf.Configuration hbaseConf) throws AtlasException {
        String tableNameStr = atlasConf.getString(CONFIG_TABLE_NAME, DEFAULT_TABLE_NAME);

        tableName = TableName.valueOf(tableNameStr);

        compressionType = atlasConf.getString(CONFIG_COMPRESSION_ALGORITHM, atlasConf.getString(HBASE_STORE_COMPRESSION_PROPERTY, "GZ"));

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

    private List<EntityAuditEventV2> listEventsV2(String entityId, EntityAuditEventV2.EntityAuditActionV2 auditAction, String sortByColumn, boolean sortOrderDesc, int offset, short limit, boolean isAgeoutTransaction, boolean createEventsAgeoutAllowed, boolean allowAgeoutByAuditCount, List<EntityAuditEventV2> eventsToKeep) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseBasedAuditRepository.listEventsV2(entityId={}, auditAction={}, sortByColumn={}, sortOrderDesc={}, offset={}, limit={})",
                    entityId, auditAction, sortByColumn, sortOrderDesc, offset, limit);
        }

        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("listEventsV2");

        if (sortByColumn == null) {
            sortByColumn = EntityAuditEventV2.SORT_COLUMN_TIMESTAMP;
        }

        if (offset < 0) {
            offset = 0;
        }

        if (!isAgeoutTransaction && limit < 0) {
            limit = 100;
        }

        try (Table table = connection.getTable(tableName)) {
            /*
             * HBase Does not support query with sorted results. To support this API inmemory sort has to be performed.
             * Audit entry can potentially have entire entity dumped into it. Loading entire audit entries for an entity can be
             * memory intensive. Therefore we load audit entries with limited columns first, perform sort on this light weight list,
             * then get the relevant section by removing offsets and reducing to limits. With this reduced list we create
             * MultiRowRangeFilter and then again scan the table to get all the columns from the table this time.
             */
            Scan scan = new Scan().setReversed(true)
                    .setCaching(DEFAULT_CACHING)
                    .setSmall(true)
                    .setStopRow(Bytes.toBytes(entityId))
                    .setStartRow(getKey(entityId, Long.MAX_VALUE, Integer.MAX_VALUE))
                    .addColumn(COLUMN_FAMILY, COLUMN_ACTION)
                    .addColumn(COLUMN_FAMILY, COLUMN_USER);

            FilterList filterList = new FilterList();

            if (auditAction != null) {
                Filter filterAction = new SingleColumnValueFilter(COLUMN_FAMILY, COLUMN_ACTION, CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(auditAction.toString())));

                filterList.addFilter(filterAction);
            }

            if (!createEventsAgeoutAllowed) {
                FilterList createEventFilterList          = new FilterList(FilterList.Operator.MUST_PASS_ALL);
                Filter     filterByCreateActionType       = new SingleColumnValueFilter(COLUMN_FAMILY, COLUMN_ACTION, CompareFilter.CompareOp.NOT_EQUAL, new BinaryComparator(Bytes.toBytes(EntityAuditActionV2.ENTITY_CREATE.toString())));
                Filter     filterByImportCreateActionType = new SingleColumnValueFilter(COLUMN_FAMILY, COLUMN_ACTION, CompareFilter.CompareOp.NOT_EQUAL, new BinaryComparator(Bytes.toBytes(EntityAuditActionV2.ENTITY_IMPORT_CREATE.toString())));

                createEventFilterList.addFilter(filterByCreateActionType);
                createEventFilterList.addFilter(filterByImportCreateActionType);

                filterList.addFilter(createEventFilterList);
            }

            scan.setFilter(filterList);

            List<EntityAuditEventV2> events = new ArrayList<>();

            try (ResultScanner scanner = table.getScanner(scan)) {
                for (Result result = scanner.next(); result != null; result = scanner.next()) {
                    EntityAuditEventV2 event = fromKeyV2(result.getRow());

                    event.setUser(getResultString(result, COLUMN_USER));
                    event.setAction(EntityAuditActionV2.fromString(getResultString(result, COLUMN_ACTION)));

                    events.add(event);
                }
            }

            EntityAuditEventV2.sortEvents(events, sortByColumn, sortOrderDesc);

            int fromIndex = Math.min(events.size(), offset);
            int endIndex  = events.size();

            if (limit > 0) {
                endIndex = Math.min(events.size(), offset + limit);
            }

            if (isAgeoutTransaction) {
                if (!allowAgeoutByAuditCount) { //No audit events allowed to age-out by audit count
                    eventsToKeep.addAll(events);

                    return Collections.emptyList();
                }

                eventsToKeep.addAll(events.subList(0, fromIndex));
            }

            events = events.subList(fromIndex, endIndex);

            if (!events.isEmpty()) {
                List<MultiRowRangeFilter.RowRange> ranges = new ArrayList<>();

                events.forEach(e -> ranges.add(new MultiRowRangeFilter.RowRange(e.getEventKey(), true, e.getEventKey(), true)));

                scan = new Scan().setReversed(true)
                        .setCaching(DEFAULT_CACHING)
                        .setSmall(true)
                        .setStopRow(Bytes.toBytes(entityId))
                        .setStartRow(getKey(entityId, Long.MAX_VALUE, Integer.MAX_VALUE))
                        .setFilter(new MultiRowRangeFilter(ranges));

                try (ResultScanner scanner = table.getScanner(scan)) {
                    events = new ArrayList<>();

                    for (Result result = scanner.next(); result != null; result = scanner.next()) {
                        EntityAuditEventV2 event = fromKeyV2(result.getRow());

                        event.setUser(getResultString(result, COLUMN_USER));
                        event.setAction(EntityAuditActionV2.fromString(getResultString(result, COLUMN_ACTION)));
                        event.setDetails(getResultString(result, COLUMN_DETAIL));

                        if (PERSIST_ENTITY_DEFINITION) {
                            String colDef = getResultString(result, COLUMN_DEFINITION);

                            if (colDef != null) {
                                event.setEntityDefinition(colDef);
                            }
                        }

                        events.add(event);
                    }
                }

                EntityAuditEventV2.sortEvents(events, sortByColumn, sortOrderDesc);
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== HBaseBasedAuditRepository.listEventsV2(entityId={}, auditAction={}, sortByColumn={}, sortOrderDesc={}, offset={}, limit={}): #records returned {}",
                        entityId, auditAction, sortByColumn, sortOrderDesc, offset, limit, events.size());
            }

            return events;
        } catch (IOException e) {
            throw new AtlasBaseException(e);
        } finally {
            RequestContext.get().endMetricRecord(metric);
        }
    }

    private <T> void addColumn(Put put, byte[] columnName, T columnValue) {
        if (columnValue != null && !columnValue.toString().isEmpty()) {
            put.addColumn(COLUMN_FAMILY, columnName, Bytes.toBytes(columnValue.toString()));
        }
    }

    private byte[] getKey(String id, Long ts) {
        String keyStr = id + FIELD_SEPARATOR + ts;

        return Bytes.toBytes(keyStr);
    }

    private String getResultString(Result result, byte[] columnName) {
        byte[] rawValue = result.getValue(COLUMN_FAMILY, columnName);

        if (rawValue != null) {
            return Bytes.toString(rawValue);
        }

        return null;
    }

    private EntityAuditEvent fromKey(byte[] keyBytes) {
        String           key   = Bytes.toString(keyBytes);
        EntityAuditEvent event = new EntityAuditEvent();

        if (StringUtils.isNotEmpty(key)) {
            String[] parts = key.split(FIELD_SEPARATOR);

            event.setEntityId(parts[0]);
            event.setTimestamp(Long.parseLong(parts[1]));
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
            event.setTimestamp(Long.parseLong(parts[1]));
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

    private void createTableIfNotExists() throws AtlasException {
        Admin admin = null;

        try {
            admin = connection.getAdmin();

            LOG.info("Checking if table {} exists", tableName.getNameAsString());

            if (!admin.tableExists(tableName)) {
                LOG.info("Creating table {}", tableName.getNameAsString());

                HTableDescriptor  tableDescriptor = new HTableDescriptor(tableName);
                HColumnDescriptor columnFamily    = new HColumnDescriptor(COLUMN_FAMILY);

                columnFamily.setMaxVersions(1);
                columnFamily.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
                columnFamily.setCompressionType(Compression.Algorithm.valueOf(compressionType));
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
}
