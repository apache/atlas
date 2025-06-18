/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.janusgraph.diskstorage.rdbms;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.EntryMetaData;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRangeQuery;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.KeySlicesIterator;
import org.janusgraph.diskstorage.keycolumnvalue.MultiSlicesQuery;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.rdbms.dao.DaoManager;
import org.janusgraph.diskstorage.rdbms.dao.JanusColumnDao;
import org.janusgraph.diskstorage.rdbms.dao.JanusKeyDao;
import org.janusgraph.diskstorage.rdbms.dao.JanusStoreDao;
import org.janusgraph.diskstorage.rdbms.entity.JanusKey;
import org.janusgraph.diskstorage.rdbms.entity.JanusStore;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.diskstorage.util.StaticArrayEntryList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * KeyColumnValue store backed by RDBMS
 *
 * @author Madhan Neethiraj &lt;madhan@apache.org&gt;
 */
public class RdbmsStore implements KeyColumnValueStore {
    private static final Logger LOG = LoggerFactory.getLogger(RdbmsStore.class);

    private final String          name;
    private final DaoManager      daoManager;
    private final EntryMetaData[] entryMetaData;
    private       Long            storeId;

    public RdbmsStore(String name, RdbmsStoreManager storeManager) {
        LOG.info("RdbmsStore(name={})", name);

        this.name          = name;
        this.daoManager    = storeManager.getDaoManager();
        this.entryMetaData = storeManager.getMetaDataSchema(name);
        this.storeId       = null;
    }

    @Override
    public EntryList getSlice(KeySliceQuery query, StoreTransaction trx) throws BackendException {
        LOG.debug("==> RdbmsStore.getSlice(name={}, query={}, trx={})", name, query, trx);

        final EntryList ret;

        if (isStorePresent(trx)) {
            JanusColumnDao         dao        = new JanusColumnDao((RdbmsTransaction) trx, this);
            Long                   keyId      = getKeyIdOrCreate(toBytes(query.getKey()), trx);
            byte[]                 sliceStart = toBytes(query.getSliceStart());
            byte[]                 sliceEnd   = toBytes(query.getSliceEnd());
            List<JanusColumnValue> entries    = dao.getColumns(keyId, sliceStart, sliceEnd, query.getLimit());

            if (entries != null && !entries.isEmpty()) {
                ret = StaticArrayEntryList.ofStaticBuffer(entries, toEntry);
            } else {
                ret = EntryList.EMPTY_LIST;
            }
        } else {
            ret = EntryList.EMPTY_LIST;
        }

        LOG.debug("<== RdbmsStore.getSlice(name={}, query={}, trx={}): ret={}", name, query, trx, ret.size());

        return ret;
    }

    @Override
    public Map<StaticBuffer, EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction trx) throws BackendException {
        LOG.debug("==> RdbmsStore.getSlice(name={}, len(keys)={}, query={}, trx={})", name, keys.size(), query, trx);

        final Map<StaticBuffer, EntryList> ret;

        if (isStorePresent(trx)) {
            ret = new TreeMap<>();

            for (StaticBuffer key : keys) {
                ret.put(key, getSlice(new KeySliceQuery(key, query), trx));
            }
        } else {
            ret = Collections.emptyMap();
        }

        LOG.debug("<== RdbmsStore.getSlice(name={}, len(keys)={}, query={}, trx={})", name, keys.size(), query, trx);

        return ret;
    }

    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction trx) throws BackendException {
        LOG.debug("==> RdbmsStore.mutate(name={}, key={}, additions={}, deletions={}, trx={})", name, key, additions, deletions, trx);

        byte[]         keyName   = toBytes(key);
        Long           keyId     = getKeyIdOrCreate(keyName, trx);
        JanusColumnDao columnDao = new JanusColumnDao((RdbmsTransaction) trx, this);

        for (StaticBuffer column : deletions) {
            byte[] columnName = toBytes(column);

            columnDao.remove(keyId, columnName);
        }

        for (Entry entry : additions) {
            columnDao.addOrUpdate(keyId, toBytes(entry.getColumn()), toBytes(entry.getValue()));
        }

        LOG.debug("<== RdbmsStore.mutate(name={}, key={}, additions={}, deletions={}, trx={})", name, key, additions, deletions, trx);
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue, StoreTransaction trx) throws BackendException {
        throw new UnsupportedOperationException();
    }

    @Override
    public KeyIterator getKeys(KeyRangeQuery query, StoreTransaction trx) throws BackendException {
        LOG.debug("==> RdbmsStore.getKeys(name={}, query={}, trx={})", name, query, trx);

        final KeyIterator ret;

        if (isStorePresent(trx)) {
            JanusColumnDao dao = new JanusColumnDao((RdbmsTransaction) trx, this);

            ret = dao.getKeysByKeyAndColumnRange(this.storeId, toBytes(query.getKeyStart()), toBytes(query.getKeyEnd()), toBytes(query.getSliceStart()), toBytes(query.getSliceEnd()), query.getLimit());
        } else {
            ret = JanusColumnDao.EMPTY_KEY_ITERATOR;
        }

        LOG.debug("<== RdbmsStore.debug(name={}, query={}, trx={})", name, query, trx);

        return ret;
    }

    @Override
    public KeyIterator getKeys(SliceQuery query, StoreTransaction trx) throws BackendException {
        LOG.debug("==> RdbmsStore.getKeys(name={}, query={}, trx={})", name, query, trx);

        final KeyIterator ret;

        if (isStorePresent(trx)) {
            JanusColumnDao dao  = new JanusColumnDao((RdbmsTransaction) trx, this);

            ret = dao.getKeysByColumnRange(this.storeId, toBytes(query.getSliceStart()), toBytes(query.getSliceEnd()), query.getLimit());
        } else {
            ret = JanusColumnDao.EMPTY_KEY_ITERATOR;
        }

        LOG.debug("<== RdbmsStore.debug(name={}, query={}, trx={})", name, query, trx);

        return ret;
    }

    @Override
    public KeySlicesIterator getKeys(MultiSlicesQuery query, StoreTransaction trx) throws BackendException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void close() throws BackendException {
        LOG.debug("==> RdbmsStore.close(name={})", name);

        LOG.debug("<== RdbmsStore.close(name={})", name);
    }

    private boolean isStorePresent(StoreTransaction trx) {
        if (this.storeId == null) {
            JanusStoreDao storeDao = new JanusStoreDao((RdbmsTransaction) trx);

            this.storeId = storeDao.getIdByName(name);

            return this.storeId != null;
        }

        return true;
    }

    private static byte[] toBytes(StaticBuffer val) {
        return val == null ? null : val.as(StaticBuffer.ARRAY_FACTORY);
    }

    private Long getStoreIdOrCreate(StoreTransaction trx) {
        Long ret = this.storeId;

        while (ret == null) {
            JanusStoreDao dao = new JanusStoreDao((RdbmsTransaction) trx);

            ret = dao.getIdByName(name);

            if (ret == null) {
                RdbmsTransaction trx2 = new RdbmsTransaction(trx.getConfiguration(), daoManager);
                JanusStoreDao    dao2 = new JanusStoreDao(trx2);

                try {
                    LOG.debug("Creating store={}", name);

                    dao2.create(new JanusStore(name));

                    trx2.commit();

                    ret = dao.getIdByName(name);

                    this.storeId = ret;

                    LOG.debug("Created store={}: id={}", name, ret);
                } catch (Exception excp) {
                    LOG.warn("Failed to create store={}", name, excp);
                } finally {
                    try {
                        trx2.close();
                    } catch (Exception excp) {
                        // ignore
                    }
                }
            }
        }

        return ret;
    }

    private Long getKeyIdOrCreate(byte[] key, StoreTransaction trx) {
        Long        ret     = null;
        JanusKeyDao dao     = new JanusKeyDao((RdbmsTransaction) trx);
        Long        storeId = getStoreIdOrCreate(trx);

        while (ret == null) {
            ret = dao.getIdByStoreIdAndName(storeId, key);

            if (ret == null) {
                RdbmsTransaction trx2 = new RdbmsTransaction(trx.getConfiguration(), daoManager);
                JanusKeyDao      dao2 = new JanusKeyDao(trx2);

                try {
                    LOG.debug("Creating key: storeId={}, key={}", storeId, key);

                    dao2.create(new JanusKey(storeId, key));

                    trx2.commit();

                    ret = dao.getIdByStoreIdAndName(storeId, key);

                    LOG.debug("Created key: storeId={}, key={}: id={}", storeId, key, ret);
                } catch (Exception excp) {
                    LOG.warn("Failed to create key: storeId={}, key={}", storeId, key, excp);
                } finally {
                    try {
                        trx2.close();
                    } catch (Exception excp) {
                        // ignore
                    }
                }
            }
        }

        return ret;
    }

    public final StaticArrayEntry.GetColVal<JanusColumnValue, StaticBuffer> toEntry =
            new StaticArrayEntry.GetColVal<JanusColumnValue, StaticBuffer>() {
                @Override
                public StaticBuffer getColumn(JanusColumnValue columnValue) {
                    return columnValue.getColumnAsStaticBuffer();
                }

                @Override
                public StaticBuffer getValue(JanusColumnValue columnValue) {
                    return columnValue.getValueAsStaticBuffer();
                }

                @Override
                public EntryMetaData[] getMetaSchema(JanusColumnValue janusColumnValue) {
                    return entryMetaData;
                }

                @Override
                public Object getMetaData(JanusColumnValue janusColumnValue, EntryMetaData entryMetaData) {
                    return new UnsupportedOperationException();
                }
            };
}
