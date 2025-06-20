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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * KeyColumnValue store backed by RDBMS
 *
 */
public class RdbmsStore implements KeyColumnValueStore {
    private static final Logger LOG = LoggerFactory.getLogger(RdbmsStore.class);

    private static final int STORE_CREATE_MAX_ATTEMPTS   = 10;
    private static final int STORE_CREATE_RETRY_DELAY_MS = 100;
    private static final int KEY_CREATE_MAX_ATTEMPTS     = 10;
    private static final int KEY_CREATE_RETRY_DELAY_MS   = 100;

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
    public EntryList getSlice(KeySliceQuery query, StoreTransaction trx) {
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
    public Map<StaticBuffer, EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction trx) {
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

        LOG.debug("<== RdbmsStore.getSlice(name={}, len(keys)={}, query={}, trx={}): ret={}", name, keys.size(), query, trx, ret);

        return ret;
    }

    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction trx) {
        LOG.debug("==> RdbmsStore.mutate(name={}, key={}, additions={}, deletions={}, trx={})", name, key, additions, deletions, trx);

        byte[]         keyName   = toBytes(key);
        long           keyId     = getKeyIdOrCreate(keyName, trx);
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
    public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue, StoreTransaction trx) {
        LOG.debug("RdbmsStore.acquireLock(key={}, column={}, expectedValue={}, trx={}): UnsupportedOperation", key, column, expectedValue, trx);

        throw new UnsupportedOperationException();
    }

    @Override
    public KeyIterator getKeys(KeyRangeQuery query, StoreTransaction trx) {
        LOG.debug("==> RdbmsStore.getKeys(name={}, query={}, trx={})", name, query, trx);

        final KeyIterator ret;

        if (isStorePresent(trx)) {
            JanusColumnDao dao = new JanusColumnDao((RdbmsTransaction) trx, this);

            ret = dao.getKeysByKeyAndColumnRange(this.storeId, toBytes(query.getKeyStart()), toBytes(query.getKeyEnd()), toBytes(query.getSliceStart()), toBytes(query.getSliceEnd()), query.getLimit());
        } else {
            ret = JanusColumnDao.EMPTY_KEY_ITERATOR;
        }

        LOG.debug("<== RdbmsStore.debug(name={}, query={}, trx={}): ret={}", name, query, trx, ret);

        return ret;
    }

    @Override
    public KeyIterator getKeys(SliceQuery query, StoreTransaction trx) {
        LOG.debug("==> RdbmsStore.getKeys(name={}, query={}, trx={})", name, query, trx);

        final KeyIterator ret;

        if (isStorePresent(trx)) {
            JanusColumnDao dao  = new JanusColumnDao((RdbmsTransaction) trx, this);

            ret = dao.getKeysByColumnRange(this.storeId, toBytes(query.getSliceStart()), toBytes(query.getSliceEnd()), query.getLimit());
        } else {
            ret = JanusColumnDao.EMPTY_KEY_ITERATOR;
        }

        LOG.debug("<== RdbmsStore.debug(name={}, query={}, trx={}): ret={}", name, query, trx, ret);

        return ret;
    }

    @Override
    public KeySlicesIterator getKeys(MultiSlicesQuery query, StoreTransaction trx) {
        LOG.debug("RdbmsStore.getKeys(query={}, trx={}): UnsupportedOperation", query, trx);

        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void close() throws BackendException {
        LOG.debug("RdbmsStore.close(name={})", name);
    }

    private boolean isStorePresent(StoreTransaction trx) {
        Long storeId = this.storeId;

        if (storeId == null) {
            JanusStoreDao storeDao = new JanusStoreDao((RdbmsTransaction) trx);

            storeId = storeDao.getIdByName(name);

            if (storeId != null) {
                this.storeId = storeId;
            }
        }

        return storeId != null;
    }

    private static byte[] toBytes(StaticBuffer val) {
        return val == null ? null : val.as(StaticBuffer.ARRAY_FACTORY);
    }

    private Long getStoreIdOrCreate(StoreTransaction trx) {
        Long ret = this.storeId;

        if (ret == null) {
            JanusStoreDao dao = new JanusStoreDao((RdbmsTransaction) trx);

            ret = dao.getIdByName(name);

            for (int attempt = 1; ret == null; attempt++) {
                try (RdbmsTransaction trx2 = new RdbmsTransaction(trx.getConfiguration(), daoManager)) {
                    JanusStoreDao dao2  = new JanusStoreDao(trx2);
                    JanusStore    store = dao2.create(new JanusStore(name));

                    trx2.commit();

                    ret = store != null ? store.getId() : null;

                    LOG.debug("attempt #{}: created store(name={}): id={}", attempt, name, ret);
                } catch (IOException excp) {
                    LOG.error("attempt #{}: failed to create store(name={})", attempt, name, excp);
                }

                if (ret != null || attempt >= STORE_CREATE_MAX_ATTEMPTS) {
                    break;
                }

                try {
                    Thread.sleep(STORE_CREATE_RETRY_DELAY_MS);
                } catch (InterruptedException excp) {
                    LOG.error("Thread interrupted while waiting to retry store creation(name={})", name, excp);
                    Thread.currentThread().interrupt();
                }
            }

            if (ret != null) {
                this.storeId = ret;
            } else {
                LOG.error("Failed to create store(name={}) after {} attempts", name, STORE_CREATE_MAX_ATTEMPTS);
            }
        }

        return ret;
    }

    private Long getKeyIdOrCreate(byte[] key, StoreTransaction trx) {
        Long        storeId = getStoreIdOrCreate(trx);
        JanusKeyDao dao     = new JanusKeyDao((RdbmsTransaction) trx);
        Long        ret     = dao.getIdByStoreIdAndName(storeId, key);

        for (int attempt = 1; ret == null; attempt++) {
            try (RdbmsTransaction trx2 = new RdbmsTransaction(trx.getConfiguration(), daoManager)) {
                JanusKeyDao dao2       = new JanusKeyDao(trx2);
                JanusKey    createdKey = dao2.create(new JanusKey(storeId, key));

                trx2.commit();

                ret = createdKey != null ? createdKey.getId() : null;

                LOG.debug("attempt #{}: created key(storeId={}, key={}): id={}", attempt, storeId, key, ret);
            } catch (IOException excp) {
                LOG.error("attempt #{}: failed to create key(storeId={}, key={})", attempt, storeId, key, excp);
            }

            if (ret != null || attempt >= KEY_CREATE_MAX_ATTEMPTS) {
                break;
            }

            try {
                Thread.sleep(KEY_CREATE_RETRY_DELAY_MS);
            } catch (InterruptedException excp) {
                LOG.error("Thread interrupted while waiting to retry key creation(storeId={}, key={})", storeId, key, excp);
                Thread.currentThread().interrupt();
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
                    LOG.debug("RdbmsStore.getMetaData(janusColumnValue={}, entryMetaData={}): UnsupportedOperation", janusColumnValue, entryMetaData);

                    return new UnsupportedOperationException();
                }
            };
}
