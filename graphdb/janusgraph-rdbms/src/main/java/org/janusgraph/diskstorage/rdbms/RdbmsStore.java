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
                } catch (Exception excp) {
                    // A store row is created on first use exactly like a key row, and races the same
                    // way when two nodes start against an empty schema, so it is recovered the same
                    // way: read the winner's row rather than insert again.  This used to catch
                    // IOException, which the persistence layer never throws, so the duplicate key
                    // violation escaped the loop and failed the caller outright.
                    ret = readStoreId(trx);

                    if (ret != null) {
                        LOG.debug("attempt #{}: store(name={}) was created by another writer: id={}", attempt, name, ret);
                    } else {
                        LOG.error("attempt #{}: failed to create store(name={})", attempt, name, excp);
                    }
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

    /**
     * The id of the {@code janus_key} row for this key, creating that row if no one has yet.
     *
     * <p>Every property key is given a row of its own the first time any node writes it, and
     * {@code janus_key} constrains (store_id, name) to be unique.  Two nodes that touch a new key at
     * the same time therefore both try to insert the same row, and the database picks one winner: the
     * loser's insert fails at commit with a duplicate key violation.
     *
     * <p>What matters is what the loser does next.  The row it was trying to create now exists, and
     * it is the row the loser has to use, because the unique constraint means there can only ever be
     * one row for this key.  Only a look-up can produce that row's id.  Inserting again cannot: the
     * winner's row is still there, so every further attempt fails exactly as the first one did.
     *
     * <p>So a failed insert is answered by reading the key back, in {@link #readKeyId}.  Previously it
     * was answered by inserting again: the loop retried the insert for all
     * {@value #KEY_CREATE_MAX_ATTEMPTS} attempts, every one of them failing on the same constraint,
     * and then returned null.  A null id fails the whole graph operation and reaches the client as a
     * server error, so two nodes writing the same new property key at the same time - which is what
     * attaching classifications concurrently does - could lose one of the writes outright.
     *
     * <p>The read-back is a separate step rather than the next turn of the loop because it resolves
     * the race at once, and because it does not depend on the caller's transaction being able to see
     * the other writer's commit: {@link #readKeyId} opens a transaction of its own, which is correct
     * whatever isolation level the database is configured with.
     *
     * <p>The insert deliberately runs in its own transaction ({@code trx2}) and commits on its own:
     * the key row has to survive regardless of what happens to the caller's transaction, since the
     * key is shared by every writer rather than owned by this one operation.  That also means a failed
     * insert leaves the caller's transaction untouched and still usable.
     *
     * <p>The catch is on {@link Exception} rather than {@link Throwable}: a duplicate key arrives as a
     * persistence exception, while an {@link Error} says something is wrong with the JVM rather than
     * with this row, and absorbing it here would only hide it.
     *
     * <p>A lost race is logged at debug, because it is ordinary and fully recovered from.  Only an
     * insert that failed for a reason the read-back cannot explain is logged as an error.
     */
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
            } catch (Exception excp) {
                ret = readKeyId(storeId, key, trx);

                if (ret != null) {
                    LOG.debug("attempt #{}: key(storeId={}, key.length={}) was created by another writer: id={}", attempt, storeId, key.length, ret);
                } else {
                    LOG.error("attempt #{}: failed to create key(storeId={}, key.length={})", attempt, storeId, key.length, excp);
                }
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

    /**
     * Reads the id of a key row in a transaction of its own.
     *
     * <p>The read has to be in its own transaction to be sure of seeing the row.  The caller's
     * transaction may have begun before the other writer committed, and under snapshot-based
     * isolation a query in that transaction can only see the state as of its own start.  A
     * transaction opened now begins after that commit and therefore sees it.
     *
     * <p>Returning null here does not distinguish "no such row" from "the read itself failed": the
     * caller treats both the same way, by trying again on its next attempt, so there is nothing for
     * this method to decide.  The failure is logged at debug for the case where the read, and not the
     * race, is what went wrong.
     */
    private Long readKeyId(Long storeId, byte[] key, StoreTransaction trx) {
        try (RdbmsTransaction trx2 = new RdbmsTransaction(trx.getConfiguration(), daoManager)) {
            return new JanusKeyDao(trx2).getIdByStoreIdAndName(storeId, key);
        } catch (Exception excp) {
            LOG.debug("failed to read key(storeId={}, key.length={}) back", storeId, key.length, excp);

            return null;
        }
    }

    /** Reads the id of this store's row, in a transaction of its own and for the reason given in {@link #readKeyId}. */
    private Long readStoreId(StoreTransaction trx) {
        try (RdbmsTransaction trx2 = new RdbmsTransaction(trx.getConfiguration(), daoManager)) {
            return new JanusStoreDao(trx2).getIdByName(name);
        } catch (Exception excp) {
            LOG.debug("failed to read store(name={}) back", name, excp);

            return null;
        }
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
