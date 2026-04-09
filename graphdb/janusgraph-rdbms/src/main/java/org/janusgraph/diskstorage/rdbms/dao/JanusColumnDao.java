/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.janusgraph.diskstorage.rdbms.dao;

import org.eclipse.persistence.queries.ScrollableCursor;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.rdbms.JanusColumnValue;
import org.janusgraph.diskstorage.rdbms.RdbmsStore;
import org.janusgraph.diskstorage.rdbms.RdbmsTransaction;
import org.janusgraph.diskstorage.rdbms.entity.JanusColumn;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;

import javax.persistence.NoResultException;

import java.util.ArrayList;
import java.util.List;

/**
 * DAO to access Column entities stored in RDBMS
 *
 */
public class JanusColumnDao extends BaseDao<JanusColumn> {
    private final RdbmsStore store;

    public JanusColumnDao(RdbmsTransaction trx, RdbmsStore store) {
        super(trx);

        this.store = store;
    }

    public void addOrUpdate(long keyId, byte[] name, byte[] val) {
        try {
            em.createNativeQuery("INSERT INTO janus_column (key_id, name, val) VALUES (?, ?, ?) ON CONFLICT (key_id, name) DO UPDATE SET val = EXCLUDED.val")
                    .setParameter(1, keyId)
                    .setParameter(2, name)
                    .setParameter(3, val)
                    .executeUpdate();
        } catch (NoResultException excp) {
            // ignore
        }
    }

    public int remove(long keyId, byte[] name) {
        return em.createNamedQuery("JanusColumn.deleteByKeyIdAndName")
                    .setParameter("keyId", keyId)
                    .setParameter("name", name)
                    .executeUpdate();
    }

    public List<JanusColumnValue> getColumns(long keyId, byte[] startColumn, byte[] endColumn, int limit) {
        List<Object[]> result = em.createNamedQuery("JanusColumn.getColumnsByKeyIdStartNameEndName", Object[].class)
                                    .setParameter("keyId", keyId)
                                    .setParameter("startName", startColumn)
                                    .setParameter("endName", endColumn)
                                    .setMaxResults(limit)
                                    .getResultList();

        return toColumnList(result);
    }

    public KeyIterator getKeysByColumnRange(long storeId, byte[] startColumn, byte[] endColumn, int limit) {
        ScrollableCursor result = (ScrollableCursor) em.createNamedQuery("JanusColumn.getKeysByStoreIdColumnRange")
                                                       .setParameter("storeId", storeId)
                                                       .setParameter("startName", startColumn)
                                                       .setParameter("endName", endColumn)
                                                       .setHint("eclipselink.cursor.scrollable", true)
                                                       .getResultList();

        return toKeyColumns(result, limit);
    }

    public KeyIterator getKeysByKeyAndColumnRange(long storeId, byte[] startKey, byte[] endKey, byte[] startColumn, byte[] endColumn, int limit) {
        ScrollableCursor result = (ScrollableCursor) em.createNamedQuery("JanusColumn.getKeysByStoreIdKeyRangeColumnRange")
                                                       .setParameter("storeId", storeId)
                                                       .setParameter("startKey", startKey)
                                                       .setParameter("endKey", endKey)
                                                       .setParameter("startName", startColumn)
                                                       .setParameter("endName", endColumn)
                                                       .setHint("eclipselink.cursor.scrollable", true)
                                                       .getSingleResult();

        return toKeyColumns(result, limit);
    }

    private List<JanusColumnValue> toColumnList(List<Object[]> result) {
        List<JanusColumnValue> ret = null;

        if (result != null && !result.isEmpty()) {
            ret = new ArrayList<>(result.size());

            for (Object[] row : result) {
                byte[] name = toByteArray(row[0]);
                byte[] val  = toByteArray(row[1]);

                ret.add(new JanusColumnValue(name, val));
            }
        }

        return ret;
    }

    private KeyIterator toKeyColumns(ScrollableCursor keysResult, int limit) {
        final KeyIterator ret;

        if (keysResult != null && keysResult.hasNext()) {
            ret = new RdbmsKeyIterator(keysResult, limit);
        } else {
            ret = EMPTY_KEY_ITERATOR;
        }

        return ret;
    }

    private class RdbmsKeyIterator implements KeyIterator {
        private final ScrollableCursor rows;
        private final int              limit;
        private final Row              currKey   = new Row();
        private final Row              nextKey   = new Row();
        private       Long             prevKeyId;
        private       boolean          isClosed;

        public RdbmsKeyIterator(ScrollableCursor rows, int limit) {
            this.rows  = rows;
            this.limit = limit;
        }

        @Override
        public boolean hasNext() {
            ensureOpen();

            if (nextKey.keyId == null) {
                while (rows.hasNext()) {
                    Object[] nextRow = (Object[]) rows.next();
                    Long     keyId   = toLong(nextRow[0]);

                    if (prevKeyId != null && prevKeyId.equals(keyId)) { // ignore additional columns for this key
                        continue;
                    }

                    nextKey.set(keyId, StaticArrayBuffer.of(toByteArray(nextRow[1])), new JanusColumnValue(toByteArray(nextRow[2]), toByteArray(nextRow[3])));

                    break;
                }
            }

            return nextKey.keyId != null;
        }

        @Override
        public StaticBuffer next() {
            ensureOpen();

            prevKeyId = currKey.keyId;

            if (nextKey.keyId == null) {
                hasNext();
            }

            currKey.copyFrom(nextKey);

            nextKey.reset();

            return currKey.key;
        }

        @Override
        public RecordIterator<Entry> getEntries() {
            ensureOpen();

            return new RecordIterator<Entry>() {
                private boolean isClosed;
                private int     colCount;

                @Override
                public boolean hasNext() {
                    ensureOpen();

                    if (currKey.column == null) {
                        while (rows.hasNext()) {
                            Object[] nextRow = (Object[]) rows.next();
                            Long     keyId   = toLong(nextRow[0]);

                            if (!keyId.equals(currKey.keyId)) {
                                nextKey.set(keyId, StaticArrayBuffer.of(toByteArray(nextRow[1])), new JanusColumnValue(toByteArray(nextRow[2]), toByteArray(nextRow[3])));
                                currKey.reset();

                                break;
                            } else if (colCount < limit) { // ignore additional columns for this key
                                currKey.column = new JanusColumnValue(toByteArray(nextRow[2]), toByteArray(nextRow[3]));

                                break;
                            }
                        }
                    }

                    return currKey.column != null;
                }

                @Override
                public Entry next() {
                    JanusColumnValue ret = currKey.column;

                    currKey.column = null;
                    colCount++;

                    return StaticArrayEntry.ofStaticBuffer(ret, store.toEntry);
                }

                @Override
                public void close() {
                    this.isClosed = true;
                }

                private void ensureOpen() {
                    if (isClosed) {
                        throw new IllegalStateException("Iterator has been closed.");
                    }
                }
            };
        }

        @Override
        public void close() {
            isClosed = true;

            rows.close();
        }

        private void ensureOpen() {
            if (isClosed) {
                throw new IllegalStateException("Iterator has been closed.");
            }
        }

        private class Row {
            private Long             keyId;
            private StaticBuffer     key;
            private JanusColumnValue column;

            Row() { }

            Row(Long keyId, StaticBuffer key, JanusColumnValue column) {
                this.keyId  = keyId;
                this.key    = key;
                this.column = column;
            }

            void copyFrom(Row other) {
                this.keyId  = other.keyId;
                this.key    = other.key;
                this.column = other.column;
            }

            void set(Long keyId, StaticBuffer key, JanusColumnValue column) {
                this.keyId  = keyId;
                this.key    = key;
                this.column = column;
            }

            void reset() {
                this.keyId  = null;
                this.key    = null;
                this.column = null;
            }
        }
    }

    public static final KeyIterator EMPTY_KEY_ITERATOR = new KeyIterator() {
        @Override
        public RecordIterator<Entry> getEntries() {
            return null;
        }

        @Override
        public void close() {
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public StaticBuffer next() {
            return null;
        }
    };
}
