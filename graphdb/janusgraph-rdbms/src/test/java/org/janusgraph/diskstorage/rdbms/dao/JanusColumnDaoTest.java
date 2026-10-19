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
package org.janusgraph.diskstorage.rdbms.dao;

import org.eclipse.persistence.queries.ScrollableCursor;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryMetaData;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.rdbms.RdbmsStore;
import org.janusgraph.diskstorage.rdbms.RdbmsStoreManager;
import org.janusgraph.diskstorage.rdbms.RdbmsTransaction;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

/**
 * A whole-store scan, which is what a JanusGraph REINDEX runs. EclipseLink rejects getResultList() on a
 * scrollable-cursor query with EclipseLink-6057, which left composite indexes created over existing data
 * stuck short of ENABLED.
 */
public class JanusColumnDaoTest {
    @Test
    public void getKeysByColumnRangeReadsTheScrollableCursor() {
        EntityManager    em     = Mockito.mock(EntityManager.class);
        RdbmsTransaction trx    = Mockito.mock(RdbmsTransaction.class);
        Query            query  = scrollableQuery(em, "JanusColumn.getKeysByStoreIdColumnRange");
        ScrollableCursor cursor = Mockito.mock(ScrollableCursor.class);

        Mockito.when(trx.getEntityManager()).thenReturn(em);
        Mockito.when(query.getSingleResult()).thenReturn(cursor);
        Mockito.when(query.getResultList()).thenThrow(new IllegalStateException("EclipseLink-6057"));
        returnRows(cursor, row(1L, "k1", "c1"), row(1L, "k1", "c2"), row(2L, "k2", "c1"));

        KeyIterator        keys = new JanusColumnDao(trx, Mockito.mock(RdbmsStore.class)).getKeysByColumnRange(7L, "a".getBytes(UTF_8), "z".getBytes(UTF_8), 10);
        List<StaticBuffer> seen = new ArrayList<>();

        while (keys.hasNext()) {
            seen.add(keys.next());
        }

        assertEquals(seen, Arrays.asList(utf8("k1"), utf8("k2")));
        Mockito.verify(query).setHint("eclipselink.cursor.scrollable", true);
        Mockito.verify(query, Mockito.never()).getResultList();
    }

    @Test
    public void eachKeyYieldsItsOwnColumns() {
        EntityManager    em     = Mockito.mock(EntityManager.class);
        RdbmsTransaction trx    = Mockito.mock(RdbmsTransaction.class);
        RdbmsStore       store  = realStore();
        Query            query  = scrollableQuery(em, "JanusColumn.getKeysByStoreIdColumnRange");
        ScrollableCursor cursor = Mockito.mock(ScrollableCursor.class);

        Mockito.when(trx.getEntityManager()).thenReturn(em);
        Mockito.when(query.getSingleResult()).thenReturn(cursor);
        returnRows(cursor, row(1L, "k1", "c1"), row(1L, "k1", "c2"), row(2L, "k2", "c1"));

        KeyIterator  keys    = new JanusColumnDao(trx, store).getKeysByColumnRange(7L, "a".getBytes(UTF_8), "z".getBytes(UTF_8), 10);
        List<String> visited = new ArrayList<>();

        while (keys.hasNext()) {
            String key   = new String(keys.next().as(StaticBuffer.ARRAY_FACTORY), UTF_8);
            int    count = 0;

            for (RecordIterator<Entry> entries = keys.getEntries(); entries.hasNext(); entries.next()) {
                count++;
            }

            visited.add(key + "=" + count);
        }

        assertEquals(visited, Arrays.asList("k1=2", "k2=1"));
    }

    /**
     * JanusGraph reads a full vertex scan to the end and never closes it, so reaching the end is the
     * only signal there is to give the cursor's connection back to the pool.
     */
    @Test
    public void aScanReadToTheEndClosesItsCursor() throws Exception {
        EntityManager    em     = Mockito.mock(EntityManager.class);
        RdbmsTransaction trx    = Mockito.mock(RdbmsTransaction.class);
        Query            query  = scrollableQuery(em, "JanusColumn.getKeysByStoreIdKeyRangeColumnRange");
        ScrollableCursor cursor = Mockito.mock(ScrollableCursor.class);

        Mockito.when(trx.getEntityManager()).thenReturn(em);
        Mockito.when(query.getSingleResult()).thenReturn(cursor);
        returnRows(cursor, row(1L, "k1", "c1"), row(2L, "k2", "c1"));

        KeyIterator keys = new JanusColumnDao(trx, Mockito.mock(RdbmsStore.class)).getKeysByKeyAndColumnRange(7L, "a".getBytes(UTF_8), "z".getBytes(UTF_8), "a".getBytes(UTF_8), "z".getBytes(UTF_8), 10);

        keys.next();
        Mockito.verify(cursor, Mockito.never()).close();

        while (keys.hasNext()) {
            keys.next();
        }

        Mockito.verify(cursor).close();
        Mockito.verify(trx).addCursor(cursor);

        keys.close();
        Mockito.verify(cursor).close();
    }

    @Test
    public void anEmptyScanClosesItsCursor() {
        EntityManager    em     = Mockito.mock(EntityManager.class);
        RdbmsTransaction trx    = Mockito.mock(RdbmsTransaction.class);
        Query            query  = scrollableQuery(em, "JanusColumn.getKeysByStoreIdColumnRange");
        ScrollableCursor cursor = Mockito.mock(ScrollableCursor.class);

        Mockito.when(trx.getEntityManager()).thenReturn(em);
        Mockito.when(query.getSingleResult()).thenReturn(cursor);
        Mockito.when(cursor.hasNext()).thenReturn(false);

        KeyIterator keys = new JanusColumnDao(trx, Mockito.mock(RdbmsStore.class)).getKeysByColumnRange(7L, "a".getBytes(UTF_8), "z".getBytes(UTF_8), 10);

        assertSame(keys, JanusColumnDao.EMPTY_KEY_ITERATOR);
        Mockito.verify(cursor).close();
    }

    private static StaticBuffer utf8(String s) {
        return StaticArrayBuffer.of(s.getBytes(UTF_8));
    }

    private static RdbmsStore realStore() {
        RdbmsStoreManager manager = Mockito.mock(RdbmsStoreManager.class);

        Mockito.when(manager.getDaoManager()).thenReturn(Mockito.mock(DaoManager.class));
        Mockito.when(manager.getMetaDataSchema(Mockito.anyString())).thenReturn(new EntryMetaData[0]);

        return new RdbmsStore("edgestore", manager);
    }

    private static Query scrollableQuery(EntityManager em, String name) {
        Query query = Mockito.mock(Query.class);

        Mockito.when(em.createNamedQuery(name)).thenReturn(query);
        Mockito.when(query.setParameter(Mockito.anyString(), Mockito.any())).thenReturn(query);
        Mockito.when(query.setHint(Mockito.anyString(), Mockito.any())).thenReturn(query);

        return query;
    }

    private static void returnRows(ScrollableCursor cursor, Object[]... rows) {
        Deque<Object[]> remaining = new ArrayDeque<>(Arrays.asList(rows));

        Mockito.when(cursor.hasNext()).thenAnswer(call -> !remaining.isEmpty());
        Mockito.when(cursor.next()).thenAnswer(call -> remaining.poll());
    }

    private static Object[] row(long keyId, String key, String column) {
        return new Object[] {keyId, key.getBytes(UTF_8), column.getBytes(UTF_8), "v".getBytes(UTF_8)};
    }
}
