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

import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.EntryMetaData;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.rdbms.dao.DaoManager;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import javax.persistence.NoResultException;
import javax.persistence.Query;
import javax.persistence.RollbackException;

import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

/**
 * Every property key is registered in a table of its own the first time it is used, so two nodes
 * touching a new key at the same time both try to create the same row.  One of them loses, and what
 * it does next decides whether the request survives: the row it wanted now exists, so reading it is
 * the answer, while creating it again cannot ever succeed.
 */
public class RdbmsStoreTest {
    private static final String STORE_NAME = "edgestore";
    private static final long   STORE_ID   = 7L;
    private static final long   KEY_ID     = 42L;

    private DaoManager    daoManager;
    private AtomicInteger keyLookups;
    private AtomicInteger createAttempts;

    @BeforeMethod
    public void setUp() {
        daoManager     = mock(DaoManager.class);
        keyLookups     = new AtomicInteger();
        createAttempts = new AtomicInteger();

        when(daoManager.createEntityManager()).thenAnswer(invocation -> newEntityManager());
    }

    @Test
    public void aKeyAnotherWriterCreatedFirstIsReadBackRatherThanCreatedAgain() throws Exception {
        Long keyId = getKeyIdOrCreate(newStore(), "__entityStatus".getBytes());

        assertEquals(keyId, (Long) KEY_ID, "The key another writer created is the key to use");
        assertEquals(createAttempts.get(), 1, "Creating the key again could only fail the same way");
    }

    private RdbmsStore newStore() {
        RdbmsStoreManager storeManager = mock(RdbmsStoreManager.class);

        when(storeManager.getDaoManager()).thenReturn(daoManager);
        when(storeManager.getMetaDataSchema(STORE_NAME)).thenReturn(new EntryMetaData[0]);

        return new RdbmsStore(STORE_NAME, storeManager);
    }

    private Long getKeyIdOrCreate(RdbmsStore store, byte[] key) throws Exception {
        StoreTransaction trx    = new RdbmsTransaction(mock(BaseTransactionConfig.class), daoManager);
        Method           method = RdbmsStore.class.getDeclaredMethod("getKeyIdOrCreate", byte[].class, StoreTransaction.class);

        method.setAccessible(true);

        return (Long) method.invoke(store, key, trx);
    }

    /**
     * A stand-in for one JPA session.  The first key lookup finds nothing - that is why the caller
     * goes on to create it - and the create fails the way Postgres fails it, at commit, once the
     * other writer's row is already there for later lookups to find.
     */
    private EntityManager newEntityManager() {
        EntityManager     entityManager = mock(EntityManager.class);
        EntityTransaction transaction   = mock(EntityTransaction.class);
        AtomicBoolean     active        = new AtomicBoolean();
        AtomicBoolean     persisted     = new AtomicBoolean();

        when(entityManager.getTransaction()).thenReturn(transaction);
        when(entityManager.isOpen()).thenReturn(true);
        when(entityManager.createNamedQuery(anyString())).thenAnswer(invocation -> namedQuery(invocation.getArgument(0)));

        doAnswer(invocation -> {
            createAttempts.incrementAndGet();
            persisted.set(true);

            return null;
        }).when(entityManager).persist(any());

        when(transaction.isActive()).thenAnswer(invocation -> active.get());

        doAnswer(invocation -> {
            active.set(true);

            return null;
        }).when(transaction).begin();

        doAnswer(invocation -> {
            active.set(false);

            if (persisted.get()) {
                throw new RollbackException("duplicate key value violates unique constraint \"janus_key_uk_store_name\"");
            }

            return null;
        }).when(transaction).commit();

        doAnswer(invocation -> {
            active.set(false);

            return null;
        }).when(transaction).rollback();

        return entityManager;
    }

    private Query namedQuery(String name) {
        Query query = mock(Query.class);

        when(query.setParameter(anyString(), any())).thenReturn(query);

        if ("JanusStore.getIdByName".equals(name)) {
            when(query.getSingleResult()).thenReturn(STORE_ID);
        } else {
            when(query.getSingleResult()).thenAnswer(invocation -> {
                if (keyLookups.incrementAndGet() == 1) {
                    throw new NoResultException("the key has not been created yet");
                }

                return KEY_ID;
            });
        }

        return query;
    }
}
