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
import org.janusgraph.diskstorage.rdbms.dao.DaoManager;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RdbmsTransactionTest {
    private EntityManager     entityManager;
    private EntityTransaction entityTransaction;
    private DaoManager        daoManager;
    private boolean           entityManagerOpen;
    private boolean           transactionActive;

    @BeforeMethod
    public void setUp() {
        entityManager     = mock(EntityManager.class);
        entityTransaction = mock(EntityTransaction.class);
        daoManager        = mock(DaoManager.class);
        entityManagerOpen = true;
        transactionActive = false;

        when(daoManager.createEntityManager()).thenReturn(entityManager);
        when(entityManager.getTransaction()).thenReturn(entityTransaction);
        when(entityManager.isOpen()).thenAnswer(invocation -> entityManagerOpen);
        when(entityTransaction.isActive()).thenAnswer(invocation -> transactionActive);

        // the behaviour that matters: a closed entity manager refuses everything, closing included
        doAnswer(invocation -> {
            if (!entityManagerOpen) {
                throw new IllegalStateException("Attempting to execute an operation on a closed EntityManager.");
            }

            entityManagerOpen = false;
            transactionActive = false;

            return null;
        }).when(entityManager).close();

        doAnswer(invocation -> {
            transactionActive = true;

            return null;
        }).when(entityTransaction).begin();

        doAnswer(invocation -> {
            transactionActive = false;

            return null;
        }).when(entityTransaction).commit();

        doAnswer(invocation -> {
            transactionActive = false;

            return null;
        }).when(entityTransaction).rollback();
    }

    /**
     * JanusGraph rolls a transaction back after a commit fails.  The rollback must not throw over the
     * top of that failure: the caller would be handed a complaint about a closed entity manager in
     * place of the lock conflict it was ready to retry.
     */
    @Test
    public void rollingBackAFinishedTransactionIsQuiet() {
        RdbmsTransaction transaction = new RdbmsTransaction(mock(BaseTransactionConfig.class), daoManager);

        transaction.commit();

        transaction.rollback();

        verify(entityTransaction, never()).rollback();
        verify(entityManager, times(1)).close();
    }

    @Test
    public void rollingBackAnUnfinishedTransactionGivesItBack() {
        RdbmsTransaction transaction = new RdbmsTransaction(mock(BaseTransactionConfig.class), daoManager);

        transaction.rollback();

        verify(entityTransaction).rollback();
        verify(entityManager, times(1)).close();
    }
}
