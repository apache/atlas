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
import org.janusgraph.diskstorage.common.AbstractStoreTransaction;
import org.janusgraph.diskstorage.rdbms.dao.DaoManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Store transaction implementation for RDBMS
 *
 */
public class RdbmsTransaction extends AbstractStoreTransaction implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(RdbmsTransaction.class);

    private static final ThreadLocal<List<RdbmsTransaction>> ACTIVE_TRANSACTIONS = ThreadLocal.withInitial(ArrayList::new);
    private static final ThreadLocal<AtomicLong>             TRANSACTION_COUNTS  = ThreadLocal.withInitial(AtomicLong::new);

    private final EntityManager     em;
    private final EntityTransaction trx;

    public RdbmsTransaction(BaseTransactionConfig trxConfig, DaoManager daoManager) {
        super(trxConfig);

        em  = daoManager.createEntityManager();
        trx = em.getTransaction();

        trx.begin();

        long count = TRANSACTION_COUNTS.get().incrementAndGet();

        if (count % 100 == 0) {
            LOG.debug("RDBMS transactions count for thread[{}]: {}", Thread.currentThread().getName(), count);
        }

        addToActiveTransactions();
    }

    public EntityManager getEntityManager() {
        return em;
    }

    @Override
    public void commit() {
        LOG.trace("==> RdbmsTransaction.commit()");

        try {
            if (trx.isActive()) {
                trx.commit();
            }
        } finally {
            removeFromActiveTransactions();
            em.close();
        }

        LOG.trace("<== RdbmsTransaction.commit()");
    }

    @Override
    public void rollback() {
        LOG.trace("==> RdbmsTransaction.rollback()");

        try {
            if (trx.isActive()) {
                trx.rollback();
            }
        } finally {
            removeFromActiveTransactions();
            em.close();
        }

        LOG.trace("<== RdbmsTransaction.rollback()");
    }

    @Override
    public void close() throws IOException {
        LOG.trace("==> RdbmsTransaction.close()");

        IOException ret = null;

        if (trx.isActive()) {
            try {
                trx.rollback();
            } catch (Exception excp) {
                ret = new IOException(excp);
            }
        }

        if (em.isOpen()) {
            try {
                em.close();
            } catch (Exception excp) {
                if (ret != null) {
                    ret = new IOException(excp);
                }
            }
        }

        removeFromActiveTransactions();

        if (ret != null) {
            throw ret;
        }

        LOG.trace("<== RdbmsTransaction.close()");
    }

    static RdbmsTransaction getActiveTransaction() {
        List<RdbmsTransaction> trxList = ACTIVE_TRANSACTIONS.get();

        return trxList.isEmpty() ? null : trxList.get(trxList.size() - 1);
    }

    private void addToActiveTransactions() {
        List<RdbmsTransaction> trxList = ACTIVE_TRANSACTIONS.get();

        if (!trxList.contains(this)) {
            trxList.add(this);
        }
    }

    private void removeFromActiveTransactions() {
        ACTIVE_TRANSACTIONS.get().remove(this);
    }
}
