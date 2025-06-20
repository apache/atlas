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

/**
 * Store transaction implementation for RDBMS
 *
 */
public class RdbmsTransaction extends AbstractStoreTransaction implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(RdbmsTransaction.class);

    private final EntityManager     em;
    private final EntityTransaction trx;

    public RdbmsTransaction(BaseTransactionConfig trxConfig, DaoManager daoManager) {
        super(trxConfig);

        em  = daoManager.createEntityManager();
        trx = em.getTransaction();

        trx.begin();
    }

    public EntityManager getEntityManager() {
        return em;
    }

    @Override
    public void commit() {
        LOG.debug("RdbmsTransaction.commit()");

        try {
            if (trx.isActive()) {
                trx.commit();
            }
        } finally {
            em.close();
        }
    }

    @Override
    public void rollback() {
        LOG.debug("RdbmsTransaction.rollback()");

        try {
            if (trx.isActive()) {
                trx.rollback();
            }
        } finally {
            em.close();
        }
    }

    @Override
    public void close() throws IOException {
        LOG.debug("RdbmsTransaction.close()");

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

        if (ret != null) {
            throw ret;
        }
    }
}
