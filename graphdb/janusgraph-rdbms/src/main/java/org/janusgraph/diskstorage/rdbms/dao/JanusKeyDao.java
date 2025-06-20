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

import org.janusgraph.diskstorage.rdbms.RdbmsTransaction;
import org.janusgraph.diskstorage.rdbms.entity.JanusKey;

import javax.persistence.NoResultException;

/**
 * DAO to access Key entities stored in RDBMS
 *
 */
public class JanusKeyDao extends BaseDao<JanusKey> {
    public JanusKeyDao(RdbmsTransaction trx) {
        super(trx);
    }

    public Long getIdByStoreIdAndName(long storeId, byte[] name) {
        try {
            Object result = em.createNamedQuery("JanusKey.getIdByStoreIdAndName")
                              .setParameter("storeId", storeId)
                              .setParameter("name", name)
                              .getSingleResult();

            return toLong(result);
        } catch (NoResultException excp) {
            // ignore
        }

        return null;
    }
}
