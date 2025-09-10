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
import org.janusgraph.diskstorage.rdbms.entity.JanusStore;

import javax.persistence.NoResultException;

/**
 * DAO to access Store entities stored in RDBMS
 *
 */
public class JanusStoreDao extends BaseDao<JanusStore> {
    public JanusStoreDao(RdbmsTransaction trx) {
        super(trx);
    }

    public Long getIdByName(String name) {
        try {
            Object result = em.createNamedQuery("JanusStore.getIdByName")
                              .setParameter("name", name)
                              .getSingleResult();

            return toLong(result);
        } catch (NoResultException excp) {
            // ignore
        }

        return null;
    }
}
