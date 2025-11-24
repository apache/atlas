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

import javax.persistence.EntityManager;

/**
 * Base DAO to access entities stored in RDBMS
 *
 */
public abstract class BaseDao<T> {
    protected final EntityManager em;

    protected BaseDao(RdbmsTransaction trx) {
        this.em = trx.getEntityManager();
    }

    protected BaseDao(EntityManager em) {
        this.em = em;
    }

    public T create(T obj) {
        em.persist(obj);

        return obj;
    }

    protected Long toLong(Object obj) {
        return (obj instanceof Long) ? (Long) obj : null;
    }

    protected byte[] toByteArray(Object obj) {
        return (obj instanceof byte[]) ? (byte[]) obj : null;
    }
}
