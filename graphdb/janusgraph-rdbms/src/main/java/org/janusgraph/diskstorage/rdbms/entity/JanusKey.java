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
package org.janusgraph.diskstorage.rdbms.entity;

import javax.persistence.Cacheable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Lob;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import java.util.Arrays;
import java.util.Objects;

/**
 * RDBMS representation of a JanusGraph key, that can hold columns which are name/value pairs
 *
 * @author Madhan Neethiraj &lt;madhan@apache.org&gt;
 */
@Entity
@Cacheable(false)
@Table(name = "janus_key",
        indexes = {@Index(name = "janus_key_idx_store_id", columnList = "store_id")},
        uniqueConstraints = {@UniqueConstraint(name = "janus_key_uk_store_name", columnNames = {"store_id", "name"})})
public class JanusKey implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    @Id
    @SequenceGenerator(name = "janus_key_seq", sequenceName = "janus_key_seq", allocationSize = 1000)
    @GeneratedValue(strategy = GenerationType.AUTO, generator = "janus_key_seq")
    @Column(name = "id")
    protected Long id;

    @Column(name = "store_id", nullable = false)
    protected Long storeId;

    @Lob
    @Column(name = "name", nullable = false)
    protected byte[] name;

    public JanusKey()  { }

    public JanusKey(Long storeId, byte[] name) {
        this.storeId = storeId;
        this.name    = name;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getId() {
        return id;
    }

    public void setStoreId(Long storeId) {
        this.storeId = storeId;
    }

    public Long getStoreId() {
        return storeId;
    }

    public void setName(byte[] name) {
        this.name = name;
    }

    public byte[] getName() {
        return name;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, storeId, name);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj instanceof JanusKey && getClass() == obj.getClass()) {
            JanusKey other = (JanusKey) obj;

            return Objects.equals(id, other.id) &&
                   Objects.equals(storeId, other.storeId) &&
                   Arrays.equals(name, other.name);
        }

        return false;
    }

    @Override
    public String toString() {
        return "JanusKey(id=" + id +  ", storeId=" + storeId + ", name=" + name + ")";
    }
}
