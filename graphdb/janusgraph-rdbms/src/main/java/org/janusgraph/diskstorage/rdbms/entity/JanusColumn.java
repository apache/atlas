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

@Entity
@Cacheable(false)
@Table(name = "janus_column",
        indexes = {@Index(name = "janus_column_idx_key_id", columnList = "key_id")},
        uniqueConstraints = {@UniqueConstraint(name = "janus_column_uk_key_name", columnNames = {"key_id", "name"})})
public class JanusColumn implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    @Id
    @SequenceGenerator(name = "janus_column_seq", sequenceName = "janus_column_seq", allocationSize = 1000)
    @GeneratedValue(strategy = GenerationType.AUTO, generator = "janus_column_seq")
    @Column(name = "id")
    protected Long id;

    @Column(name = "key_id", nullable = false)
    protected Long keyId;

    @Lob
    @Column(name = "name", nullable = false)
    protected byte[] name;

    @Lob
    @Column(name = "val")
    protected byte[] val;

    public JanusColumn() { }

    public JanusColumn(Long keyId, byte[] name, byte[] val) {
        this.keyId = keyId;
        this.name  = name;
        this.val   = val;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getId() {
        return id;
    }

    public void setKeyId(Long keyId) {
        this.keyId = keyId;
    }

    public Long getKeyId() {
        return keyId;
    }

    public void setName(byte[] name) {
        this.name = name;
    }

    public byte[] getName() {
        return name;
    }

    public void setVal(byte[] val) {
        this.val = val;
    }

    public byte[] getVal() {
        return val;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, keyId, name, val);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj instanceof JanusColumn && getClass() == obj.getClass()) {
            JanusColumn other = (JanusColumn) obj;

            return Objects.equals(id, other.id) &&
                   Objects.equals(keyId, other.keyId) &&
                   Arrays.equals(name, other.name) &&
                   Arrays.equals(val, other.val);
        }

        return false;
    }

    @Override
    public String toString() {
        return "JanusColumn(id=" + id + ", keyId=" + keyId + ", name=" + name + ", val=" + val + ")";
    }
}
