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

import org.eclipse.persistence.annotations.Index;

import javax.persistence.Cacheable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import java.util.Objects;

@Entity
@Cacheable(false)
@Table(name = "janus_unique_edge_key",
        uniqueConstraints = {@UniqueConstraint(name = "janus_unique_edge_key_uk", columnNames = {"key_name", "val"})})
public class JanusUniqueEdgeKey implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    @Id
    @Column(name = "id")
    protected Long id;

    @Column(name = "edge_id", nullable = false)
    @Index
    protected Long edgeId;

    @Column(name = "key_name", nullable = false)
    protected String keyName;

    @Lob
    @Column(name = "val", nullable = false)
    protected String val;

    public JanusUniqueEdgeKey() { }

    public JanusUniqueEdgeKey(String keyName, String val) {
        this.keyName = keyName;
        this.val     = val;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getId() {
        return id;
    }

    public void setEdgeId(Long edgeId) {
        this.edgeId = edgeId;
    }

    public Long getEdgeId() {
        return edgeId;
    }

    public void setKeyName(String keyName) {
        this.keyName = keyName;
    }

    public String getKeyName() {
        return keyName;
    }

    public void setVal(String val) {
        this.val = val;
    }

    public String getVal() {
        return val;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, edgeId, keyName, val);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj instanceof JanusUniqueEdgeKey && getClass() == obj.getClass()) {
            JanusUniqueEdgeKey other = (JanusUniqueEdgeKey) obj;

            return Objects.equals(id, other.id) &&
                    Objects.equals(edgeId, other.edgeId) &&
                    Objects.equals(keyName, other.keyName) &&
                    Objects.equals(val, other.val);
        }

        return false;
    }

    @Override
    public String toString() {
        return "JanusUniqueEdgeKey(id=" + id + ", edgeId=" + edgeId +  ", keyName=" + keyName + ", val=" + val + ")";
    }
}
