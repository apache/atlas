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
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import java.util.Objects;

/**
 * RDBMS representation of a store that can hold keys
 *
 * @author Madhan Neethiraj &lt;madhan@apache.org&gt;
 */
@Entity
@Cacheable(false)
@Table(name = "janus_store",
        uniqueConstraints = @UniqueConstraint(name = "janus_store_uk_name", columnNames = "name"))
public class JanusStore implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    @Id
    @SequenceGenerator(name = "janus_store_seq", sequenceName = "janus_store_seq", allocationSize = 1)
    @GeneratedValue(strategy = GenerationType.AUTO, generator = "janus_store_seq")
    @Column(name = "id")
    protected Long id;

    @Column(name = "name", nullable = false)
    protected String name;

    public JanusStore() { }

    public JanusStore(String name) {
        this.name = name;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getId() {
        return id;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj instanceof JanusStore && getClass() == obj.getClass()) {
            JanusStore other = (JanusStore) obj;

            return Objects.equals(id, other.id) &&
                   Objects.equals(name, other.name);
        }

        return false;
    }

    @Override
    public String toString() {
        return "JanusStore(id=" + id + ", name=" + name + ")";
    }
}
