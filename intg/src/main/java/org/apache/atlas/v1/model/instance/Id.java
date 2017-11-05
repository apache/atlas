/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.v1.model.instance;


import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.NONE;
import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.PUBLIC_ONLY;


@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class Id implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonIgnore
    private static AtomicLong s_nextId = new AtomicLong(System.nanoTime());

    public enum EntityState { ACTIVE, DELETED }

    private String      id;
    private String      typeName;
    private int         version;
    private EntityState state;


    public Id() {
    }

    public Id(Id that) {
        if (that != null) {
            this.id       = that.id;
            this.typeName = that.typeName;
            this.version  = that.version;
            this.state    = that.state;
        }
    }

    public Id(String typeName) {
        this("" + nextNegativeLong(), 0, typeName);
    }

    public Id(String id, int version, String typeName) {
        this(id, version, typeName, null);
    }

    public Id(long id, int version, String typeName) {
        this(id, version, typeName, null);
    }

    public Id(long id, int version, String typeName, String state) {
        this("" + id, version, typeName, state);
    }

    public Id(String id, int version, String typeName, String state) {
        this.id       = id;
        this.typeName = typeName;
        this.version  = version;
        this.state    = state == null ? EntityState.ACTIVE : EntityState.valueOf(state.toUpperCase());
    }

    // for serialization backward compatibility
    public String getJsonClass() {
        return "org.apache.atlas.typesystem.json.InstanceSerialization$_Id";
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public EntityState getState() {
        return state;
    }

    public void setState(EntityState state) {
        this.state = state;
    }

    @JsonIgnore
    public String _getId() {
        return id;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Id obj = (Id) o;

        return version == obj.version &&
               Objects.equals(id, obj.id) &&
               Objects.equals(typeName, obj.typeName) &&
                Objects.equals(state, obj.state);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, typeName, version, state);
    }


    @Override
    public String toString() {
        return toString(new StringBuilder()).toString();
    }

    public StringBuilder toString(StringBuilder sb) {
        if (sb == null) {
            sb = new StringBuilder();
        }

        sb.append("Id{")
          .append("id=").append(id)
          .append(", typeName=").append(typeName)
          .append(", version=").append(version)
          .append(", state=").append(state)
          .append("}");

        return sb;
    }

    private static long nextNegativeLong() {
        long ret = s_nextId.getAndDecrement();

        if (ret > 0) {
            ret *= -1;
        } else if (ret == 0) {
            ret = Long.MIN_VALUE;
        }

        return ret;
    }
}
