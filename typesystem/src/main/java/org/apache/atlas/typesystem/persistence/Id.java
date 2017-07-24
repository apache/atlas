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

package org.apache.atlas.typesystem.persistence;

import com.google.common.collect.ImmutableList;
import org.apache.atlas.AtlasException;
import org.apache.atlas.utils.ParamChecker;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.types.FieldMapping;
import org.apache.atlas.utils.SHA256Utils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class Id implements ITypedReferenceableInstance {
    public enum EntityState {
        ACTIVE, DELETED
    }

    public final String id;
    public final String typeName;
    public final int version;
    public EntityState state;
    private static AtomicLong s_nextId = new AtomicLong(System.nanoTime());
    public final AtlasSystemAttributes systemAttributes;

    public Id(String id, int version, String typeName, String state) {
        id       = ParamChecker.notEmpty(id, "id");
        typeName = ParamChecker.notEmpty(typeName, "typeName");
        state    = ParamChecker.notEmptyIfNotNull(state, "state");
        this.id = id;
        this.typeName = typeName;
        this.version = version;
        if (state == null) {
            this.state = EntityState.ACTIVE;
        } else {
            this.state = EntityState.valueOf(state.toUpperCase());
        }
        this.systemAttributes = new AtlasSystemAttributes();
    }

    public Id(String id, int version, String typeName) {
        this(id, version, typeName, null);
    }

    public Id(long id, int version, String typeName) {
        this("" + id, version, typeName);
    }

    public Id(long id, int version, String typeName, String state) {
        this("" + id, version, typeName, state);
    }

    public Id(String typeName) {
        this("" + Id.nextNegativeLong(), 0, typeName);
    }

    public boolean isUnassigned() {
        try {
            long l = Long.parseLong(id);
            return l < 0;
        } catch (NumberFormatException ne) {
            return false;
        }
    }

    public boolean isAssigned() {
        try {
            UUID.fromString(id);
        } catch (IllegalArgumentException e) {
            return false;
        }

        return true;
    }

    @Override
    public String toString() {
        return String.format("(type: %s, id: %s)", typeName, isUnassigned() ? "<unassigned>" : "" + id);
    }

    @Override
    public String toShortString() {
        return String.format("id[type=%s guid=%s state=%s]", typeName, id, state);
    }

    @Override
    public AtlasSystemAttributes getSystemAttributes(){
        return systemAttributes;
    }

    public String getClassName() {
        return typeName;
    }

    public int getVersion() {
        return version;
    }

    public String _getId() {
        return id;
    }

    public EntityState getState() {
        return state;
    }

    public String getStateAsString() {
        return state == null ? null : state.name();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Id id1 = (Id) o;
        return version == id1.version &&
                Objects.equals(id, id1.id) &&
                Objects.equals(typeName, id1.typeName) &&
                state == id1.state;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, typeName, version, state);
    }

    @Override
    public ImmutableList<String> getTraits() {
        return null;
    }

    @Override
    public Id getId() {
        return this;
    }

    @Override
    public IStruct getTrait(String typeName) {
        return null;
    }

    @Override
    public String getTypeName() {
        return typeName;
    }

    @Override
    public Object get(String attrName) throws AtlasException {
        throw new AtlasException("Get/Set not supported on an Id object");
    }

    @Override
    public void set(String attrName, Object val) throws AtlasException {
        throw new AtlasException("Get/Set not supported on an Id object");
    }

    @Override
    public FieldMapping fieldMapping() {
        return null;
    }

    @Override
    public Map<String, Object> getValuesMap() throws AtlasException {
        throw new AtlasException("Get/Set not supported on an Id object");
    }

    public void setNull(String attrName) throws AtlasException {
        set(attrName, null);
    }

    public boolean getBoolean(String attrName) throws AtlasException {
        throw new AtlasException("Get/Set not supported on an Id object");
    }

    public byte getByte(String attrName) throws AtlasException {
        throw new AtlasException("Get/Set not supported on an Id object");
    }

    public short getShort(String attrName) throws AtlasException {
        throw new AtlasException("Get/Set not supported on an Id object");
    }

    public int getInt(String attrName) throws AtlasException {
        throw new AtlasException("Get/Set not supported on an Id object");
    }

    public long getLong(String attrName) throws AtlasException {
        throw new AtlasException("Get/Set not supported on an Id object");
    }

    public float getFloat(String attrName) throws AtlasException {
        throw new AtlasException("Get/Set not supported on an Id object");
    }

    public double getDouble(String attrName) throws AtlasException {
        throw new AtlasException("Get/Set not supported on an Id object");
    }

    public BigInteger getBigInt(String attrName) throws AtlasException {
        throw new AtlasException("Get/Set not supported on an Id object");
    }

    public BigDecimal getBigDecimal(String attrName) throws AtlasException {
        throw new AtlasException("Get/Set not supported on an Id object");
    }

    public Date getDate(String attrName) throws AtlasException {
        throw new AtlasException("Get/Set not supported on an Id object");
    }

    public String getString(String attrName) throws AtlasException {
        throw new AtlasException("Get/Set not supported on an Id object");
    }

    public void setBoolean(String attrName, boolean val) throws AtlasException {
        throw new AtlasException("Get/Set not supported on an Id object");
    }

    public void setByte(String attrName, byte val) throws AtlasException {
        throw new AtlasException("Get/Set not supported on an Id object");
    }

    public void setShort(String attrName, short val) throws AtlasException {
        throw new AtlasException("Get/Set not supported on an Id object");
    }

    public void setInt(String attrName, int val) throws AtlasException {
        throw new AtlasException("Get/Set not supported on an Id object");
    }

    public void setLong(String attrName, long val) throws AtlasException {
        throw new AtlasException("Get/Set not supported on an Id object");
    }

    public void setFloat(String attrName, float val) throws AtlasException {
        throw new AtlasException("Get/Set not supported on an Id object");
    }

    public void setDouble(String attrName, double val) throws AtlasException {
        throw new AtlasException("Get/Set not supported on an Id object");
    }

    public void setBigInt(String attrName, BigInteger val) throws AtlasException {
        throw new AtlasException("Get/Set not supported on an Id object");
    }

    public void setBigDecimal(String attrName, BigDecimal val) throws AtlasException {
        throw new AtlasException("Get/Set not supported on an Id object");
    }

    public void setDate(String attrName, Date val) throws AtlasException {
        throw new AtlasException("Get/Set not supported on an Id object");
    }

    public void setString(String attrName, String val) throws AtlasException {
        throw new AtlasException("Get/Set not supported on an Id object");
    }

    public boolean isValueSet(String attrName) throws AtlasException {
        throw new AtlasException("Attributes not set on an Id object");
    }

    @Override
    public String getSignatureHash(MessageDigest digester) throws AtlasException {
        digester.update(id.getBytes(Charset.forName("UTF-8")));
        digester.update(typeName.getBytes(Charset.forName("UTF-8")));
        byte[] digest = digester.digest();
        return SHA256Utils.toString(digest);
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
