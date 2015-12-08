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
import org.apache.atlas.utils.MD5Utils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

public class Id implements ITypedReferenceableInstance {

    public final String id;
    public final String className;
    public final int version;

    public Id(String id, int version, String className) {
        ParamChecker.notEmpty(className, "id");
        ParamChecker.notEmpty(className, "className");
        this.id = id;
        this.className = className;
        this.version = version;
    }

    public Id(long id, int version, String className) {
        this("" + id, version, className);
    }

    public Id(String className) {
        this("" + (-System.nanoTime()), 0, className);
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

    public String toString() {
        return String.format("(type: %s, id: %s)", className, isUnassigned() ? "<unassigned>" : "" + id);
    }

    public String getClassName() {
        return className;
    }

    public int getVersion() {
        return version;
    }

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

        Id id1 = (Id) o;

        if (version != id1.version) {
            return false;
        }
        if (!className.equals(id1.className)) {
            return false;
        }
        if (!id.equals(id1.id)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + className.hashCode();
        result = 31 * result + version;
        return result;
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
        return className;
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

    @Override
    public String getSignatureHash(MessageDigest digester) throws AtlasException {
        digester.update(id.getBytes(Charset.forName("UTF-8")));
        digester.update(className.getBytes(Charset.forName("UTF-8")));
        byte[] digest = digester.digest();
        return MD5Utils.toString(digest);
    }
}
