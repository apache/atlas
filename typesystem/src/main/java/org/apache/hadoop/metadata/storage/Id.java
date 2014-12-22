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

package org.apache.hadoop.metadata.storage;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.metadata.IReferenceableInstance;
import org.apache.hadoop.metadata.IStruct;
import org.apache.hadoop.metadata.ITypedReferenceableInstance;
import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.types.FieldMapping;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

public class Id implements ITypedReferenceableInstance {

    public final long id;
    public final String className;
    public final int version;

    public Id(long id, int version, String className) {
        this.id = id;
        this.className = className;
        this.version = version;
    }

    public Id(String className) {
        this(-System.currentTimeMillis(), 0, className);
    }

    public boolean isUnassigned() {
        return id < 0;
    }

    public String toString() {
        return String.format("(type: %s, id: %s)", className, isUnassigned() ? "<unassigned>" : "" + id);
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
    public Object get(String attrName) throws MetadataException {
        throw new MetadataException("Get/Set not supported on an Id object");
    }

    @Override
    public void set(String attrName, Object val) throws MetadataException {
        throw new MetadataException("Get/Set not supported on an Id object");
    }

    @Override
    public FieldMapping fieldMapping() {
        return null;
    }

    public void setNull(String attrName) throws MetadataException {
        set(attrName, null);
    }

    public boolean getBoolean(String attrName) throws MetadataException {
        throw new MetadataException("Get/Set not supported on an Id object");
    }

    public byte getByte(String attrName) throws MetadataException {
        throw new MetadataException("Get/Set not supported on an Id object");
    }

    public short getShort(String attrName) throws MetadataException {
        throw new MetadataException("Get/Set not supported on an Id object");
    }

    public int getInt(String attrName) throws MetadataException {
        throw new MetadataException("Get/Set not supported on an Id object");
    }

    public long getLong(String attrName) throws MetadataException {
        throw new MetadataException("Get/Set not supported on an Id object");
    }

    public float getFloat(String attrName) throws MetadataException {
        throw new MetadataException("Get/Set not supported on an Id object");
    }

    public double getDouble(String attrName) throws MetadataException {
        throw new MetadataException("Get/Set not supported on an Id object");
    }

    public BigInteger getBigInt(String attrName) throws MetadataException {
        throw new MetadataException("Get/Set not supported on an Id object");
    }

    public BigDecimal getBigDecimal(String attrName) throws MetadataException {
        throw new MetadataException("Get/Set not supported on an Id object");
    }

    public Date getDate(String attrName) throws MetadataException {
        throw new MetadataException("Get/Set not supported on an Id object");
    }

    public String getString(String attrName) throws MetadataException {
        throw new MetadataException("Get/Set not supported on an Id object");
    }

    public void setBoolean(String attrName, boolean val) throws MetadataException {
        throw new MetadataException("Get/Set not supported on an Id object");
    }

    public void setByte(String attrName, byte val) throws MetadataException {
        throw new MetadataException("Get/Set not supported on an Id object");
    }

    public void setShort(String attrName, short val) throws MetadataException {
        throw new MetadataException("Get/Set not supported on an Id object");
    }

    public void setInt(String attrName, int val) throws MetadataException {
        throw new MetadataException("Get/Set not supported on an Id object");
    }

    public void setLong(String attrName, long val) throws MetadataException {
        throw new MetadataException("Get/Set not supported on an Id object");
    }

    public void setFloat(String attrName, float val) throws MetadataException {
        throw new MetadataException("Get/Set not supported on an Id object");
    }

    public void setDouble(String attrName, double val) throws MetadataException {
        throw new MetadataException("Get/Set not supported on an Id object");
    }

    public void setBigInt(String attrName, BigInteger val) throws MetadataException {
        throw new MetadataException("Get/Set not supported on an Id object");
    }

    public void setBigDecimal(String attrName, BigDecimal val) throws MetadataException {
        throw new MetadataException("Get/Set not supported on an Id object");
    }

    public void setDate(String attrName, Date val) throws MetadataException {
        throw new MetadataException("Get/Set not supported on an Id object");
    }

    public void setString(String attrName, String val) throws MetadataException {
        throw new MetadataException("Get/Set not supported on an Id object");
    }
}
