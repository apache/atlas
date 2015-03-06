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

package org.apache.hadoop.metadata.typesystem;

import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.typesystem.types.FieldMapping;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

/**
 * An instance whose structure is associated with a IDataType.
 * This is obtained by a call to 'createInstance' or the result of a Query.
 * A ITypedInstance can only contain information on attributes of the associated Type.
 * Instance can still be invalid because of missing required fields or incorrect multiplicity.
 * But user can only get/set on a known field of the associated type. Type values have to match
 * the IDataType of the associated attribute.
 */
public interface ITypedInstance extends IInstance {

    FieldMapping fieldMapping();

    public void setNull(String attrName) throws MetadataException;

    public boolean getBoolean(String attrName) throws MetadataException;

    public byte getByte(String attrName) throws MetadataException;

    public short getShort(String attrName) throws MetadataException;

    public int getInt(String attrName) throws MetadataException;

    public long getLong(String attrName) throws MetadataException;

    public float getFloat(String attrName) throws MetadataException;

    public double getDouble(String attrName) throws MetadataException;

    public BigInteger getBigInt(String attrName) throws MetadataException;

    public BigDecimal getBigDecimal(String attrName) throws MetadataException;

    public Date getDate(String attrName) throws MetadataException;

    public String getString(String attrName) throws MetadataException;

    public void setBoolean(String attrName, boolean val) throws MetadataException;

    public void setByte(String attrName, byte val) throws MetadataException;

    public void setShort(String attrName, short val) throws MetadataException;

    public void setInt(String attrName, int val) throws MetadataException;

    public void setLong(String attrName, long val) throws MetadataException;

    public void setFloat(String attrName, float val) throws MetadataException;

    public void setDouble(String attrName, double val) throws MetadataException;

    public void setBigInt(String attrName, BigInteger val) throws MetadataException;

    public void setBigDecimal(String attrName, BigDecimal val) throws MetadataException;

    public void setDate(String attrName, Date val) throws MetadataException;

    public void setString(String attrName, String val) throws MetadataException;
}
