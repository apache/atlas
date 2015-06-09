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

package org.apache.atlas.typesystem;

import org.apache.atlas.MetadataException;
import org.apache.atlas.typesystem.types.FieldMapping;

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

    void setNull(String attrName) throws MetadataException;

    boolean getBoolean(String attrName) throws MetadataException;

    byte getByte(String attrName) throws MetadataException;

    short getShort(String attrName) throws MetadataException;

    int getInt(String attrName) throws MetadataException;

    long getLong(String attrName) throws MetadataException;

    float getFloat(String attrName) throws MetadataException;

    double getDouble(String attrName) throws MetadataException;

    BigInteger getBigInt(String attrName) throws MetadataException;

    BigDecimal getBigDecimal(String attrName) throws MetadataException;

    Date getDate(String attrName) throws MetadataException;

    String getString(String attrName) throws MetadataException;

    void setBoolean(String attrName, boolean val) throws MetadataException;

    void setByte(String attrName, byte val) throws MetadataException;

    void setShort(String attrName, short val) throws MetadataException;

    void setInt(String attrName, int val) throws MetadataException;

    void setLong(String attrName, long val) throws MetadataException;

    void setFloat(String attrName, float val) throws MetadataException;

    void setDouble(String attrName, double val) throws MetadataException;

    void setBigInt(String attrName, BigInteger val) throws MetadataException;

    void setBigDecimal(String attrName, BigDecimal val) throws MetadataException;

    void setDate(String attrName, Date val) throws MetadataException;

    void setString(String attrName, String val) throws MetadataException;
}
