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

import org.apache.atlas.AtlasException;
import org.apache.atlas.typesystem.types.FieldMapping;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.security.MessageDigest;
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

    boolean getBoolean(String attrName) throws AtlasException;

    byte getByte(String attrName) throws AtlasException;

    short getShort(String attrName) throws AtlasException;

    int getInt(String attrName) throws AtlasException;

    long getLong(String attrName) throws AtlasException;

    float getFloat(String attrName) throws AtlasException;

    double getDouble(String attrName) throws AtlasException;

    BigInteger getBigInt(String attrName) throws AtlasException;

    BigDecimal getBigDecimal(String attrName) throws AtlasException;

    Date getDate(String attrName) throws AtlasException;

    String getString(String attrName) throws AtlasException;

    void setBoolean(String attrName, boolean val) throws AtlasException;

    void setByte(String attrName, byte val) throws AtlasException;

    void setShort(String attrName, short val) throws AtlasException;

    void setInt(String attrName, int val) throws AtlasException;

    void setLong(String attrName, long val) throws AtlasException;

    void setFloat(String attrName, float val) throws AtlasException;

    void setDouble(String attrName, double val) throws AtlasException;

    void setBigInt(String attrName, BigInteger val) throws AtlasException;

    void setBigDecimal(String attrName, BigDecimal val) throws AtlasException;

    void setDate(String attrName, Date val) throws AtlasException;

    void setString(String attrName, String val) throws AtlasException;

    String getSignatureHash(MessageDigest digester) throws AtlasException;
}
