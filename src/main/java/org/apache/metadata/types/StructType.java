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
package org.apache.metadata.types;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.metadata.IStruct;
import org.apache.metadata.ITypedStruct;
import org.apache.metadata.MetadataException;
import org.apache.metadata.Struct;
import org.apache.metadata.storage.StructInstance;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

public class StructType  extends AbstractDataType<IStruct>
        implements IConstructableType<IStruct, ITypedStruct> {

    public final ITypeBrowser typeSystem;
    public final String name;
    public final FieldMapping fieldMapping;
    public final int numFields;
    private final TypedStructHandler handler;

    /**
     * Used when creating a StructType, to support recursive Structs.
     */
    protected StructType(ITypeBrowser typeSystem, String name, int numFields) {
        this.typeSystem = typeSystem;
        this.name = name;
        this.fieldMapping = null;
        this.numFields = numFields;
        this.handler = null;
    }

    protected StructType(ITypeBrowser typeSystem, String name,
                         ImmutableList<String> superTypes, AttributeInfo... fields) throws MetadataException {
        this.typeSystem = typeSystem;
        this.name = name;
        this.fieldMapping = constructFieldMapping(superTypes,
                fields);
        this.numFields = this.fieldMapping.fields.size();
        this.handler = new TypedStructHandler(this);
    }

    public FieldMapping fieldMapping() {
        return fieldMapping;
    }

    @Override
    public String getName() {
        return name;
    }

    protected FieldMapping constructFieldMapping(ImmutableList<String> superTypes,
                                                 AttributeInfo... fields)
            throws MetadataException {

        Map<String,AttributeInfo> fieldsMap = new LinkedHashMap<String, AttributeInfo>();
        Map<String, Integer> fieldPos = new HashMap<String, Integer>();
        Map<String, Integer> fieldNullPos = new HashMap<String, Integer>();
        int numBools = 0;
        int numBytes = 0;
        int numShorts = 0;
        int numInts = 0;
        int numLongs = 0;
        int numFloats = 0;
        int numDoubles = 0;
        int numBigInts = 0;
        int numBigDecimals = 0;
        int numDates = 0;
        int numStrings = 0;
        int numArrays = 0;
        int numMaps = 0;
        int numStructs = 0;

        for(AttributeInfo i : fields) {
            if ( fieldsMap.containsKey(i.name) ) {
                throw new MetadataException(
                        String.format("Struct defintion cannot contain multiple fields with the same name %s", i.name));
            }
            fieldsMap.put(i.name, i);
            fieldNullPos.put(i.name, fieldNullPos.size());
            if ( i.dataType() == DataTypes.BOOLEAN_TYPE ) {
                fieldPos.put(i.name, numBools);
                numBools++;
            } else if ( i.dataType() == DataTypes.BYTE_TYPE ) {
                fieldPos.put(i.name, numBytes);
                numBytes++;
            } else if ( i.dataType() == DataTypes.SHORT_TYPE ) {
                fieldPos.put(i.name, numShorts);
                numShorts++;
            } else if ( i.dataType() == DataTypes.INT_TYPE ) {
                fieldPos.put(i.name, numInts);
                numInts++;
            } else if ( i.dataType() == DataTypes.LONG_TYPE ) {
                fieldPos.put(i.name, numLongs);
                numLongs++;
            } else if ( i.dataType() == DataTypes.FLOAT_TYPE ) {
                fieldPos.put(i.name, numFloats);
                numFloats++;
            } else if ( i.dataType() == DataTypes.DOUBLE_TYPE ) {
                fieldPos.put(i.name, numDoubles);
                numDoubles++;
            } else if ( i.dataType() == DataTypes.BIGINTEGER_TYPE ) {
                fieldPos.put(i.name, numBigInts);
                numBigInts++;
            } else if ( i.dataType() == DataTypes.BIGDECIMAL_TYPE ) {
                fieldPos.put(i.name, numBigDecimals);
                numBigDecimals++;
            } else if ( i.dataType() == DataTypes.DATE_TYPE ) {
                fieldPos.put(i.name, numDates);
                numDates++;
            } else if ( i.dataType() == DataTypes.STRING_TYPE ) {
                fieldPos.put(i.name, numStrings);
                numStrings++;
            } else if ( i.dataType().getTypeCategory() == DataTypes.TypeCategory.ARRAY ) {
                fieldPos.put(i.name, numArrays);
                numArrays++;
            } else if ( i.dataType().getTypeCategory() == DataTypes.TypeCategory.MAP ) {
                fieldPos.put(i.name, numMaps);
                numMaps++;
            } else if ( i.dataType().getTypeCategory() == DataTypes.TypeCategory.STRUCT ) {
                fieldPos.put(i.name, numStructs);
                numStructs++;
            } else {
                throw new MetadataException(String.format("Unknown datatype %s", i.dataType()));
            }
        }

        return new FieldMapping(fieldsMap,
                fieldPos,
                fieldNullPos,
                numBools,
                numBytes,
                numShorts,
                numInts,
                numLongs,
                numFloats,
                numDoubles,
                numBigInts,
                numBigDecimals,
                numDates,
                numStrings,
                numArrays,
                numMaps,
                numStructs);
    }


    @Override
    public DataTypes.TypeCategory getTypeCategory() {
        return DataTypes.TypeCategory.STRUCT;
    }

    @Override
    public ITypedStruct convert(Object val, Multiplicity m) throws MetadataException {
        return handler.convert(val, m);
    }

    public ITypedStruct createInstance() {
        return handler.createInstance();
    }

    @Override
    public void output(IStruct s, Appendable buf, String prefix) throws MetadataException {
        handler.output(s, buf, prefix);
    }
}
