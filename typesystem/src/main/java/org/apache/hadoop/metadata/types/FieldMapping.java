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
package org.apache.hadoop.metadata.types;

import org.apache.hadoop.metadata.IReferenceableInstance;
import org.apache.hadoop.metadata.IStruct;
import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.storage.Id;

import java.util.Map;

public class FieldMapping {

    public final Map<String,AttributeInfo> fields;
    public final Map<String, Integer> fieldPos;
    public final Map<String, Integer> fieldNullPos;
    public final int numBools;
    public final int numBytes;
    public final int numShorts;
    public final int numInts;
    public final int numLongs;
    public final int numFloats;
    public final int numDoubles;
    public final int numBigInts;
    public final int numBigDecimals;
    public final int numDates;
    public final int numStrings;
    public final int numArrays;
    public final int numMaps;
    public final int numStructs;
    public final int numReferenceables;

    public FieldMapping(Map<String, AttributeInfo> fields, Map<String, Integer> fieldPos,
                        Map<String, Integer> fieldNullPos, int numBools, int numBytes, int numShorts,
                        int numInts, int numLongs, int numFloats, int numDoubles, int numBigInts, int numBigDecimals,
                        int numDates, int numStrings, int numArrays, int numMaps, int numStructs,
                        int numReferenceables) {
        this.fields = fields;
        this.fieldPos = fieldPos;
        this.fieldNullPos = fieldNullPos;
        this.numBools = numBools;
        this.numBytes = numBytes;
        this.numShorts = numShorts;
        this.numInts = numInts;
        this.numLongs = numLongs;
        this.numFloats = numFloats;
        this.numDoubles = numDoubles;
        this.numBigInts = numBigInts;
        this.numBigDecimals = numBigDecimals;
        this.numDates = numDates;
        this.numStrings = numStrings;
        this.numArrays = numArrays;
        this.numMaps = numMaps;
        this.numStructs = numStructs;
        this.numReferenceables = numReferenceables;
    }

    protected void outputFields(IStruct s, Appendable buf, String fieldPrefix) throws MetadataException {
        for(Map.Entry<String,AttributeInfo> e : fields.entrySet()) {
            String attrName = e.getKey();
            AttributeInfo i = e.getValue();
            Object aVal = s.get(attrName);
            TypeUtils.outputVal(attrName + " : ", buf, fieldPrefix);
            if ( aVal != null && aVal instanceof Id) {
                TypeUtils.outputVal(aVal.toString(), buf, "");
            } else {
                i.dataType().output(aVal, buf, fieldPrefix);
            }
            TypeUtils.outputVal("\n", buf, "");
        }
    }

    public void output(IStruct s, Appendable buf, String prefix) throws MetadataException {
        if ( s == null ) {
            TypeUtils.outputVal("<null>\n", buf, "");
            return;
        }
        TypeUtils.outputVal("{", buf, prefix);

        TypeUtils.outputVal("\n", buf, "");
        String fieldPrefix = prefix + "\t";

        outputFields(s, buf, fieldPrefix);

        TypeUtils.outputVal("}", buf, prefix);
    }

    public void output(IReferenceableInstance s, Appendable buf, String prefix) throws MetadataException {
        if ( s == null ) {
            TypeUtils.outputVal("<null>\n", buf, "");
            return;
        }
        TypeUtils.outputVal("{", buf, prefix);

        TypeUtils.outputVal("\n", buf, "");
        String fieldPrefix = prefix + "\t";

        TypeUtils.outputVal("id : ", buf, fieldPrefix);
        TypeUtils.outputVal(s.getId().toString(), buf, "");
        TypeUtils.outputVal("\n", buf, "");

        outputFields(s, buf, fieldPrefix);

        TypeSystem ts = TypeSystem.getInstance();

        for(String sT : s.getTraits() ) {
            TraitType tt = ts.getDataType(TraitType.class, sT);
            TypeUtils.outputVal(sT + " : ", buf, fieldPrefix);
            tt.output(s.getTrait(sT), buf, fieldPrefix);
        }

        TypeUtils.outputVal("}", buf, prefix);
    }

}
