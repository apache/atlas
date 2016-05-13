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

package org.apache.atlas.typesystem.types;

import org.apache.atlas.AtlasException;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.persistence.Id;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class FieldMapping {

    public final Map<String, AttributeInfo> fields;
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
            Map<String, Integer> fieldNullPos, int numBools, int numBytes, int numShorts, int numInts, int numLongs,
            int numFloats, int numDoubles, int numBigInts, int numBigDecimals, int numDates, int numStrings,
            int numArrays, int numMaps, int numStructs, int numReferenceables) {
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

    protected void outputFields(IStruct s, Appendable buf, String fieldPrefix, Set<? extends IStruct> inProcess) throws AtlasException {
        for (Map.Entry<String, AttributeInfo> e : fields.entrySet()) {
            String attrName = e.getKey();
            AttributeInfo i = e.getValue();
            Object aVal = s.get(attrName);
            TypeUtils.outputVal(attrName + " : ", buf, fieldPrefix);
            if (aVal != null && aVal instanceof Id) {
                TypeUtils.outputVal(aVal.toString(), buf, "");
            } else {
                i.dataType().output(aVal, buf, fieldPrefix, inProcess);
            }
            TypeUtils.outputVal("\n", buf, "");
        }
    }

    public void output(IStruct s, Appendable buf, String prefix, Set<IStruct> inProcess) throws AtlasException {
        if (s == null) {
            TypeUtils.outputVal("<null>\n", buf, "");
            return;
        }

        if (inProcess == null) {
            inProcess = new HashSet<>();
        }
        else if (inProcess.contains(s)) {
            // Avoid infinite recursion when structs reference each other.
            return;
        }
        inProcess.add(s);

        try {
            TypeUtils.outputVal("{", buf, prefix);

            TypeUtils.outputVal("\n", buf, "");
            String fieldPrefix = prefix + "\t";

            outputFields(s, buf, fieldPrefix, inProcess);

            TypeUtils.outputVal("}", buf, prefix);
        }
        finally {
            inProcess.remove(s);
        }
    }

    public void output(IReferenceableInstance s, Appendable buf, String prefix, Set<IReferenceableInstance> inProcess) throws AtlasException {
        if (s == null) {
            TypeUtils.outputVal("<null>\n", buf, "");
            return;
        }

        if (inProcess == null) {
            inProcess = new HashSet<>();
        }
        else if (inProcess.contains(s)) {
            // Avoid infinite recursion when structs reference each other.
            return;
        }
        inProcess.add(s);

        try {
            TypeUtils.outputVal("{", buf, prefix);

            TypeUtils.outputVal("\n", buf, "");
            String fieldPrefix = prefix + "\t";

            TypeUtils.outputVal("id : ", buf, fieldPrefix);
            TypeUtils.outputVal(s.getId().toString(), buf, "");
            TypeUtils.outputVal("\n", buf, "");

            outputFields(s, buf, fieldPrefix, inProcess);

            TypeSystem ts = TypeSystem.getInstance();

            for (String sT : s.getTraits()) {
                TraitType tt = ts.getDataType(TraitType.class, sT);
                TypeUtils.outputVal(sT + " : ", buf, fieldPrefix);
                tt.output(s.getTrait(sT), buf, fieldPrefix, null);
            }

            TypeUtils.outputVal("}", buf, prefix);
        }
        finally {
            inProcess.remove(s);
        }
    }

}
