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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.atlas.AtlasException;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.persistence.DownCastStructInstance;
import org.apache.atlas.typesystem.types.TypeUtils.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * Represents a Type that can have SuperTypes. An Instance of the HierarchicalType can be
 * downcast to a SuperType.
 * @param <ST> the Type of the SuperType. TraitTypes have TraitTypes as SuperTypes, ClassTypes
 *            have ClassTypes
 *            as SuperTypes.
 * @param <T> the class of the Instance of this DataType.
 */
public abstract class HierarchicalType<ST extends HierarchicalType, T> extends AbstractDataType<T>
        implements Comparable<ST> {

    public final TypeSystem typeSystem;
    public final Class<ST> superTypeClass;
    public final String name;
    public final FieldMapping fieldMapping;
    public final int numFields;
    public final ImmutableList<String> superTypes;
    public final ImmutableList<AttributeInfo> immediateAttrs;
    public final ImmutableMap<String, String> attributeNameToType;
    protected ImmutableMap<String, List<Path>> superTypePaths;
    protected ImmutableMap<String, Path> pathNameToPathMap;

    /**
     * Used when creating a Type, to support recursive Structs.
     */
    HierarchicalType(TypeSystem typeSystem, Class<ST> superTypeClass, String name, ImmutableList<String> superTypes,
            int numFields) {
        this.typeSystem = typeSystem;
        this.superTypeClass = superTypeClass;
        this.name = name;
        this.fieldMapping = null;
        this.numFields = numFields;
        this.superTypes = superTypes;
        this.immediateAttrs = ImmutableList.of();
        this.attributeNameToType = null;
    }

    HierarchicalType(TypeSystem typeSystem, Class<ST> superTypeClass, String name, ImmutableList<String> superTypes,
            AttributeInfo... fields) throws AtlasException {
        this.typeSystem = typeSystem;
        this.superTypeClass = superTypeClass;
        this.name = name;
        Pair<FieldMapping, ImmutableMap<String, String>> p = constructFieldMapping(superTypes, fields);
        this.fieldMapping = p.left;
        this.attributeNameToType = p.right;
        this.numFields = this.fieldMapping.fields.size();
        this.superTypes = superTypes == null ? ImmutableList.<String>of() : superTypes;
        this.immediateAttrs = ImmutableList.copyOf(fields);
    }

    @Override
    public String getName() {
        return name;
    }

    public FieldMapping fieldMapping() {
        return fieldMapping;
    }

    /**
     * Given type must be a SubType of this type.
     * @param typeName
     * @throws AtlasException
     */
    public boolean isSubType(String typeName) throws AtlasException {
        HierarchicalType cType = typeSystem.getDataType(HierarchicalType.class, typeName);
        return (cType == this || cType.superTypePaths.containsKey(getName()));
    }

    /**
     * Validate that current definition can be updated with the new definition
     * @param newType
     * @return true if the current definition can be updated with the new definition, else false
     */
    @Override
    public void validateUpdate(IDataType newType) throws TypeUpdateException {
        super.validateUpdate(newType);

        HierarchicalType newHierarchicalType = (HierarchicalType) newType;

        //validate on supertypes
        if (!newHierarchicalType.superTypes.containsAll(superTypes)) {
            throw new TypeUpdateException(newType, "New type doesn't contain all super types of old type");
        }

        //validate on fields
        try {
            TypeUtils.validateUpdate(fieldMapping, newHierarchicalType.fieldMapping);
        } catch (TypeUpdateException e) {
            throw new TypeUpdateException(newType, e);
        }
    }

    protected void setupSuperTypesGraph() throws AtlasException {
        setupSuperTypesGraph(superTypes);
    }

    private void setupSuperTypesGraph(ImmutableList<String> superTypes) throws AtlasException {
        Map<String, List<Path>> superTypePaths = new HashMap<String, List<Path>>();
        Map<String, Path> pathNameToPathMap = new HashMap<String, Path>();
        Queue<Path> queue = new LinkedList<Path>();
        queue.add(new Node(getName()));
        while (!queue.isEmpty()) {
            Path currentPath = queue.poll();

            ST superType = currentPath.typeName == getName() ? (ST) this :
                    (ST) typeSystem.getDataType(superTypeClass, currentPath.typeName);

            pathNameToPathMap.put(currentPath.pathName, currentPath);
            if (superType != this) {
                List<Path> typePaths = superTypePaths.get(superType.getName());
                if (typePaths == null) {
                    typePaths = new ArrayList<Path>();
                    superTypePaths.put(superType.getName(), typePaths);
                }
                typePaths.add(currentPath);
            }

            ImmutableList<String> sTs = superType == this ? superTypes : superType.superTypes;

            if (sTs != null) {
                for (String sT : sTs) {
                    queue.add(new Path(sT, currentPath));
                }
            }
        }

        this.superTypePaths = ImmutableMap.copyOf(superTypePaths);
        this.pathNameToPathMap = ImmutableMap.copyOf(pathNameToPathMap);

    }

    protected Pair<FieldMapping, ImmutableMap<String, String>> constructFieldMapping(ImmutableList<String> superTypes,
            AttributeInfo... fields) throws AtlasException {

        Map<String, AttributeInfo> fieldsMap = new LinkedHashMap();
        Map<String, Integer> fieldPos = new HashMap();
        Map<String, Integer> fieldNullPos = new HashMap();
        Map<String, String> attributeNameToType = new HashMap<>();

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
        int numReferenceables = 0;

        setupSuperTypesGraph(superTypes);

        Iterator<Path> pathItr = pathIterator();
        while (pathItr.hasNext()) {
            Path currentPath = pathItr.next();

            ST superType = currentPath.typeName == getName() ? (ST) this :
                    (ST) typeSystem.getDataType(superTypeClass, currentPath.typeName);

            ImmutableList<AttributeInfo> superTypeFields =
                    superType == this ? ImmutableList.copyOf(fields) : superType.immediateAttrs;

            Set<String> immediateFields = new HashSet<String>();

            for (AttributeInfo i : superTypeFields) {
                if (superType == this) {
                    if (immediateFields.contains(i.name)) {
                        throw new AtlasException(String.format(
                                "Struct defintion cannot contain multiple fields with the" + " same name %s", i.name));
                    }
                    immediateFields.add(i.name);
                }

                String attrName = i.name;
                if (fieldsMap.containsKey(attrName)) {
                    attrName = currentPath.addOverrideAttr(attrName);
                }
                attributeNameToType.put(attrName, superType.getName());

                fieldsMap.put(attrName, i);
                fieldNullPos.put(attrName, fieldNullPos.size());
                if (i.dataType() == DataTypes.BOOLEAN_TYPE) {
                    fieldPos.put(attrName, numBools);
                    numBools++;
                } else if (i.dataType() == DataTypes.BYTE_TYPE) {
                    fieldPos.put(attrName, numBytes);
                    numBytes++;
                } else if (i.dataType() == DataTypes.SHORT_TYPE) {
                    fieldPos.put(attrName, numShorts);
                    numShorts++;
                } else if (i.dataType() == DataTypes.INT_TYPE) {
                    fieldPos.put(attrName, numInts);
                    numInts++;
                } else if (i.dataType() == DataTypes.LONG_TYPE) {
                    fieldPos.put(attrName, numLongs);
                    numLongs++;
                } else if (i.dataType() == DataTypes.FLOAT_TYPE) {
                    fieldPos.put(attrName, numFloats);
                    numFloats++;
                } else if (i.dataType() == DataTypes.DOUBLE_TYPE) {
                    fieldPos.put(attrName, numDoubles);
                    numDoubles++;
                } else if (i.dataType() == DataTypes.BIGINTEGER_TYPE) {
                    fieldPos.put(attrName, numBigInts);
                    numBigInts++;
                } else if (i.dataType() == DataTypes.BIGDECIMAL_TYPE) {
                    fieldPos.put(attrName, numBigDecimals);
                    numBigDecimals++;
                } else if (i.dataType() == DataTypes.DATE_TYPE) {
                    fieldPos.put(attrName, numDates);
                    numDates++;
                } else if (i.dataType() == DataTypes.STRING_TYPE) {
                    fieldPos.put(attrName, numStrings);
                    numStrings++;
                } else if (i.dataType().getTypeCategory() == DataTypes.TypeCategory.ENUM) {
                    fieldPos.put(i.name, numInts);
                    numInts++;
                } else if (i.dataType().getTypeCategory() == DataTypes.TypeCategory.ARRAY) {
                    fieldPos.put(attrName, numArrays);
                    numArrays++;
                } else if (i.dataType().getTypeCategory() == DataTypes.TypeCategory.MAP) {
                    fieldPos.put(attrName, numMaps);
                    numMaps++;
                } else if (i.dataType().getTypeCategory() == DataTypes.TypeCategory.STRUCT
                        || i.dataType().getTypeCategory() == DataTypes.TypeCategory.TRAIT) {
                    fieldPos.put(attrName, numStructs);
                    numStructs++;
                } else if (i.dataType().getTypeCategory() == DataTypes.TypeCategory.CLASS) {
                    fieldPos.put(attrName, numReferenceables);
                    numReferenceables++;
                } else {
                    throw new AtlasException(String.format("Unknown datatype %s", i.dataType()));
                }
            }
        }

        this.superTypePaths = ImmutableMap.copyOf(superTypePaths);
        this.pathNameToPathMap = ImmutableMap.copyOf(pathNameToPathMap);

        FieldMapping fm =
                new FieldMapping(fieldsMap, fieldPos, fieldNullPos, numBools, numBytes, numShorts, numInts, numLongs,
                        numFloats, numDoubles, numBigInts, numBigDecimals, numDates, numStrings, numArrays, numMaps,
                        numStructs, numReferenceables);

        return new Pair(fm, ImmutableMap.copyOf(attributeNameToType));
    }

    public IStruct castAs(IStruct s, String superTypeName) throws AtlasException {

        if (!superTypePaths.containsKey(superTypeName)) {
            throw new AtlasException(String.format("Cannot downcast to %s from type %s", superTypeName, getName()));
        }

        if (s != null) {
            if (s.getTypeName() != getName()) {
                throw new AtlasException(
                        String.format("Downcast called on wrong type %s, instance type is %s", getName(),
                                s.getTypeName()));
            }

            List<Path> pathToSuper = superTypePaths.get(superTypeName);
            if (pathToSuper.size() > 1) {
                throw new AtlasException(String.format(
                        "Cannot downcast called to %s, from %s: there are multiple paths " + "to SuperType",
                        superTypeName, getName()));
            }

            ST superType = (ST) typeSystem.getDataType(superTypeClass, superTypeName);
            Map<String, String> downCastMap = superType.constructDowncastFieldMap(this, pathToSuper.get(0));
            return new DownCastStructInstance(superTypeName, new DownCastFieldMapping(ImmutableMap.copyOf(downCastMap)),
                    s);
        }

        return null;
    }

    public ST getDefinedType(String attrName) throws AtlasException {
        if (!attributeNameToType.containsKey(attrName)) {
            throw new AtlasException(String.format("Unknown attribute %s in type %s", attrName, getName()));
        }
        return typeSystem.getDataType(superTypeClass, attributeNameToType.get(attrName));
    }

    public String getDefinedTypeName(String attrName) throws AtlasException {
        return getDefinedType(attrName).getName();
    }

    public String getQualifiedName(String attrName) throws AtlasException {
        String attrTypeName = getDefinedTypeName(attrName);
        return attrName.contains(".") ? attrName : String.format("%s.%s", attrTypeName, attrName);
    }

    protected Map<String, String> constructDowncastFieldMap(ST subType, Path pathToSubType) {

        String pathToSubTypeName = pathToSubType.pathAfterThis;
        /*
         * the downcastMap;
         */
        Map<String, String> dCMap = new HashMap<String, String>();
        Iterator<Path> itr = pathIterator();
        while (itr.hasNext()) {
            Path p = itr.next();
            Path pInSubType = (Path) subType.pathNameToPathMap.get(p.pathName + "." + pathToSubTypeName);

            if (pInSubType.hiddenAttributeMap != null) {
                for (Map.Entry<String, String> e : pInSubType.hiddenAttributeMap.entrySet()) {
                    String mappedInThisType =
                            p.hiddenAttributeMap != null ? p.hiddenAttributeMap.get(e.getKey()) : null;
                    if (mappedInThisType == null) {
                        dCMap.put(e.getKey(), e.getValue());
                    } else {
                        dCMap.put(mappedInThisType, e.getValue());
                    }
                }
            }
        }
        return dCMap;
    }

    @Override
    public int compareTo(ST o) {
        String oName = o.getName();
        try {
            if (o.isSubType(getName())) {
                return 1;
            } else if (isSubType(oName)) {
                return -1;
            } else {
                return getName().compareTo(oName);
            }
        } catch(AtlasException e) {
            throw new RuntimeException(e);
        }
    }

    public Set<String> getAllSuperTypeNames() {
        return superTypePaths.keySet();
    }

    public Iterator<Path> pathIterator() {
        return new PathItr();
    }

    static class Path {
        public final String typeName;
        public final String pathName;
        public final String pathAfterThis;
        private final Path subTypePath;
        /*
         * name mapping for attributes hidden by a SubType.
         */ Map<String, String> hiddenAttributeMap;

        Path(String typeName, Path childPath) throws AtlasException {
            this.typeName = typeName;
            this.subTypePath = childPath;
            if (childPath.contains(typeName)) {
                throw new CyclicTypeDefinition(this);
            }
            pathName = String.format("%s.%s", typeName, childPath.pathName);
            pathAfterThis = childPath.pathName;
        }

        Path(String typeName) {
            assert getClass() == Node.class;
            this.typeName = typeName;
            this.subTypePath = null;
            pathName = typeName;
            pathAfterThis = null;
        }

        public boolean contains(String typeName) {
            return this.typeName.equals(typeName) || (subTypePath != null && subTypePath.contains(typeName));
        }

        public String pathString(String nodeSep) {

            StringBuilder b = new StringBuilder();
            Path p = this;

            while (p != null) {
                b.append(p.typeName);
                p = p.subTypePath;
                if (p != null) {
                    b.append(nodeSep);
                }
            }
            return b.toString();
        }

        String addOverrideAttr(String name) {
            hiddenAttributeMap = hiddenAttributeMap == null ? new HashMap<String, String>() : hiddenAttributeMap;
            String oName = pathName + "." + name;
            hiddenAttributeMap.put(name, oName);
            return oName;
        }
    }

    static class Node extends Path {
        Node(String typeName) {
            super(typeName);
        }
    }

    static class CyclicTypeDefinition extends AtlasException {

        CyclicTypeDefinition(Path p) {
            super(String.format("Cycle in Type Definition %s", p.pathString(" -> ")));
        }
    }

    class PathItr implements Iterator<Path> {

        Queue<Path> pathQueue;

        PathItr() {
            pathQueue = new LinkedList<Path>();
            pathQueue.add(pathNameToPathMap.get(getName()));
        }

        @Override
        public boolean hasNext() {
            return !pathQueue.isEmpty();
        }

        @Override
        public Path next() {
            Path p = pathQueue.poll();
            ST t = null;
            try {
                t = (ST) typeSystem.getDataType(superTypeClass, p.typeName);
            } catch (AtlasException me) {
                throw new RuntimeException(me);
            }
            if (t.superTypes != null) {
                ImmutableList<String> sTs = t.superTypes;
                for (String sT : sTs) {
                    String nm = sT + "." + p.pathName;
                    pathQueue.add(pathNameToPathMap.get(nm));
                }
            }
            return p;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }


}
