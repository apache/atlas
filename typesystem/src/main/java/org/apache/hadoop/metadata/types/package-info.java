/*
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

/**
 * <h2>Types:</h2>
 * <img src="doc-files/dataTypes.png" />
 * <ul>
 *     <li> <b>IDataType:</b> Represents a <i>DataType</i> in the TypeSystem. All Instances and Attributes are associated
 *     with a DataType. They represent the <b>Set</b> of values that Instances/Attributes of this type can have.
 *     Currently the namespace of DataTypes is flat. DataTypes can be asked to <i>convert</i> arbitrary java Objects
 *     to instances of this type, and they can be asked for a String representation of an instance.</li>
 *     <li><b>Type Categories:</b></li> DataTypes are grouped into Categories. A Category implies certain semantics about
 *     the Types belonging to the Category. We have PRIMITIVE, ENUM, ARRAY, MAP, STRUCT, TRAIT, and CLASS categories.
 *     <li><b>Primitive Types:</b> There are corresponding DataTypes for the java primitives: Boolean, Byte, Short,
 *     Int, Long, Float, Double. We also support BigInteger, BigDecimal, String, and Date</li>
 *     <li><b>Collection Types:</b>ArrayType and MapType are parameterized DataTypes taking one and two parameters
 *     respectively.</li>
 *     <li><b>Enum Types:</b> Used to define DataTypes with all valid values listed in the Type definition. For e.g.
 * <pre>
 * {@code
 * ts.defineEnumType("HiveObjectType",
        new EnumValue("GLOBAL", 1),
        new EnumValue("DATABASE", 2),
        new EnumValue("TABLE", 3),
        new EnumValue("PARTITION", 4),
        new EnumValue("COLUMN", 5))
 * }
 * </pre> Each <i>EnumValue</i> has name and an ordinal. Either one can be used as a value for an Attribute of this Type.
 *     </li>
 *     <li><b>Constructable Types:</b> Are complex Types that are composed of Attributes. We support Structs, Classes
 *     and Traits constructable types. A ConstructableType is parameterized by the Type of its <i>Instance</i> java
 *     class(these are implementations of the ITypedInstance interface). A value of the IConstructableType will
 *     implement this parameterized Type. IConstructableTypes can be asked to create an 'empty' instance of their Type.
 *     IConstructableTypes are associated with FieldMappings that encapsulate the mapping from/to the ITypedInstance
 *     java object.
 *     </li>
 *     <li><b>Attribute Info:</b>Represents an Attribute of a complex datatype. Attributes are defined by a name, a
 *     dataType, its Multiplicity and whether it is a composite relation. <i>Multiplicity</i> is a constraint on the
 *     number of instances that an instance can have. For non collection types and Maps: Multiplicity is OPTIONAL or
 *     REQUIRED.
 *     For Arrays the Multiplicity is specified by a lower-bound, upper-bound and a uniqueness constraint.
 *     </li>
 *     <li><b>Struct Types:</b>Are IConstructableTypes whose instances are IStructs. Conceptually these are like 'C'
 *     structs: they represent a collection of Attributes. For e.g.
 * <pre>
 * {@code
 * ts.defineStructType(STRUCT_TYPE_1,
        true,
        createRequiredAttrDef("a", DataTypes.INT_TYPE),
    createOptionalAttrDef("b", DataTypes.BOOLEAN_TYPE),
    createOptionalAttrDef("c", DataTypes.BYTE_TYPE),
    createOptionalAttrDef("d", DataTypes.SHORT_TYPE),
    createOptionalAttrDef("e", DataTypes.INT_TYPE),
    createOptionalAttrDef("f", DataTypes.INT_TYPE),
    createOptionalAttrDef("g", DataTypes.LONG_TYPE),
    createOptionalAttrDef("h", DataTypes.FLOAT_TYPE),
    createOptionalAttrDef("i", DataTypes.DOUBLE_TYPE),
    createOptionalAttrDef("j", DataTypes.BIGINTEGER_TYPE),
    createOptionalAttrDef("k", DataTypes.BIGDECIMAL_TYPE),
    createOptionalAttrDef("l", DataTypes.DATE_TYPE),
    createOptionalAttrDef("m", ts.defineArrayType(DataTypes.INT_TYPE)),
    createOptionalAttrDef("n", ts.defineArrayType(DataTypes.BIGDECIMAL_TYPE)),
    createOptionalAttrDef("o", ts.defineMapType(DataTypes.STRING_TYPE, DataTypes.DOUBLE_TYPE))
 * }
 * </pre>
 *     </li>
 *     <li><b>Hierarchical Types:</b>Are DataTypes that can have a SuperType. Classes and Traits are the supported
 *     Hierarchical Types. </li>
 *     <li><b>Class Types:</b></li>
 *     <li><b>Trait Types:</b></li>
 * </ul>
 *
 *
 * <h2>Instances:</h2>
 * <img src="doc-files/instance.png" />
 * <ul>
 *     <li> <b>IStruct:</b></li>
 *     <li><b>IReferenceableInstance:</b></li>
 *     <li><b>ITypedStruct:</b></li>
 *     <li><b>ITypedReferenceableInstance:</b></li>
 * </ul>
 *
 * <h3>Serialization of Types:</h3>
 *
 * <h3>Serialization of Instances:</h3>
 *
 * <h3>Searching on Classes and Traits:</h3>
 */
package org.apache.hadoop.metadata.types;