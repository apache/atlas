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

package org.apache.atlas.tools.thrift

import com.google.common.collect.ImmutableList
import org.apache.atlas.MetadataException
import org.apache.atlas.typesystem.TypesDef
import org.apache.atlas.typesystem.types.{DataTypes, HierarchicalTypeDefinition, Multiplicity, TraitType, _}
import org.slf4j.{Logger, LoggerFactory}

import scala.io.Source
import scala.util.{Failure, Success, Try}


case class CompositeRelation(typeName: String, fieldName: String, reverseFieldName: Option[String])

/**
 * Convert a [[ThriftDef ThriftDef]] to
 * [[TypesDef TypesDef]]. Currently there are several restrictions:
 *
 * - CppIncludes, SEnums are not allowed
 * - The only include allowed is that of "share/fb303/if/fb303.thrift". This include is ignored.
 * Any other include will trigger an exception
 * - Namespaces, TypeDefs, Contants, Unions, Exceptions, and Service definitions are ignored.
 * - So for fields typeDefs are not applied.
 * - Field Constant values are ignored.
 * - Type Annotations, XSD information is ignored.
 *
 * Thrift Structs can be mapped to Structs, Traits for Classes. The caller can specify their preference by
 * providing the structNames, classNames and thriftNames parameters. A Struct that is not in one of these 3
 * lists is not mapped.
 *
 * The ThriftDef doesn't specify if a relationship is composite. For e.g. in the thrift definition
 * {{{
 *     struct Person {
        1: string       name,
        2: Address   addr,
      }
      struct Address {
        1: string       street,
        2: string   city,
      }
 * }}}
 *
 * If Person and Address are mapped to classes, you may not to make the Person -> Address a Composite relation.
 * The caller can specify these in the 'compositeRelations' parameter.
 *
 */
class ThriftTypesGen(val structNames: List[String], val classNames: List[String], val traitNames: List[String],
                     val compositeRelations: List[CompositeRelation]) {

    private val LOG: Logger = LoggerFactory.getLogger(classOf[ThriftTypesGen])
    private val FB_INCLUDE = "share/fb303/if/fb303.thrift"
    /**
     * for a (typeName, fieldName) specifies (isComposite, reverseName)
     * if entry doesn't exist than field is not composite.
     */
    private var compositeRelsMap: Map[(String, String), (Boolean, Option[String])] = Map()

    def apply(thriftResource: String): TypesDef = {
        val tDef = parseThrift(thriftResource)

        tDef.flatMap(buildCompositeRelations).flatMap(typesDef) match {
            case Success(t) => t
            case Failure(v) => throw v
        }

    }

    def buildCompositeRelations(thriftDef: ThriftDef): Try[ThriftDef] = Try {

        compositeRelations.foreach { cr =>

            val sDef = thriftDef.structs.find(_.name == cr.typeName)
            if (!sDef.isDefined) {
                throw new MetadataException(s"Unknown Struct (${cr.typeName}) specified in CompositeRelation")

            }
            val fDef = sDef.get.fields.find(_.name == cr.fieldName)
            if (!fDef.isDefined) {
                throw new MetadataException(s"Unknown Field (${cr.fieldName}) specified in CompositeRelation")

            }

            compositeRelsMap = compositeRelsMap + ((cr.typeName, cr.fieldName) ->(true, cr.reverseFieldName))

            if (cr.reverseFieldName.isDefined) {
                val reverseStructName = dataTypeName(fDef.get.fieldType)
                val reverseStructDef = thriftDef.structs.find(_.name == reverseStructName)
                if (!reverseStructDef.isDefined) {
                    throw new MetadataException(s"Cannot find Struct $reverseStructName in CompositeRelation $cr")
                }
                val rfDef = reverseStructDef.get.fields.find(_.name == cr.reverseFieldName)
                if (!rfDef.isDefined) {
                    throw new MetadataException(s"Unknown Reverse Field (${cr.reverseFieldName}) specified in CompositeRelation")
                }

                List(cr, CompositeRelation(reverseStructName, cr.reverseFieldName.get, Some(cr.fieldName)))

                compositeRelsMap = compositeRelsMap +
                    ((reverseStructName, cr.reverseFieldName.get) ->(false, Some(cr.fieldName)))
            }
        }

        thriftDef
    }

    def typesDef(thriftDef: ThriftDef): Try[TypesDef] = {
        var tDef: Try[TypesDef] = Try {
            TypesDef(Seq(), Seq(), Seq(), Seq())
        }

        tDef.flatMap((t: TypesDef) => includes(t, thriftDef.includes)).flatMap((t: TypesDef) => cppIncludes(t, thriftDef.cppIncludes))

        tDef = tDef.flatMap((t: TypesDef) => includes(t, thriftDef.includes)).
            flatMap((t: TypesDef) => cppIncludes(t, thriftDef.cppIncludes)).
            flatMap((t: TypesDef) => namespaces(t, thriftDef.namespaces)).
            flatMap((t: TypesDef) => constants(t, thriftDef.constants)).
            flatMap((t: TypesDef) => senums(t, thriftDef.senums)).
            flatMap((t: TypesDef) => enums(t, thriftDef.enums)).
            flatMap((t: TypesDef) => structs(t, thriftDef.structs)).
            flatMap((t: TypesDef) => unions(t, thriftDef.unions)).
            flatMap((t: TypesDef) => exceptions(t, thriftDef.xceptions)).
            flatMap((t: TypesDef) => services(t, thriftDef.services))


        tDef
    }

    private def parseThrift(thriftResource: String): Try[ThriftDef] = {
        Try {
            LOG.debug("Parsing Thrift resource {}", thriftResource)
            val is = getClass().getResourceAsStream(thriftResource)
            val src: Source = Source.fromInputStream(is)
            val thriftStr: String = src.getLines().mkString("\n")
            val p = new ThriftParser
            var thriftDef: Option[ThriftDef] = p(thriftStr)
            thriftDef match {
                case Some(s) => s
                case None => {
                    LOG.debug("Parse for thrift resource {} failed", thriftResource)
                    throw new MetadataException(s"Failed to parse thrift resource: $thriftResource")
                }
            }
        }
    }

    @throws[MetadataException]
    private def dataTypeName(fT: FieldType): String = fT match {
        case IdentifierType(n) => n
        case BaseType(typ, _) => BASE_TYPES.toPrimitiveTypeName(typ)
        case ListType(elemType, _, _) => DataTypes.arrayTypeName(dataTypeName(elemType))
        case SetType(elemType, _, _) => DataTypes.arrayTypeName(dataTypeName(elemType))
        case MapType(keyType, valueType, _, _) => DataTypes.mapTypeName(dataTypeName(keyType), dataTypeName(valueType))
    }

    private def enumValue(e: EnumValueDef, defId: Int): EnumValue = e match {
        case EnumValueDef(value, Some(id), _) => new EnumValue(value, id.value)
        case EnumValueDef(value, None, _) => new EnumValue(value, defId)
    }

    private def enumDef(td: TypesDef, e: EnumDef): Try[TypesDef] = {
        Success(
            td.copy(enumTypes = td.enumTypes :+
                new EnumTypeDefinition(e.name, e.enumValues.zipWithIndex.map(t => enumValue(t._1, -t._2)): _*))
        )
    }

    private def includeDef(td: TypesDef, i: IncludeDef): Try[TypesDef] = {
        Try {
            if (i.value != FB_INCLUDE) {
                throw new MetadataException(s"Unsupported Include ${i.value}, only fb303.thrift is currently allowed.")
            }
            td
        }
    }

    private def cppIncludeDef(td: TypesDef, i: CppIncludeDef): Try[TypesDef] = {
        Try {
            throw new MetadataException(s"Unsupported CppInclude ${i.value}.")
        }
    }

    private def namespaceDef(td: TypesDef, i: NamespaceDef): Try[TypesDef] = {
        Try {
            LOG.debug(s"Ignoring Namespace definition $i")
            td
        }
    }

    private def constantDef(td: TypesDef, i: ConstDef): Try[TypesDef] = {
        Try {
            LOG.debug(s"Ignoring ConstantDef definition $i")
            td
        }
    }

    private def senumDef(td: TypesDef, i: SEnumDef): Try[TypesDef] = {
        Try {
            throw new MetadataException(s"Unsupported SEnums ${i}.")
        }
    }

    private def fieldDef(typName: String, fd: FieldDef): AttributeDefinition = {
        val name: String = fd.name
        val dTName: String = dataTypeName(fd.fieldType)

        var m: Multiplicity = Multiplicity.OPTIONAL

        if (fd.requiredNess) {
            m = Multiplicity.REQUIRED
        }

        fd.fieldType match {
            case _: ListType => m = Multiplicity.COLLECTION
            case _: SetType => m = Multiplicity.SET
            case _ => ()
        }

        var isComposite = false
        var reverseAttrName: String = null

        val r = compositeRelsMap.get((typName, name))
        if (r.isDefined) {
            isComposite = r.get._1
            if (r.get._2.isDefined) {
                reverseAttrName = r.get._2.get
            }
        }
        new AttributeDefinition(name, dTName, m, isComposite, reverseAttrName)
    }

    private def structDef(td: TypesDef, structDef: StructDef): Try[TypesDef] = Try {
        val typeName: String = structDef.name

        typeName match {
            case t if structNames contains t => td.copy(structTypes = td.structTypes :+
                new StructTypeDefinition(typeName, structDef.fields.map(fieldDef(typeName, _)).toArray))
            case t: String if traitNames contains t => {
                val ts = td.traitTypes :+
                    new HierarchicalTypeDefinition[TraitType](classOf[TraitType],
                        typeName, ImmutableList.of[String](), structDef.fields.map(fieldDef(typeName, _)).toArray)
                td.copy(traitTypes = ts)
            }
            case t: String if classNames contains t => {
                val cs = td.classTypes :+
                    new HierarchicalTypeDefinition[ClassType](classOf[ClassType],
                        typeName, ImmutableList.of[String](), structDef.fields.map(fieldDef(typeName, _)).toArray)
                td.copy(classTypes = cs)
            }
            case _ => td
        }
    }

    private def unionDef(td: TypesDef, i: UnionDef): Try[TypesDef] = {
        Try {
            LOG.debug(s"Ignoring Union definition $i")
            td
        }
    }

    private def exceptionDef(td: TypesDef, i: ExceptionDef): Try[TypesDef] = {
        Try {
            LOG.debug(s"Ignoring Exception definition $i")
            td
        }
    }

    private def serviceDef(td: TypesDef, i: ServiceDef): Try[TypesDef] = {
        Try {
            LOG.debug(s"Ignoring Service definition $i")
            td
        }
    }

    private def applyList[T](fn: (TypesDef, T) => Try[TypesDef])(td: TypesDef, l: List[T]): Try[TypesDef] = {
        l.foldLeft[Try[TypesDef]](Success(td))((b, a) => b.flatMap({ Unit => fn(td, a)}))
    }

    private def includes = applyList(includeDef) _

    private def cppIncludes = applyList(cppIncludeDef) _

    private def namespaces = applyList(namespaceDef) _

    private def constants = applyList(constantDef) _

    private def enums = applyList(enumDef) _

    private def senums = applyList(senumDef) _

    private def structs = applyList(structDef) _

    private def unions = applyList(unionDef) _

    private def exceptions = applyList(exceptionDef) _

    private def services = applyList(serviceDef) _

}
