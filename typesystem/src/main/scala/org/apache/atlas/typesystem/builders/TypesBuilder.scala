/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.typesystem.builders

import com.google.common.collect.ImmutableList
import org.apache.atlas.typesystem.TypesDef
import org.apache.atlas.typesystem.types._
import org.apache.atlas.typesystem.types.utils.TypesUtil
import scala.collection.mutable.ArrayBuffer
import scala.language.{dynamics, implicitConversions, postfixOps}
import scala.util.DynamicVariable
import com.google.common.collect.ImmutableSet

object TypesBuilder {

  case class Context(enums : ArrayBuffer[EnumTypeDefinition],
                      structs : ArrayBuffer[StructTypeDefinition],
                      classes : ArrayBuffer[HierarchicalTypeDefinition[ClassType]],
                      traits : ArrayBuffer[HierarchicalTypeDefinition[TraitType]],
                      currentTypeAttrs : ArrayBuffer[Attr] = null)

  class AttrOption()
  class ReverseAttributeName(val rNm : String) extends AttrOption
  class MultiplicityOption(val lower: Int, val upper: Int, val isUnique: Boolean) extends AttrOption

  val required = new AttrOption()
  val optional = new AttrOption()
  val collection = new AttrOption()
  val set = new AttrOption()
  val composite = new AttrOption()
  val unique = new AttrOption()
  val indexed = new AttrOption()
  def reverseAttributeName(rNm : String) = new ReverseAttributeName(rNm)
  def multiplicty(lower: Int, upper: Int, isUnique: Boolean) = new MultiplicityOption(lower, upper, isUnique)

  val boolean = DataTypes.BOOLEAN_TYPE.getName
  val byte = DataTypes.BYTE_TYPE.getName
  val short = DataTypes.SHORT_TYPE.getName
  val int = DataTypes.INT_TYPE.getName
  val long = DataTypes.LONG_TYPE.getName
  val float = DataTypes.FLOAT_TYPE.getName

  val double = DataTypes.DOUBLE_TYPE.getName
  val bigint = DataTypes.BIGINTEGER_TYPE.getName
  val bigdecimal = DataTypes.BIGDECIMAL_TYPE.getName
  val date = DataTypes.DATE_TYPE.getName
  val string = DataTypes.STRING_TYPE.getName

  def array(t : String) : String = {
    DataTypes.arrayTypeName(t)
  }

  def map(kt : String, vt : String) : String = {
    DataTypes.mapTypeName(kt, vt)
  }

  class Attr(ctx : Context, val name : String) {

    private var dataTypeName : String = DataTypes.BOOLEAN_TYPE.getName
    private var multiplicity: Multiplicity = Multiplicity.OPTIONAL
    private var isComposite: Boolean = false
    private var reverseAttributeName: String = null
    private var isUnique: Boolean = false
    private var isIndexable: Boolean = false

    ctx.currentTypeAttrs += this

    def getDef : AttributeDefinition =
      new AttributeDefinition(name, dataTypeName,
        multiplicity, isComposite, isUnique, isIndexable, reverseAttributeName)

    def `~`(dT : String, options : AttrOption*) : Attr = {
      dataTypeName = dT
      options.foreach { o =>
        o match {
          case `required` => {multiplicity = Multiplicity.REQUIRED}
          case `optional` => {multiplicity = Multiplicity.OPTIONAL}
          case `collection` => {multiplicity = Multiplicity.COLLECTION}
          case `set` => {multiplicity = Multiplicity.SET}
          case `composite` => {isComposite = true}
          case `unique` => {isUnique = true}
          case `indexed` => {isIndexable = true}
          case m : MultiplicityOption => {multiplicity = new Multiplicity(m.lower, m.upper, m.isUnique)}
          case r : ReverseAttributeName => {reverseAttributeName = r.rNm}
          case _ => ()
        }
      }
      this
    }

  }

}

class TypesBuilder {

  import org.apache.atlas.typesystem.builders.TypesBuilder.{Attr, Context}

  val required = TypesBuilder.required
  val optional = TypesBuilder.optional
  val collection = TypesBuilder.collection
  val set = TypesBuilder.set
  val composite = TypesBuilder.composite
  val unique = TypesBuilder.unique
  val indexed = TypesBuilder.indexed
  def multiplicty = TypesBuilder.multiplicty _
  def reverseAttributeName = TypesBuilder.reverseAttributeName _

  val boolean = TypesBuilder.boolean
  val byte = TypesBuilder.byte
  val short = TypesBuilder.short
  val int = TypesBuilder.int
  val long = TypesBuilder.long
  val float = TypesBuilder.float

  val double = TypesBuilder.double
  val bigint = TypesBuilder.bigint
  val bigdecimal = TypesBuilder.bigdecimal
  val date = TypesBuilder.date
  val string = TypesBuilder.string

  def array = TypesBuilder.array _

  def map = TypesBuilder.map _

  val context = new DynamicVariable[Context](Context(new ArrayBuffer(),
    new ArrayBuffer(),
    new ArrayBuffer(),
    new ArrayBuffer()))

  implicit def strToAttr(s : String) = new Attr(context.value, s)

  def types(f : => Unit ) : TypesDef = {
    f
    TypesDef(context.value.enums.toSeq,
      context.value.structs.toSeq,
      context.value.traits.toSeq,
      context.value.classes.toSeq)
  }

  def _class(name : String, superTypes : List[String] = List())(f : => Unit): Unit = {
    val attrs = new ArrayBuffer[Attr]()
    context.withValue(context.value.copy(currentTypeAttrs = attrs)){f}
    context.value.classes +=
      TypesUtil.createClassTypeDef(name, ImmutableSet.copyOf[String](superTypes.toArray), attrs.map(_.getDef):_*)
  }

  def _trait(name : String, superTypes : List[String] = List())(f : => Unit): Unit = {
    val attrs = new ArrayBuffer[Attr]()
    context.withValue(context.value.copy(currentTypeAttrs = attrs)){f}
    context.value.traits +=
      TypesUtil.createTraitTypeDef(name, ImmutableSet.copyOf[String](superTypes.toArray), attrs.map(_.getDef):_*)
    val v = context.value
    v.traits.size
  }

  def struct(name : String)(f : => Unit): Unit = {
    val attrs = new ArrayBuffer[Attr]()
    context.withValue(context.value.copy(currentTypeAttrs = attrs)){f}
    context.value.structs +=
      new StructTypeDefinition(name, attrs.map(_.getDef).toArray)
  }

  def enum(name : String, values : String*) : Unit = {
    val enums = values.zipWithIndex.map{ case (v, i) =>
        new EnumValue(v,i)
    }
    context.value.enums +=
      TypesUtil.createEnumTypeDef(name, enums:_*)
  }

}
