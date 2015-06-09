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

package org.apache.atlas.typesystem.builders

import org.apache.atlas.typesystem.{IReferenceableInstance, IStruct, Referenceable, Struct}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.language.{dynamics, implicitConversions}
import scala.util.DynamicVariable

class InstanceBuilder extends Dynamic {

  private val references : ArrayBuffer[Referenceable] = new ArrayBuffer[Referenceable]()

  val context = new DynamicVariable[DynamicStruct](null)

  def struct(typeName : String) : DynamicStruct = {
    context.value = new DynamicStruct(this, new Struct(typeName))
    context.value
  }

  def instance(typeName: String, traitNames: String*)(f : => Unit) : DynamicReference = {
    val r = new Referenceable(typeName, traitNames:_*)
    references.append(r)
    val dr = new DynamicReference(this, r)
    context.withValue(dr){f}
    dr
  }

  def create( f : => Unit ) : java.util.List[Referenceable] = {
    f
    references.asJava
  }

  def applyDynamic(name : String)(value : Any) : Any = {
    context.value.updateDynamic(name)(value)
  }

  implicit def symbolToDynamicStruct(s : Symbol) : DynamicValue =
    new DynamicValue(this, s.name, if (context.value == null) null else context.value.s)

}

object DynamicValue {

  private[builders] def transformOut(s: IStruct, attr : String, v : Any)(implicit ib : InstanceBuilder) : DynamicValue =
    v match {
    case r : Referenceable => new DynamicReference(ib, r)
    case s : Struct => new DynamicStruct(ib, s)
    case jL : java.util.List[_] => {
      if ( s != null ) {
        new DynamicCollection(ib, attr, s)
      } else {
        new DynamicValue(ib, attr, s, jL.map{ e => transformOut(null, null, e)}.toSeq)
      }
    }
    case jM : java.util.Map[_,_] => {
      if ( s != null ) {
        new DynamicMap(ib, attr, s)
      } else {
        new DynamicValue(ib, attr, s, jM.map {
          case (k, v) => k -> transformOut(null, null, v)
        }.toMap)
      }
    }
    case x => {
      if ( s != null ) {
        new DynamicValue(ib, attr, s)
      } else {
        new DynamicValue(ib, attr, s, x)
      }
    }
  }

  private[builders] def transformIn(v : Any) : Any = v match {
    case dr : DynamicReference => dr.r
    case ds : DynamicStruct => ds.s
    case dv : DynamicValue => dv.get
    case l : Seq[_] => l.map{ e => transformIn(e)}.asJava
    case m : Map[_,_] => m.map {
      case (k,v) => k -> transformIn(v)
    }.asJava
    case x => x
  }

}

class DynamicValue(val ib : InstanceBuilder, val attrName : String, val s: IStruct, var value : Any = null) extends Dynamic {
  import DynamicValue._

  implicit val iib : InstanceBuilder = ib

  def ~(v : Any): Unit = {
    if ( s != null ) {
      s.set(attrName, transformIn(v))
    } else {
      value = v
    }
  }

  def get : Any = if ( s != null ) s.get(attrName) else value

  def selectDynamic(name: String) : DynamicValue = {

    throw new UnsupportedOperationException()
  }

  def update(key : Any, value : Object): Unit = {
    throw new UnsupportedOperationException()
  }

  def apply(key : Any): DynamicValue = {

    if ( s != null && s.isInstanceOf[Referenceable] && key.isInstanceOf[String]) {
      val r = s.asInstanceOf[Referenceable]
      if ( r.getTraits contains attrName ) {
        val traitAttr = key.asInstanceOf[String]
        return new DynamicStruct(ib, r.getTrait(attrName)).selectDynamic(traitAttr)
      }
    }
    throw new UnsupportedOperationException()
  }
}

class DynamicCollection(ib : InstanceBuilder, attrName : String, s: IStruct) extends DynamicValue(ib, attrName ,s) {
  import DynamicValue._

  override def update(key : Any, value : Object): Unit = {
    var jL = s.get(attrName)
    val idx = key.asInstanceOf[Int]
    if (jL == null ) {
      val l = new java.util.ArrayList[Object]()
      l.ensureCapacity(idx)
      jL = l
    }
    val nJL = new java.util.ArrayList[Object](jL.asInstanceOf[java.util.List[Object]])
    nJL.asInstanceOf[java.util.List[Object]].set(idx, transformIn(value).asInstanceOf[Object])
    s.set(attrName, nJL)
  }

  override def apply(key : Any): DynamicValue = {
    var jL = s.get(attrName)
    val idx = key.asInstanceOf[Int]
    if (jL == null ) {
      null
    } else {
      transformOut(null, null, jL.asInstanceOf[java.util.List[Object]].get(idx))
    }
  }
}

class DynamicMap(ib : InstanceBuilder, attrName : String, s: IStruct) extends DynamicValue(ib, attrName ,s) {
  import DynamicValue._
  override def update(key : Any, value : Object): Unit = {
    var jM = s.get(attrName)
    if (jM == null ) {
      jM = new java.util.HashMap[Object, Object]()
    }
    jM.asInstanceOf[java.util.Map[Object, Object]].put(key.asInstanceOf[AnyRef], value)
  }

  override def apply(key : Any): DynamicValue = {
    var jM = s.get(attrName)
    if (jM == null ) {
      null
    } else {
      transformOut(null, null, jM.asInstanceOf[java.util.Map[Object, Object]].get(key))
    }
  }
}

class DynamicStruct(ib : InstanceBuilder, s: IStruct) extends DynamicValue(ib, null ,s) {
  import DynamicValue._
  override def selectDynamic(name: String) : DynamicValue = {
    transformOut(s, name, s.get(name))
  }

  def updateDynamic(name: String)(value: Any) {
    s.set(name, transformIn(value))
  }

  override def ~(v : Any): Unit = { throw new UnsupportedOperationException()}
  override def get : Any = s

}

class DynamicReference(ib : InstanceBuilder, val r : IReferenceableInstance) extends DynamicStruct(ib, r) {

  private def _trait(name : String) = new DynamicStruct(ib, r.getTrait(name))

  override def selectDynamic(name: String) : DynamicValue = {
    if ( r.getTraits contains name ) {
      _trait(name)
    } else {
      super.selectDynamic(name)
    }
  }

}
