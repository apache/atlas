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

package org.apache.hadoop.metadata.tools.simpleserver

import akka.actor._
import akka.util.Timeout
import com.google.common.collect.ImmutableList
import org.apache.hadoop.metadata.{MetadataService, ITypedReferenceableInstance}
import org.apache.hadoop.metadata.json._
import org.apache.hadoop.metadata.storage.memory.MemRepository
import org.apache.hadoop.metadata.types._
import org.json4s.{Formats, NoTypeHints}
import spray.httpx.Json4sSupport
import org.apache.hadoop.metadata.storage.Id

import scala.concurrent.duration._


class MetadataActor(val typeSystem: TypeSystem, val memRepository : MemRepository) extends Actor with ActorLogging {
  import org.apache.hadoop.metadata.tools.simpleserver.MetadataProtocol._

import scala.collection.JavaConversions._
  import scala.language.postfixOps
  implicit val timeout = Timeout(5 seconds)


  def receive = {
    case ListTypeNames() =>
      sender ! TypeNames(typeSystem.getTypeNames.toList)

    case GetTypeDetails(typeNames) =>
      val typesDef = TypesSerialization.convertToTypesDef(typeSystem, (d : IDataType[_]) => typeNames.contains(d.getName))
      sender ! TypeDetails(typesDef)

    case DefineTypes(typesDef : TypesDef) =>
      typesDef.enumTypes.foreach(typeSystem.defineEnumType(_))

      typeSystem.defineTypes(ImmutableList.copyOf(typesDef.structTypes.toArray),
        ImmutableList.copyOf(typesDef.traitTypes.toArray),
        ImmutableList.copyOf(typesDef.classTypes.toArray))

      var newTypes : List[HierarchicalType[_ <: HierarchicalType[_ <: AnyRef, _], _]] = Nil
      typesDef.traitTypes.foreach { tDef =>
        val nm = tDef.typeName
        newTypes = newTypes :+
          typeSystem.getDataType(classOf[HierarchicalType[_ <: HierarchicalType[_ <: AnyRef, _], _]], nm)
      }
      typesDef.classTypes.foreach { tDef =>
        val nm = tDef.typeName
        newTypes = newTypes :+
          typeSystem.getDataType(classOf[HierarchicalType[_ <: HierarchicalType[_ <: AnyRef, _], _]], nm)
      }

      memRepository.defineTypes(newTypes)

      sender ! TypesCreated

    case CreateInstance(i) =>
      MetadataService.setCurrentService(new MetadataService(memRepository, typeSystem))
      val r = memRepository.create(i)
      sender ! InstanceCreated(r.getId)

    case GetInstance(id) =>
      MetadataService.setCurrentService(new MetadataService(memRepository, typeSystem))
      val r = memRepository.get(id)
      sender ! InstanceDetails(r)
  }

}

object MetadataProtocol {
  case class ListTypeNames()
  case class TypeNames(typeNames : List[String])
  case class GetTypeDetails(typeNames : List[String])
  case class TypeDetails(types : TypesDef)
  case class DefineTypes(types : TypesDef)
  case class TypesCreated()
  case class CreateInstance(i: ITypedReferenceableInstance)
  case class InstanceCreated(id : Id)
  case class GetInstance(id : Id)
  case class InstanceDetails(i: ITypedReferenceableInstance)
}



trait Json4sProtocol extends Json4sSupport {
  val typeSystem : TypeSystem
  val memRepository : MemRepository

  implicit def json4sFormats: Formats =
    org.json4s.native.Serialization.formats(NoTypeHints) + new MultiplicitySerializer +
      new TypedStructSerializer(Some(new MetadataService(memRepository, typeSystem))) +
      new TypedReferenceableInstanceSerializer(Some(new MetadataService(memRepository, typeSystem))) +
      new BigDecimalSerializer + new BigIntegerSerializer + new IdSerializer
}

