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
import org.apache.hadoop.metadata.{TypesDef, ITypedReferenceableInstance}
import org.apache.hadoop.metadata.storage.Id
import org.apache.hadoop.metadata.storage.memory.MemRepository
import org.apache.hadoop.metadata.types.TypeSystem
import spray.http.StatusCodes
import spray.routing._
import scala.concurrent.duration._

class Responder(val typeSystem: TypeSystem, val memRepository : MemRepository,
                requestContext:RequestContext, mdSvc:ActorRef) extends Actor with Json4sProtocol with ActorLogging {
  import org.apache.hadoop.metadata.tools.simpleserver.MetadataProtocol._

  def receive = {

    case typNames:TypeNames =>
      requestContext.complete(StatusCodes.OK, typNames)
      self ! PoisonPill

    case tD:TypeDetails =>
      requestContext.complete(StatusCodes.OK, tD)
      self ! PoisonPill

    case TypesCreated =>
      requestContext.complete(StatusCodes.OK)
      self ! PoisonPill

    case InstanceCreated(id) =>
      requestContext.complete(StatusCodes.OK, id)

    case InstanceDetails(i) =>
      requestContext.complete(StatusCodes.OK, i)
  }
}

class RestInterface(val typeSystem: TypeSystem, val memRepository : MemRepository) extends HttpServiceActor
with RestApi {
  def receive = runRoute(routes)
}


trait RestApi extends HttpService with Json4sProtocol with ActorLogging { actor: Actor =>
  import MetadataProtocol._
  import scala.language.postfixOps
  import scala.concurrent.ExecutionContext.Implicits.global

  val typeSystem : TypeSystem
  val memRepository : MemRepository

  implicit val timeout = Timeout(10 seconds)

  import akka.pattern.{ask, pipe}

  val mdSvc = context.actorOf(Props(new MetadataActor(typeSystem, memRepository)))

  def routes: Route =

    path("listTypeNames") {
      get { requestContext =>
        val responder: ActorRef = createResponder(requestContext)

        pipe(mdSvc.ask(ListTypeNames))

        mdSvc.ask(ListTypeNames()).pipeTo(responder)
      }
    } ~
      path("typeDetails") {
        get {
          entity(as[GetTypeDetails]) { typeDetails => requestContext =>
            val responder = createResponder(requestContext)
            mdSvc.ask(typeDetails).pipeTo(responder)
          }
        }
      } ~
      path("defineTypes") {
        put {
          entity(as[TypesDef]) { typesDef => requestContext =>
            val responder = createResponder(requestContext)
            mdSvc.ask(DefineTypes(typesDef)).pipeTo(responder)
          }
        }
      } ~
      path("createInstance") {
        put {
          entity(as[ITypedReferenceableInstance]) { i => requestContext =>
            val responder = createResponder(requestContext)
            mdSvc.ask(CreateInstance(i)).pipeTo(responder)
          }
        }
      } ~
      path("getInstance") {
        get {
          entity(as[Id]) { id => requestContext =>
            val responder = createResponder(requestContext)
            mdSvc.ask(GetInstance(id)).pipeTo(responder)
          }
        }
      }

  def createResponder(requestContext:RequestContext) = {
    context.actorOf(Props(new Responder(typeSystem, memRepository, requestContext, mdSvc)))
  }

}