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
import org.apache.hadoop.metadata.json.TypesDef
import spray.http.StatusCodes
import spray.routing._
import scala.concurrent.duration._

class Responder(requestContext:RequestContext, ticketMaster:ActorRef) extends Actor with ActorLogging {
  import org.apache.hadoop.metadata.tools.simpleserver.Json4sProtocol._
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
  }
}

class RestInterface extends HttpServiceActor
with RestApi {
  def receive = runRoute(routes)
}


trait RestApi extends HttpService with ActorLogging { actor: Actor =>
  import MetadataProtocol._
  import scala.language.postfixOps
  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val timeout = Timeout(10 seconds)

  import akka.pattern.{ask, pipe}

  val mdSvc = context.actorOf(Props[MetadataService])

  import Json4sProtocol._

  def routes: Route =

    path("listTypeNames") {
      get { requestContext =>
        val responder : ActorRef = createResponder(requestContext)

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
      }
  def createResponder(requestContext:RequestContext) = {
    context.actorOf(Props(new Responder(requestContext, mdSvc)))
  }

}