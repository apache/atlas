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

import akka.actor.{Props, ActorSystem}
import akka.io.IO
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.metadata.storage.memory.MemRepository
import org.apache.hadoop.metadata.types.TypeSystem
import spray.can.Http

/**
 * A Simple Spray based server to test the TypeSystem and MemRepository.
 *
 *  @example {{{
 *    -- Using the [[ https://github.com/jakubroztocil/httpie Httpie tool]]
 *
 *    http GET localhost:9140/listTypeNames
 *    pbpaste | http PUT localhost:9140/defineTypes
 *    http GET localhost:9140/typeDetails typeNames:='["Department", "Person", "Manager"]'
 *
 *    pbpaste | http PUT localhost:9140/createInstance
 *    pbpaste | http GET localhost:9140/getInstance
 *  }}}
 *
 *  - On the Mac, pbpaste makes available what is copied to clipboard. Copy contents of resources/sampleTypes.json
 *  - for createInstance resources/sampleInstance.json is an example
 *  - for getInstance send an Id back, you can copy the output from createInstance.
 *
 */
object Main extends App {
  val config = ConfigFactory.load()
  val host = config.getString("http.host")
  val port = config.getInt("http.port")

  implicit val system = ActorSystem("metadataservice")

  val typSys = new TypeSystem
  val memRepo = new MemRepository(typSys)

  val api = system.actorOf(Props(new RestInterface(typSys, memRepo)), "httpInterface")
  IO(Http) ! Http.Bind(listener = api, interface = host, port = port)
}