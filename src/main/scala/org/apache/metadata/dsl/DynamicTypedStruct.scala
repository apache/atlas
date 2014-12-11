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

package org.apache.metadata.dsl

import org.apache.metadata.{ITypedStruct, MetadataService}
import org.apache.metadata.storage.StructInstance
import org.apache.metadata.types.TypeSystem
import scala.language.dynamics

class DynamicTypedStruct(val ts : ITypedStruct) extends Dynamic {
  def selectDynamic(name: String) = ts.get(name)
  def updateDynamic(name: String)(value: Any) {
    var value1 = value
    if ( value != null && value.isInstanceOf[DynamicTypedStruct]) {
      value1 = value.asInstanceOf[DynamicTypedStruct].ts
    }
    ts.set(name, value1)
  }
  def dataType = MetadataService.getCurrentTypeSystem.getDataType(ts.getTypeName)
}
