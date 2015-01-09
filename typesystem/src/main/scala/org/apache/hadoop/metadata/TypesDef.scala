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

package org.apache.hadoop.metadata

import org.apache.hadoop.metadata.types._

case class TypesDef(enumTypes: Seq[EnumTypeDefinition],
                    structTypes: Seq[StructTypeDefinition],
                    traitTypes: Seq[HierarchicalTypeDefinition[TraitType]],
                    classTypes: Seq[HierarchicalTypeDefinition[ClassType]]) {

  def enumTypesAsJavaList() =  {
    import scala.collection.JavaConverters._
    enumTypes.asJava
  }

  def structTypesAsJavaList() =  {
    import scala.collection.JavaConverters._
    structTypes.asJava
  }

  def traitTypesAsJavaList() =  {
    import scala.collection.JavaConverters._
    traitTypes.asJava
  }

  def classTypesAsJavaList() =  {
    import scala.collection.JavaConverters._
    classTypes.asJava
  }
}
