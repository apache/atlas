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
package org.apache.atlas.fs.model;

import org.apache.atlas.addons.ModelDefinitionDump;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.json.TypesSerialization;

import java.io.IOException;

public class FSDataModelGenerator {

    public static void main(String[] args) throws IOException {
      FSDataModel.main(args);
      TypesDef typesDef = FSDataModel.typesDef();
      String fsTypesAsJSON = TypesSerialization.toJson(typesDef);
      if (args.length == 1) {
        ModelDefinitionDump.dumpModelToFile(args[0], fsTypesAsJSON);
        return;
      }
      System.out.println("FS Data Model as JSON = " + fsTypesAsJSON);
    }
}
