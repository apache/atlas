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

package org.apache.atlas.addons;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * A class to write a model to a file.
 */
public final class ModelDefinitionDump {

    private ModelDefinitionDump() {
    }

    /**
     * Write given model as JSON to given file.
     * @param outputFileName file name to write model to
     * @param modelAsJson model serialized as JSON
     * @throws IOException
     */
    public static void dumpModelToFile(String outputFileName, String modelAsJson) throws IOException {
        File dir = new File(outputFileName).getParentFile();
        if (!dir.exists()) {
            dir.mkdirs();
        }
        PrintWriter printWriter = new PrintWriter(new BufferedWriter(new FileWriter(outputFileName)));
        printWriter.write(modelAsJson);
        printWriter.close();
    }

}
