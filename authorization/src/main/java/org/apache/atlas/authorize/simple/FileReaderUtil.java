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

package org.apache.atlas.authorize.simple;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

public class FileReaderUtil {
    private static Logger LOG = Logger.getLogger(FileReaderUtil.class);
    private static boolean isDebugEnabled = LOG.isDebugEnabled();

    public static List<String> readFile(String path) throws IOException {
        if (isDebugEnabled) {
            LOG.debug("==> FileReaderUtil readFile");
        }
        List<String> list = new ArrayList<String>();
        LOG.info("reading the file" + path);
        List<String> fileLines = Files.readAllLines(Paths.get(path), Charset.forName("UTF-8"));
        if (fileLines != null) {
            for (String line : fileLines) {
                if ((!line.startsWith("##")) && Pattern.matches(".+;;.*;;.*;;.+", line))
                    list.add(line);
            }
        }

        if (isDebugEnabled) {
            LOG.debug("<== FileReaderUtil readFile");
            LOG.debug("Policies read :: " + list);
        }

        return list;
    }
}