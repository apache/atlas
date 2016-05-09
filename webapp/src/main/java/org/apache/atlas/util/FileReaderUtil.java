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

package org.apache.atlas.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

public class FileReaderUtil {
    private static Logger LOG = Logger.getLogger(FileReaderUtil.class);
    private static boolean isDebugEnabled = LOG.isDebugEnabled();

    public static List<String> readFile(String path) throws IOException {
        if (isDebugEnabled) {
            LOG.debug("<== FileReaderUtil readFile");
        }
        LOG.info("reading the file" + path);
        BufferedReader br = new BufferedReader(new FileReader(path));
        List<String> list = new ArrayList<String>();
        String line = null;
        while ((line = br.readLine()) != null) {
            if ((!line.startsWith("##")) && Pattern.matches(".+;;.*;;.*;;.+", line))
                list.add(line);
        }

        if (isDebugEnabled) {
            LOG.debug("==> FileReaderUtil readFile");
            LOG.debug("Policies read :: " + list);
        }
        if (br != null) {
            br.close();
        }
        return list;
    }
}