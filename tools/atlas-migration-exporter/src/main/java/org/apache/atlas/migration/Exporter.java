/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.migration;

import org.apache.atlas.model.impexp.AtlasExportRequest;
import org.apache.atlas.repository.impexp.ExportService;
import org.apache.atlas.repository.impexp.ZipSink;
import org.apache.atlas.type.AtlasType;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class Exporter {
    private static final Logger LOG = LoggerFactory.getLogger(Exporter.class);

    private static final String EXPORT_REQUEST_JSON_FILE = "migration-export-request.json";
    private static final String ATLAS_EXPORT_SERVICE     = "exportService";
    private static final String APPLICATION_CONTEXT      = "migrationContext.xml";
    private static final int    PROGRAM_ERROR_STATUS     = -1;


    private ApplicationContext applicationContext;


    public static void main(String args[]) {
        int result = PROGRAM_ERROR_STATUS;

        try {
            display("=== Atlas Migration: Export === >>");
            String fileName = getExportToFileName(args);

            Exporter exporter = new Exporter(APPLICATION_CONTEXT);

            result = exporter.perform(fileName);

            display("<< === Atlas Migration: Export: Done! ===");
        } catch (Exception e) {
            LOG.error("<=== Atlas Migration: Export: Failed! ===", e);

            result = PROGRAM_ERROR_STATUS;
        }

        System.exit(result);
    }

    public Exporter(String contextXml) {
        try {
            applicationContext = new ClassPathXmlApplicationContext(contextXml);
        } catch (Exception ex) {
            LOG.error("Initialization failed!", ex);

            throw ex;
        }
    }

    public int perform(String fileName) {
        LOG.info("Starting export to {}", fileName);

        int                ret = 0;
        AtlasExportRequest req = getRequest(getExportRequestFile(), getDefaultExportRequest());
        OutputStream       os  = null;
        ZipSink            zs  = null;

        try {
            os = new FileOutputStream(fileName);
            zs = new ZipSink(os);

            ExportService svc = getExportService();

            svc.run(zs, req, getUserName(), getHostName(), getIPAddress());

            ret = 0;
        } catch (Exception ex) {
            LOG.error("Export failed!", ex);

            ret = PROGRAM_ERROR_STATUS;
        } finally {
            if (zs != null) {
                try {
                    zs.close();
                } catch (Throwable t) {
                    // ignore
                }
            }

            if (os != null) {
                try {
                    os.close();
                } catch (Throwable t) {
                    // ignore
                }
            }
        }

        return ret;
    }

    private AtlasExportRequest getRequest(File requestFile, String defaultJson) {
        String reqJson = null;

        try {
            if (requestFile.exists()) {
                LOG.info("Using request from the file {}", requestFile.getPath());

                reqJson = FileUtils.readFileToString(requestFile);
            } else {
                LOG.info("Using default request...");

                reqJson = defaultJson;
            }
        } catch (IOException e) {
            LOG.error("Error reading request from {}", requestFile.getPath());

            reqJson = defaultJson;
        }

        LOG.info("Export request: {}", reqJson);

        return AtlasType.fromJson(reqJson, AtlasExportRequest.class);
    }

    private ExportService getExportService() {
        return applicationContext.getBean(ATLAS_EXPORT_SERVICE, ExportService.class);
    }

    private String getUserName() {
        return System.getProperty("user.name");
    }

    private String getHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            LOG.error("faild to get hostname; using localhost", e);

            return "localhost";
        }
    }

    private String getIPAddress() {
        try {
            return InetAddress.getLocalHost().toString();
        } catch (UnknownHostException e) {
            LOG.error("failed to get IP address; using 127.0.0.1", e);

            return "127.0.0.1";
        }
    }

    private File getExportRequestFile() {
        return getFile(getCurrentDirectory(), EXPORT_REQUEST_JSON_FILE);
    }

    private File getFile(String currentDir, String fileName) {
        LOG.info("Attempting to use request file: {}/{}", currentDir, fileName);
        return new File(currentDir, fileName);
    }

    private String getCurrentDirectory() {
        return System.getProperty("user.dir");
    }

    private String getDefaultExportRequest() {
        return "{ \"itemsToExport\": [ { \"typeName\": \"hive_db\" } ], \"options\": {  \"fetchType\": \"FULL\", \"matchType\": \"forType\"} }";
    }

    private static void display(String s) {
        LOG.info(s);
    }

    private static String getExportToFileName(String[] args) {
        String fileName = (args.length > 0) ? args[0] : getDefaultFileName();

        if (args.length == 0) {
            printUsage(fileName);
        }

        return fileName;
    }

    private static void printUsage(String fileName) {
        display("Exporting to file " + fileName + ". To export data to a different file, please specify the file path as argument");
    }

    private static String getDefaultFileName() {
        return String.format("atlas-export-%s.zip", System.currentTimeMillis());
    }
}
