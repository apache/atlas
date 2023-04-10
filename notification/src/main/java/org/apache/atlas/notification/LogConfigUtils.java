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
package org.apache.atlas.notification;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.core.Appender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Iterator;
import java.util.Map;

public class LogConfigUtils {
    private static final Logger LOG = LoggerFactory.getLogger(LogConfigUtils.class);

    public static String getRootDir() {
        String ret = getFileAppenderPath();

        if (StringUtils.isEmpty(ret)) {
            ret = getFileAppenderPathApproach2();
        }

        if (StringUtils.isNotEmpty(ret)) {
            ret = StringUtils.substringBeforeLast(ret, File.separator);
        } else {
            ret = null;
        }

        LOG.info("getRootDir(): ret={}", ret);

        return ret;
    }

    private static String getFileAppenderPath() {
        String ret = StringUtils.EMPTY;

        try {
            org.apache.logging.log4j.core.LoggerContext        loggerContext = (org.apache.logging.log4j.core.LoggerContext) org.apache.logging.log4j.LogManager.getContext();
            org.apache.logging.log4j.core.config.Configuration configuration = loggerContext.getConfiguration();

            String rrfaFilename = null;
            String rfaFilename  = null;
            String faFilename   = null;

            // get log file path in the following order:
            //   1. first RollingRandomAccessFileAppender
            //   2. first RollingFileAppender, if no RollingRandomAccessFileAppender is found
            //   3. first FileAppender, if no RollingFileAppender is found
            for (org.apache.logging.log4j.core.Appender appender : configuration.getAppenders().values()) {
                if (rrfaFilename == null && appender instanceof org.apache.logging.log4j.core.appender.RollingRandomAccessFileAppender) {
                    org.apache.logging.log4j.core.appender.RollingRandomAccessFileAppender fileAppender = (org.apache.logging.log4j.core.appender.RollingRandomAccessFileAppender) appender;

                    rrfaFilename = fileAppender.getFileName();

                    LOG.debug("RollingRandomAccessFileAppender(name={}, fileName={})", fileAppender.getName(), fileAppender.getFileName());
                } else if (rfaFilename == null && appender instanceof org.apache.logging.log4j.core.appender.RollingFileAppender) {
                    org.apache.logging.log4j.core.appender.RollingFileAppender fileAppender = (org.apache.logging.log4j.core.appender.RollingFileAppender) appender;

                    rfaFilename = fileAppender.getFileName();

                    LOG.debug("RollingFileAppender(name={}, fileName={})", fileAppender.getName(), fileAppender.getFileName());
                } else if (faFilename == null && appender instanceof org.apache.logging.log4j.core.appender.FileAppender) {
                    org.apache.logging.log4j.core.appender.FileAppender fileAppender = (org.apache.logging.log4j.core.appender.FileAppender) appender;

                    faFilename =  fileAppender.getFileName();

                    LOG.debug("FileAppender(name={}, fileName={})", fileAppender.getName(), fileAppender.getFileName());
                } else {
                    LOG.info("Could not infer log path from this appender: {}", appender.getClass().getName());
                }
            }

            if (rrfaFilename != null) {
                ret = rrfaFilename;
            } else if (rfaFilename != null) {
                ret = rfaFilename;
            } else if (faFilename != null) {
                ret = faFilename;
            }

            LOG.info("getFileAppenderPath(): ret={}", ret);
        } catch (Throwable t) {
            LOG.info("getFileAppenderPath(): failed to get log path from org.apache.logging.log4j. error: {}", t.getMessage());
        }

        return ret;
    }

    private static String getFileAppenderPathApproach2() {
        String ret = StringUtils.EMPTY;

        try {
            org.apache.logging.log4j.Logger rootLogger   = org.apache.logging.log4j.LogManager.getRootLogger();
            Map<String, Appender> allAppenders = ((org.apache.logging.log4j.core.Logger) rootLogger).getAppenders();

            if (allAppenders != null) {
                String rfaFilename  = null;
                String faFilename   = null;

                // get log file path in the following order:
                //   1. first DailyRollingFileAppender
                //   2. first RollingFileAppender, if no DailyRollingFileAppender is found
                //   3. first FileAppender, if no RollingFileAppender is found
                Iterator<Map.Entry<String, Appender>> iterator = allAppenders.entrySet().iterator();
                while (iterator.hasNext()) {
                    Object appender = iterator.next().getValue();

                    if (rfaFilename == null && appender instanceof org.apache.logging.log4j.core.appender.RollingFileAppender) {
                        org.apache.logging.log4j.core.appender.RollingFileAppender fileAppender = (org.apache.logging.log4j.core.appender.RollingFileAppender) appender;

                        rfaFilename = fileAppender.getFileName();

                        LOG.debug("RollingFileAppender(name={}, file={}, append={})", fileAppender.getName(), fileAppender.getFileName());
                    } else if (faFilename == null && appender instanceof org.apache.logging.log4j.core.appender.FileAppender) {
                        org.apache.logging.log4j.core.appender.FileAppender fileAppender = (org.apache.logging.log4j.core.appender.FileAppender) appender;

                        faFilename = fileAppender.getFileName();

                        LOG.debug("FileAppender(name={}, file={}, append={})", fileAppender.getName(), fileAppender.getFileName());
                    }
                }

                if (rfaFilename != null) {
                    ret = rfaFilename;
                } else if (faFilename != null) {
                    ret = faFilename;
                }
            }

            LOG.info("getFileAppenderPathApproach2(): ret={}", ret);
        } catch (Throwable t) {
            LOG.error("getFileAppenderPathApproach2(): failed to get log path from org.apache.logging.log4j.", t);
        }

        return ret;
    }
}