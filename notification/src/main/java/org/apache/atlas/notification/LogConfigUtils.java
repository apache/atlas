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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.FileAppender;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.appender.RollingRandomAccessFileAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Enumeration;

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
        String        ret           = StringUtils.EMPTY;
        LoggerContext loggerContext = (LoggerContext) LogManager.getContext();
        Configuration configuration = loggerContext.getConfiguration();

        for (Appender appender : configuration.getAppenders().values()) {
            if (appender instanceof RollingRandomAccessFileAppender) {
                ret = ((RollingRandomAccessFileAppender) appender).getFileName();
                break;
            } else if (appender instanceof RollingFileAppender) {
                ret =  ((RollingRandomAccessFileAppender) appender).getFileName();
                break;
            } else if (appender instanceof FileAppender) {
                ret =  ((FileAppender) appender).getFileName();
                break;
            } else {
                LOG.info("Could not infer log path from this appender: {}", appender.getClass().getName());
            }
        }

        LOG.info("getFileAppenderPath(): ret={}", ret);

        return ret;
    }

    private static String getFileAppenderPathApproach2() {
        String ret = null;

        try {
            org.apache.log4j.Logger rootLogger   = org.apache.log4j.Logger.getRootLogger();
            Enumeration             allAppenders = rootLogger.getAllAppenders();

            if (allAppenders != null) {
                while (allAppenders.hasMoreElements()) {
                    Object appender = allAppenders.nextElement();

                    if (appender instanceof org.apache.log4j.FileAppender) {
                        org.apache.log4j.FileAppender fileAppender = (org.apache.log4j.FileAppender) appender;

                        ret = fileAppender.getName();

                        break;
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("getFileAppenderPathApproach2(): failed to get appender path", e);
        }

        LOG.info("getFileAppenderPathApproach2(): ret={}", ret);

        return ret;
    }
}
