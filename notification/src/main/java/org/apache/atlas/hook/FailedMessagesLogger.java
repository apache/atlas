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

package org.apache.atlas.hook;


import org.apache.log4j.Appender;
import org.apache.log4j.DailyRollingFileAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import java.io.File;
import java.io.IOException;
import java.util.Enumeration;

/**
 * A logger wrapper that can be used to write messages that failed to be sent to a log file.
 */
public class FailedMessagesLogger {

    public static final String PATTERN_SPEC_TIMESTAMP_MESSAGE_NEWLINE = "%d{ISO8601} %m%n";
    public static final String DATE_PATTERN = ".yyyy-MM-dd";

    private final Logger logger = Logger.getLogger("org.apache.atlas.hook.FailedMessagesLogger");
    private String failedMessageFile;

    public FailedMessagesLogger(String failedMessageFile) {
        this.failedMessageFile = failedMessageFile;
    }

    void init() {
        String rootLoggerDirectory = getRootLoggerDirectory();
        if (rootLoggerDirectory == null) {
            return;
        }
        String absolutePath = new File(rootLoggerDirectory, failedMessageFile).getAbsolutePath();
        try {
            DailyRollingFileAppender failedLogFilesAppender = new DailyRollingFileAppender(
                    new PatternLayout(PATTERN_SPEC_TIMESTAMP_MESSAGE_NEWLINE), absolutePath, DATE_PATTERN);
            logger.addAppender(failedLogFilesAppender);
            logger.setLevel(Level.ERROR);
            logger.setAdditivity(false);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Get the root logger file location under which the failed log messages will be written.
     *
     * Since this class is used in Hooks which run within JVMs of other components like Hive,
     * we want to write the failed messages file under the same location as where logs from
     * the host component are saved. This method attempts to get such a location from the
     * root logger's appenders. It will work only if at least one of the appenders is a {@link FileAppender}
     *
     * @return directory under which host component's logs are stored.
     */
    private String getRootLoggerDirectory() {
        String      rootLoggerDirectory = null;
        Logger      rootLogger          = Logger.getRootLogger();
        Enumeration allAppenders        = rootLogger.getAllAppenders();

        if (allAppenders != null) {
            while (allAppenders.hasMoreElements()) {
                Appender appender = (Appender) allAppenders.nextElement();

                if (appender instanceof FileAppender) {
                    FileAppender fileAppender   = (FileAppender) appender;
                    String       rootLoggerFile = fileAppender.getFile();

                    rootLoggerDirectory = rootLoggerFile != null ? new File(rootLoggerFile).getParent() : null;
                    break;
                }
            }
        }
        return rootLoggerDirectory;
    }

    void log(String message) {
        logger.error(message);
    }
}
