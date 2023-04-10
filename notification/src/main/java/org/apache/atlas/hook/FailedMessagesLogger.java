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


import org.apache.atlas.notification.LogConfigUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.appender.rolling.TimeBasedTriggeringPolicy;
import org.apache.logging.log4j.core.config.AppenderRef;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;

import java.io.File;

/**
 * A logger wrapper that can be used to write messages that failed to be sent to a log file.
 */
public class FailedMessagesLogger {

    public static final String PATTERN_SPEC_TIMESTAMP_MESSAGE_NEWLINE = "%d{ISO8601} %m%n";
    public static final String DATE_PATTERN = ".yyyy-MM-dd";
    private PatternLayout PATTERN_LAYOUT = PatternLayout.newBuilder().withPattern(PATTERN_SPEC_TIMESTAMP_MESSAGE_NEWLINE).build();

    private String failedMessageFile;
    private String loggerName = "org.apache.atlas.hook.FailedMessagesLogger";
    private final Logger logger = LogManager.getLogger(loggerName);
    private static final LoggerContext ctx = (LoggerContext)LogManager.getContext(false);
    private static final Configuration config = ctx.getConfiguration();

    public FailedMessagesLogger(String failedMessageFile) {
        this.failedMessageFile = failedMessageFile;
    }

    void init() {
        String rootLoggerDirectory = LogConfigUtils.getRootDir();
        if (rootLoggerDirectory == null) {
            return;
        }
        String absolutePath = new File(rootLoggerDirectory, failedMessageFile).getAbsolutePath();

        RollingFileAppender.Builder builder = RollingFileAppender.newBuilder()
                .setName("RollingFileAppender")
                .withFileName(absolutePath)
                .withFilePattern(absolutePath+"-%d{yyyy-MM-dd}-%i")
                .setLayout(PATTERN_LAYOUT).withPolicy(TimeBasedTriggeringPolicy.createPolicy("1", "true"));
        RollingFileAppender rollingFileAppender = builder.build();
        rollingFileAppender.start();
        config.addAppender(rollingFileAppender);

        AppenderRef ref = AppenderRef.createAppenderRef(loggerName, Level.ERROR,null);
        AppenderRef[] refs = new AppenderRef[] {ref};
        LoggerConfig loggerConfig =
                LoggerConfig.createLogger(false, Level.ERROR, loggerName, "true", refs, null, config, null);
        loggerConfig.addAppender(rollingFileAppender, null, null);
        config.addLogger(loggerName, loggerConfig);
        ctx.updateLoggers();
    }

    public void log(String message) {
        logger.error(message);
    }
}