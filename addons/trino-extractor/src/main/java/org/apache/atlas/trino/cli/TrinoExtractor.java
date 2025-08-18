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

package org.apache.atlas.trino.cli;

import org.apache.atlas.AtlasException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.quartz.CronExpression;
import org.quartz.CronScheduleBuilder;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static org.apache.atlas.trino.cli.ExtractorContext.OPTION_CATALOG_LONG;
import static org.apache.atlas.trino.cli.ExtractorContext.OPTION_CATALOG_SHORT;
import static org.apache.atlas.trino.cli.ExtractorContext.OPTION_CRON_EXPRESSION_LONG;
import static org.apache.atlas.trino.cli.ExtractorContext.OPTION_CRON_EXPRESSION_SHORT;
import static org.apache.atlas.trino.cli.ExtractorContext.OPTION_HELP_LONG;
import static org.apache.atlas.trino.cli.ExtractorContext.OPTION_HELP_SHORT;
import static org.apache.atlas.trino.cli.ExtractorContext.OPTION_SCHEMA_LONG;
import static org.apache.atlas.trino.cli.ExtractorContext.OPTION_SCHEMA_SHORT;
import static org.apache.atlas.trino.cli.ExtractorContext.OPTION_TABLE_LONG;
import static org.apache.atlas.trino.cli.ExtractorContext.OPTION_TABLE_SHORT;

public class TrinoExtractor {
    private static final Logger LOG = LoggerFactory.getLogger(TrinoExtractor.class);

    private static final int EXIT_CODE_SUCCESS = 0;
    private static final int EXIT_CODE_FAILED  = 1;
    private static final int EXIT_CODE_HELP    = 2;

    private static TrinoExtractor instance;

    private final ExtractorContext extractorContext;
    private       int              exitCode = EXIT_CODE_FAILED;

    private TrinoExtractor(String[] args) throws Exception {
        extractorContext = createExtractorContext(args);
    }

    public static void main(String[] args) {
        try {
            instance = new TrinoExtractor(args);

            if (instance.extractorContext != null) {
                instance.run();
            } else {
                LOG.error("Extractor context is null. Cannot proceed with extraction.");

                instance.exitCode = EXIT_CODE_FAILED;
            }
        } catch (Exception e) {
            LOG.error("Extraction failed", e);

            instance.exitCode = EXIT_CODE_FAILED;
        }

        System.exit(instance != null ? instance.exitCode : EXIT_CODE_FAILED);
    }

    private void run() {
        try {
            String cronExpression = extractorContext.getCronExpression();

            if (StringUtils.isNotEmpty(cronExpression)) {
                if (!CronExpression.isValidExpression(cronExpression)) {
                    LOG.error("Invalid cron expression: {}", cronExpression);

                    exitCode = EXIT_CODE_FAILED;
                } else {
                    LOG.info("Scheduling extraction for cron expression: {}", cronExpression);

                    JobDetail job = JobBuilder.newJob(MetadataJob.class).withIdentity("metadataJob", "group1").build();
                    Trigger trigger = TriggerBuilder.newTrigger()
                            .withSchedule(CronScheduleBuilder.cronSchedule(cronExpression)).startNow()
                            .build();

                    Scheduler scheduler = null;

                    try {
                        scheduler = new StdSchedulerFactory().getScheduler();

                        scheduler.scheduleJob(job, trigger);
                        scheduler.start();

                        try {
                            Thread.currentThread().join();
                        } catch (InterruptedException ie) {
                            LOG.info("Main thread interrupted, shutting down scheduler.");

                            exitCode = EXIT_CODE_SUCCESS;
                        }
                    } finally {
                        try {
                            if (scheduler != null && !scheduler.isShutdown()) {
                                scheduler.shutdown(true);
                            }
                        } catch (SchedulerException se) {
                            LOG.warn("Error shutting down scheduler: {}", se.getMessage());
                        }
                    }
                }
            } else {
                LOG.info("Running extraction once");

                ExtractorService extractorService = new ExtractorService();

                if (extractorService.execute(extractorContext)) {
                    exitCode = EXIT_CODE_SUCCESS;
                }
            }
        } catch (Exception e) {
            LOG.error("Error encountered. exitCode: {}", exitCode, e);
        } finally {
            if (extractorContext.getAtlasConnector() != null) {
                extractorContext.getAtlasConnector().close();
            }
        }

        System.exit(exitCode);
    }

    private ExtractorContext createExtractorContext(String[] args) throws AtlasBaseException, IOException {
        Options acceptedCliOptions = prepareCommandLineOptions();

        try {
            CommandLine  cmd              = new BasicParser().parse(acceptedCliOptions, args, true);
            List<String> argsNotProcessed = cmd.getArgList();

            if (argsNotProcessed != null && !argsNotProcessed.isEmpty()) {
                throw new AtlasBaseException("Unrecognized arguments.");
            }

            ExtractorContext ret = null;

            if (cmd.hasOption(ExtractorContext.OPTION_HELP_SHORT)) {
                printUsage(acceptedCliOptions);
                exitCode = EXIT_CODE_HELP;
            } else {
                ret = new ExtractorContext(cmd);
                LOG.debug("Successfully initialized the extractor context.");
            }

            return ret;
        } catch (ParseException | AtlasBaseException e) {
            printUsage(acceptedCliOptions);

            throw new AtlasBaseException("Invalid arguments. Reason: " + e.getMessage(), e);
        } catch (AtlasException e) {
            throw new AtlasBaseException("Error in getting Application Properties. Reason: " + e.getMessage(), e);
        } catch (IOException e) {
            throw new IOException(e);
        }
    }

    private static Options prepareCommandLineOptions() {
        Options acceptedCliOptions = new Options();

        return acceptedCliOptions.addOption(OPTION_CATALOG_SHORT, OPTION_CATALOG_LONG, true, "Catalog name")
                .addOption(OPTION_SCHEMA_SHORT, OPTION_SCHEMA_LONG, true, "Schema name")
                .addOption(OPTION_TABLE_SHORT, OPTION_TABLE_LONG, true, "Table name")
                .addOption(OPTION_CRON_EXPRESSION_SHORT, OPTION_CRON_EXPRESSION_LONG, true, "Cron expression to run extraction")
                .addOption(OPTION_HELP_SHORT, OPTION_HELP_LONG, false, "Print help message");
    }

    private static void printUsage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(TrinoExtractor.class.getName(), options);
    }

    @DisallowConcurrentExecution
    public static class MetadataJob implements Job {
        private final Logger LOG = LoggerFactory.getLogger(MetadataJob.class);

        public void execute(JobExecutionContext context) throws JobExecutionException {
            ExtractorContext extractorContext = TrinoExtractor.instance != null ? TrinoExtractor.instance.extractorContext : null;

            if (extractorContext != null) {
                LOG.info("Executing metadata extraction at: {}", java.time.LocalTime.now());

                ExtractorService extractorService = new ExtractorService();

                try {
                    if (extractorService.execute(extractorContext)) {
                        TrinoExtractor.instance.exitCode = EXIT_CODE_SUCCESS;
                    }
                } catch (Exception e) {
                    LOG.error("Error encountered: ", e);
                    throw new JobExecutionException(e);
                }
                LOG.info("Completed executing metadata extraction at: {}", java.time.LocalTime.now());
            }
        }
    }
}
