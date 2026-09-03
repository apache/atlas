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

package org.apache.atlas.web.setup;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.setup.SetupException;
import org.apache.atlas.setup.SetupStep;
import org.apache.commons.configuration2.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Set;

@Singleton
@Component
@Conditional(SetupSteps.SetupRequired.class)
public class SetupSteps {
    private static final Logger LOG = LoggerFactory.getLogger(SetupSteps.class);

    private final Set<SetupStep> setupSteps;
    private final Configuration configuration;

    @Inject
    public SetupSteps(Set<SetupStep> steps, Configuration configuration) {
        this.setupSteps    = steps;
        this.configuration = configuration;
    }

    /**
     * Call each registered {@link SetupStep} one after the other.
     * @throws SetupException Thrown with any error during running setup, including Zookeeper interactions, and
     *                          individual failures in the {@link SetupStep}.
     */
    @PostConstruct
    public void runSetup() throws SetupException {
        try {
            LOG.info("Running setup steps (active-active mode, no curator lock).");
            for (SetupStep step : setupSteps) {
                LOG.info("Running setup step: {}", step);

                step.run();
            }
        } catch (SetupException se) {
            LOG.error("Got setup exception while trying to setup", se);

            throw se;
        } catch (Throwable e) {
            LOG.error("Error running setup steps", e);

            throw new SetupException("Error running setup steps", e);
        }
    }

    static class SetupRequired implements Condition {
        private static final String ATLAS_SERVER_RUN_SETUP_KEY = "atlas.server.run.setup.on.start";

        @Override
        public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
            try {
                Configuration configuration  = ApplicationProperties.get();
                boolean       shouldRunSetup = configuration.getBoolean(ATLAS_SERVER_RUN_SETUP_KEY, false);

                if (shouldRunSetup) {
                    LOG.warn("Running setup per configuration {}.", ATLAS_SERVER_RUN_SETUP_KEY);

                    return true;
                } else {
                    LOG.info("Not running setup per configuration {}.", ATLAS_SERVER_RUN_SETUP_KEY);
                }
            } catch (AtlasException e) {
                LOG.error("Unable to read config to determine if setup is needed. Not running setup.");
            }

            return false;
        }
    }
}
