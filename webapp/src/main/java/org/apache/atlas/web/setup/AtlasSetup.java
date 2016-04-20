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

package org.apache.atlas.web.setup;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.setup.SetupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An application that is used to setup dependencies for the Atlas web service.
 *
 * This should be executed immediately after installation with the same configuration
 * as the Atlas web service itself. The application runs all steps registered with {@link SetupSteps}.
 */
public class AtlasSetup {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasSetup.class);

    private final Injector injector;

    public AtlasSetup() {
        injector = Guice.createInjector(new AtlasSetupModule());
        LOG.info("Got injector: " + injector);
    }

    public static void main(String[] args) {
        try {
            AtlasSetup atlasSetup = new AtlasSetup();
            atlasSetup.run();
            LOG.info("Finished running all setup steps.");
        } catch (SetupException e) {
            LOG.error("Could not run setup step.", e);
        }
    }

    public void run() throws SetupException {
        SetupSteps setupSteps = injector.getInstance(SetupSteps.class);
        LOG.info("Got setup steps.");
        try {
            setupSteps.runSetup(ApplicationProperties.get());
        } catch (AtlasException e) {
            throw new SetupException("Cannot get application properties.", e);
        }
    }
}
