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

package org.apache.atlas.repository.patches;

import org.apache.atlas.AtlasException;
import org.apache.atlas.service.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.inject.Inject;

@Component
@Order(2)
public class AtlasPatchService implements Service {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasPatchService.class);

    private final AtlasPatchManager patchManager;


    @Inject
    public AtlasPatchService(AtlasPatchManager patchManager) {
        this.patchManager = patchManager;
    }

    @Override
    public void start() throws AtlasException {
        LOG.info("PatchService: Started.");

        patchManager.applyAll();
    }

    @Override
    public void stop() throws AtlasException {
        LOG.info("PatchService: Stopped.");
    }
}
