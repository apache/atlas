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
package org.apache.atlas.tools;

import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.janus.AtlasJanusGraphDatabase;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.semantic.OpenSearchSemanticStore;
import org.apache.atlas.semantic.SemanticNotificationGuidExpander;
import org.apache.atlas.semantic.SemanticTextBuilder;
import org.apache.atlas.semantic.indexer.SemanticEntityIndexer;
import org.apache.atlas.utils.SSLUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

import static org.apache.atlas.repository.Constants.TYPE_NAME_PROPERTY_KEY;

public class SemanticRepair {
    private static final Logger LOG = LoggerFactory.getLogger(SemanticRepair.class);

    private static final int EXIT_CODE_SUCCESS = 0;
    private static final int EXIT_CODE_FAILED  = 1;

    public static void main(String[] args) {
        int exitCode = EXIT_CODE_FAILED;

        try {
            CommandLine cmd       = parseArgs(args);
            int         batchSize = Integer.parseInt(cmd.getOptionValue("batch-size", "50"));
            boolean     dryRun    = cmd.hasOption("dry-run");
            String      guid      = cmd.getOptionValue("guid");

            SSLUtil sslUtil = new SSLUtil();
            sslUtil.setSSLContext();

            AtlasJanusGraphDatabase.getGraphInstance();
            OpenSearchSemanticStore semanticStore = new OpenSearchSemanticStore();
            semanticStore.initialize();

            AtlasGraph graph = AtlasGraphProvider.getGraphInstance();
            SemanticTextBuilder textBuilder = new SemanticTextBuilder();
            SemanticEntityIndexer entityIndexer = new SemanticEntityIndexer(textBuilder, semanticStore);

            Set<String> guids = collectGuids(graph, guid);
            LOG.info("Semantic repair starting for {} entit(y/ies), dryRun={}", guids.size(), dryRun);

            int processed = dryRun ? guids.size()
                    : entityIndexer.indexGuidsInBatches(guids, batchSize).getIndexed();

            if (dryRun) {
                LOG.info("Dry-run: would index {} embeddable active entit(y/ies)", processed);
            }

            LOG.info("Semantic repair completed. processedEntities={}", processed);
            exitCode = EXIT_CODE_SUCCESS;
        } catch (Exception e) {
            LOG.error("Semantic repair failed", e);
        }

        System.exit(exitCode);
    }

    private static Set<String> collectGuids(AtlasGraph graph, String singleGuid) {
        Set<String> guids = new HashSet<>();

        if (StringUtils.isNotBlank(singleGuid)) {
            guids.add(singleGuid);
            return guids;
        }

        for (Object vertexObj : graph.query().vertices()) {
            AtlasVertex vertex = (AtlasVertex) vertexObj;
            String guid     = AtlasGraphUtilsV2.getIdFromVertex(vertex);
            String typeName = AtlasGraphUtilsV2.getEncodedProperty(vertex, TYPE_NAME_PROPERTY_KEY, String.class);
            if (StringUtils.isBlank(guid) || !SemanticNotificationGuidExpander.isEmbeddableEntityType(typeName)) {
                continue;
            }
            if (AtlasGraphUtilsV2.getState(vertex) != AtlasEntity.Status.ACTIVE) {
                continue;
            }
            guids.add(guid);
        }

        return guids;
    }

    private static CommandLine parseArgs(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(Option.builder().longOpt("batch-size").hasArg().desc("Batch size for bulk indexing").build());
        options.addOption(Option.builder().longOpt("dry-run").desc("List entities without indexing").build());
        options.addOption(Option.builder().longOpt("guid").hasArg().desc("Repair a single entity guid").build());
        return new DefaultParser().parse(options, args);
    }
}
