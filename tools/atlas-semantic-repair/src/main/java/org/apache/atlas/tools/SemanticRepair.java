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
import org.apache.atlas.repository.graphdb.janus.AtlasJanusGraphDatabase;
import org.apache.atlas.semantic.OpenSearchSemanticStore;
import org.apache.atlas.semantic.SemanticEntityEmbedder;
import org.apache.atlas.semantic.SemanticEntityEmbedder.IndexStats;
import org.apache.atlas.semantic.SemanticIndexSetup;
import org.apache.atlas.semantic.SemanticTextBuilder;
import org.apache.atlas.semantic.SemanticVectorStore;
import org.apache.atlas.utils.SSLUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * Backfills semantic embeddings. Candidates come from the JanusGraph vertex index in OpenSearch (no graph scan):
 * by default only active entities without an embedding (e.g. after a reindex wiped them); with {@code --all} every
 * active entity (e.g. after a model change); with {@code --guid} a single entity. Entity text is always built
 * from the graph, which also re-checks state and type.
 */
public class SemanticRepair {
    private static final Logger LOG = LoggerFactory.getLogger(SemanticRepair.class);

    private static final int EXIT_CODE_SUCCESS = 0;
    private static final int EXIT_CODE_FAILED  = 1;
    private static final int DEFAULT_PAGE_SIZE = 50;

    private final SemanticEntityEmbedder embedder;
    private final AtlasGraph             graph;
    private final boolean                dryRun;

    long       matched;
    IndexStats stats = new IndexStats(0, 0, 0);

    SemanticRepair(SemanticEntityEmbedder embedder, AtlasGraph graph, boolean dryRun) {
        this.embedder = embedder;
        this.graph    = graph;
        this.dryRun   = dryRun;
    }

    public static void main(String[] args) {
        int exitCode = EXIT_CODE_FAILED;

        try {
            CommandLine cmd       = parseArgs(args);
            int         batchSize = Integer.parseInt(cmd.getOptionValue("batch-size", String.valueOf(DEFAULT_PAGE_SIZE)));
            boolean     dryRun    = cmd.hasOption("dry-run");
            boolean     all       = cmd.hasOption("all");
            String      guid      = cmd.getOptionValue("guid");

            if (batchSize <= 0) {
                throw new IllegalArgumentException("--batch-size must be > 0");
            }

            if (all && StringUtils.isNotBlank(guid)) {
                throw new IllegalArgumentException("--all and --guid are mutually exclusive");
            }

            SSLUtil sslUtil = new SSLUtil();
            sslUtil.setSSLContext();

            SemanticIndexSetup.validateConfiguration(); // before opening the graph, which may create the vertex index
            AtlasJanusGraphDatabase.getGraphInstance();
            AtlasGraph graph = AtlasGraphProvider.getGraphInstance();

            SemanticVectorStore semanticStore = new OpenSearchSemanticStore();
            if (!dryRun) {
                semanticStore.initialize(); // checks knn, creates/updates pipeline and mapping: skip on dry-run
            }

            SemanticEntityEmbedder embedder = new SemanticEntityEmbedder(new SemanticTextBuilder(), semanticStore);
            SemanticRepair         repair   = new SemanticRepair(embedder, graph, dryRun);
            String                 mode     = StringUtils.isNotBlank(guid) ? "guid" : all ? "all" : "missing";

            LOG.info("Semantic repair starting (mode={}, dryRun={}, batchSize={})", mode, dryRun, batchSize);

            if (StringUtils.isNotBlank(guid)) {
                repair.repairPage(Collections.singletonList(guid));
            } else {
                semanticStore.scanEntities(!all, batchSize, repair::repairPage);
            }

            LOG.info("Semantic repair completed (mode={}, dryRun={}): matched={}, indexed={}, skipped={}, failed={}",
                    mode, dryRun, repair.matched, repair.stats.getIndexed(), repair.stats.getSkipped(), repair.stats.getFailed());

            exitCode = repair.stats.hasFailures() ? EXIT_CODE_FAILED : EXIT_CODE_SUCCESS;
        } catch (Exception e) {
            LOG.error("Semantic repair failed", e);
        }

        System.exit(exitCode);
    }

    void repairPage(List<String> guids) {
        matched += guids.size();

        if (!dryRun) {
            try {
                stats = stats.add(embedder.embed(guids));
            } finally {
                graph.rollback(); // read-only: close the thread-bound tx so cached vertices don't pile up across pages
            }
        }

        LOG.info("Semantic repair progress: matched={}, indexed={}, skipped={}, failed={}",
                matched, stats.getIndexed(), stats.getSkipped(), stats.getFailed());
    }

    private static CommandLine parseArgs(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(Option.builder().longOpt("batch-size").hasArg().desc("Entities per OpenSearch page (default " + DEFAULT_PAGE_SIZE + ")").build());
        options.addOption(Option.builder().longOpt("dry-run").desc("Count candidate entities without indexing or changing OpenSearch").build());
        options.addOption(Option.builder().longOpt("all").desc("Re-embed every active entity (e.g. after a model change), not only those missing an embedding").build());
        options.addOption(Option.builder().longOpt("guid").hasArg().desc("Repair a single entity guid").build());
        return new DefaultParser().parse(options, args);
    }
}
