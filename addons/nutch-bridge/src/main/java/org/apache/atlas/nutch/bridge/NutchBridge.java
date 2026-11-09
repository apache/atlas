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
 * See the License for the specific language governing limitations under
 * the License.
 */
package org.apache.atlas.nutch.bridge;

import com.google.common.annotations.VisibleForTesting;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.nutch.model.NutchDataTypes;
import org.apache.atlas.utils.AuthenticationUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableName;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Batch-imports Nutch crawl catalog metadata into Atlas. Does not create nutch_index_process
 * (<a href="https://issues.apache.org/jira/browse/NUTCH-3210">NUTCH-3210</a>).
 */
public class NutchBridge {
    private static final Logger LOG = LoggerFactory.getLogger(NutchBridge.class);

    private static final int    EXIT_CODE_SUCCESS    = 0;
    private static final int    EXIT_CODE_FAILED     = 1;
    private static final String ATLAS_ENDPOINT       = "atlas.rest.address";
    private static final String DEFAULT_ATLAS_URL    = "http://localhost:21000/";
    private static final String CLUSTER_NAME_KEY     = "atlas.cluster.name";
    private static final String DEFAULT_CLUSTER_NAME = "primary";
    static final String         QUALIFIED_NAME       = "qualifiedName";

    private final AtlasClientV2 atlasClientV2;
    private final String        clusterName;

    /**
     * CLI entry: {@code -c crawlId -d crawldb [-H hostdb] [-s seeds] [-g segments]}.
     *
     * @param args command-line arguments
     */
    public static void main(String[] args) {
        int           exitCode      = EXIT_CODE_FAILED;
        AtlasClientV2 atlasClientV2 = null;

        System.out.println("\n################################\n# Apache Atlas Nutch bridge #\n################################\n");

        try {
            Options options = new Options();
            options.addOption("c", "crawlId", true, "Nutch crawlId");
            options.addOption("d", "crawldb", true, "Path to CrawlDb (crawldb or crawldb/current)");
            options.addOption("s", "seeds", true, "Seed directory");
            options.addOption("g", "segments", true, "Segments directory");
            options.addOption("H", "hostdb", true, "Path to HostDB (hostdb or hostdb/current); preferred for hosts");

            CommandLine   cmd       = new DefaultParser().parse(options, args);
            String        crawlId   = cmd.getOptionValue("c");
            String        crawldb   = cmd.getOptionValue("d");
            String        seeds     = cmd.getOptionValue("s");
            String        segments  = cmd.getOptionValue("g");
            String        hostdb    = cmd.getOptionValue("H");
            Configuration atlasConf = ApplicationProperties.get();
            String[]      urls      = atlasConf.getStringArray(ATLAS_ENDPOINT);

            if (urls == null || urls.length == 0) {
                urls = new String[] {DEFAULT_ATLAS_URL};
            }

            if (StringUtils.isBlank(crawlId) || StringUtils.isBlank(crawldb)) {
                printUsage();
                System.exit(EXIT_CODE_FAILED);
            }

            if (!AuthenticationUtil.isKerberosAuthenticationEnabled()) {
                String[] basicAuthUsernamePassword = AuthenticationUtil.getBasicAuthenticationInput();

                atlasClientV2 = new AtlasClientV2(urls, basicAuthUsernamePassword);
            } else {
                UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

                atlasClientV2 = new AtlasClientV2(ugi, ugi.getShortUserName(), urls);
            }

            NutchBridge bridge = new NutchBridge(atlasConf, atlasClientV2);

            registerCrawlDatumAlias();
            registerHostDatumAlias();
            bridge.importCrawl(crawlId, crawldb, seeds, segments, hostdb);

            exitCode = EXIT_CODE_SUCCESS;
        } catch (Exception e) {
            System.out.println("ImportNutchEntities failed. Please check the log file for the detailed error message");
            e.printStackTrace();
            LOG.error("ImportNutchEntities failed", e);
        } finally {
            if (atlasClientV2 != null) {
                atlasClientV2.close();
            }
        }

        System.exit(exitCode);
    }

    static void registerCrawlDatumAlias() {
        WritableName.addName(CrawlDbReader.NutchCrawlDatum.class, "org.apache.nutch.crawl.CrawlDatum");
    }

    static void registerHostDatumAlias() {
        WritableName.addName(HostDbReader.NutchHostDatum.class, "org.apache.nutch.hostdb.HostDatum");
    }

    /**
     * @param atlasConf     Atlas application properties ({@code atlas.cluster.name})
     * @param atlasClientV2 REST client used to create and update entities
     */
    public NutchBridge(Configuration atlasConf, AtlasClientV2 atlasClientV2) {
        this.atlasClientV2 = atlasClientV2;
        this.clusterName   = atlasConf.getString(CLUSTER_NAME_KEY, DEFAULT_CLUSTER_NAME);
    }

    /**
     * Imports crawl, seedlist, segments, domains, hosts, and crawl-host metrics.
     * Hosts come from HostDB when present; otherwise CrawlDb rollup.
     *
     * @param crawlId     Nutch crawl id
     * @param crawldbPath CrawlDb directory ({@code crawldb} or {@code crawldb/current})
     * @param seedDir     optional inject seed directory
     * @param segmentsDir optional segments directory
     * @throws Exception if CrawlDb/HostDB cannot be read or Atlas REST calls fail
     */
    public void importCrawl(String crawlId, String crawldbPath, String seedDir, String segmentsDir) throws Exception {
        importCrawl(crawlId, crawldbPath, seedDir, segmentsDir, null);
    }

    /**
     * Same as {@link #importCrawl(String, String, String, String)} with an explicit HostDB path.
     *
     * @param crawlId     Nutch crawl id
     * @param crawldbPath CrawlDb directory ({@code crawldb} or {@code crawldb/current})
     * @param seedDir     optional inject seed directory
     * @param segmentsDir optional segments directory
     * @param hostDbPath  HostDB directory, or {@code null} to discover {@code ../hostdb/current}
     * @throws Exception if CrawlDb/HostDB cannot be read or Atlas REST calls fail
     */
    public void importCrawl(String crawlId, String crawldbPath, String seedDir, String segmentsDir, String hostDbPath) throws Exception {
        Path crawldb = resolveCurrentDir(crawldbPath);
        List<CrawlRecord> records = CrawlDbReader.read(crawldb);
        Map<String, HostMetrics> hosts = resolveHosts(crawldbPath, hostDbPath, records);
        List<String> segmentNames = listSegmentNames(segmentsDir);
        long seedCount = countSeedLines(seedDir);

        LOG.info("Importing Nutch crawl {} ({} URLs, {} fetched hosts, {} segments)", crawlId, records.size(), hosts.size(), segmentNames.size());

        AtlasEntityWithExtInfo crawl = createOrUpdateCrawl(crawlId, crawldbPath, records.size());
        createOrUpdateSeedlist(crawlId, seedDir, seedCount, crawl.getEntity());
        for (String segmentName : segmentNames) {
            createOrUpdateSegment(crawlId, segmentsDir, segmentName, crawl.getEntity());
        }
        for (HostMetrics metrics : hosts.values()) {
            createOrUpdateHostAndDomain(crawl.getEntity(), metrics);
        }
    }

    @VisibleForTesting
    AtlasEntityWithExtInfo createOrUpdateCrawl(String crawlId, String crawldbPath, long urlCount) throws Exception {
        String                 qn     = NutchQualifiedNames.crawl(crawlId, clusterName);
        AtlasEntityWithExtInfo existing = findEntity(NutchDataTypes.NUTCH_CRAWL.getName(), qn);
        AtlasEntity            entity   = existing != null ? existing.getEntity() : new AtlasEntity(NutchDataTypes.NUTCH_CRAWL.getName());

        entity.setAttribute(QUALIFIED_NAME, qn);
        entity.setAttribute("name", crawlId);
        entity.setAttribute("crawlId", crawlId);
        entity.setAttribute("clusterName", clusterName);
        entity.setAttribute("crawldbPath", crawldbPath);
        entity.setAttribute("urlCount", urlCount);

        return save(entity, existing);
    }

    @VisibleForTesting
    AtlasEntityWithExtInfo createOrUpdateSeedlist(String crawlId, String seedPath, long seedCount, AtlasEntity crawl) throws Exception {
        String                 qn       = NutchQualifiedNames.seedlist(crawlId, clusterName);
        AtlasEntityWithExtInfo existing = findEntity(NutchDataTypes.NUTCH_SEEDLIST.getName(), qn);
        AtlasEntity            entity   = existing != null ? existing.getEntity() : new AtlasEntity(NutchDataTypes.NUTCH_SEEDLIST.getName());

        entity.setAttribute(QUALIFIED_NAME, qn);
        entity.setAttribute("name", crawlId + "-seeds");
        entity.setAttribute("seedPath", seedPath);
        entity.setAttribute("seedCount", seedCount);
        entity.setRelationshipAttribute("crawl", objectId(crawl));

        return save(entity, existing);
    }

    @VisibleForTesting
    AtlasEntityWithExtInfo createOrUpdateSegment(String crawlId, String segmentsDir, String segmentName, AtlasEntity crawl) throws Exception {
        String                 qn       = NutchQualifiedNames.segment(crawlId, segmentName, clusterName);
        AtlasEntityWithExtInfo existing = findEntity(NutchDataTypes.NUTCH_SEGMENT.getName(), qn);
        AtlasEntity            entity   = existing != null ? existing.getEntity() : new AtlasEntity(NutchDataTypes.NUTCH_SEGMENT.getName());
        String                 path     = segmentsDir == null ? segmentName : segmentsDir + "/" + segmentName;

        entity.setAttribute(QUALIFIED_NAME, qn);
        entity.setAttribute("name", segmentName);
        entity.setAttribute("segmentName", segmentName);
        entity.setAttribute("path", path);
        entity.setRelationshipAttribute("crawl", objectId(crawl));

        return save(entity, existing);
    }

    @VisibleForTesting
    void createOrUpdateHostAndDomain(AtlasEntity crawl, HostMetrics metrics) throws Exception {
        String domainQn = NutchQualifiedNames.domain(metrics.getEtldPlusOne(), clusterName);
        String hostQn   = NutchQualifiedNames.host(metrics.getHostname(), clusterName);

        AtlasEntityWithExtInfo domainExisting = findEntity(NutchDataTypes.NUTCH_DOMAIN.getName(), domainQn);
        AtlasEntity            domain         = domainExisting != null ? domainExisting.getEntity() : new AtlasEntity(NutchDataTypes.NUTCH_DOMAIN.getName());

        domain.setAttribute(QUALIFIED_NAME, domainQn);
        domain.setAttribute("name", metrics.getEtldPlusOne());
        domain.setAttribute("clusterName", clusterName);
        AtlasEntityWithExtInfo savedDomain = save(domain, domainExisting);

        AtlasEntityWithExtInfo hostExisting = findEntity(NutchDataTypes.NUTCH_HOST.getName(), hostQn);
        AtlasEntity            host         = hostExisting != null ? hostExisting.getEntity() : new AtlasEntity(NutchDataTypes.NUTCH_HOST.getName());

        host.setAttribute(QUALIFIED_NAME, hostQn);
        host.setAttribute("name", metrics.getHostname());
        host.setAttribute("hostname", metrics.getHostname());
        host.setAttribute("protocol", metrics.getProtocol());
        host.setAttribute("clusterName", clusterName);
        host.setRelationshipAttribute("domain", objectId(savedDomain.getEntity()));

        AtlasStruct relAttrs = new AtlasStruct();
        relAttrs.setAttribute("fetchedCount", metrics.getFetchedCount());
        relAttrs.setAttribute("unfetchedCount", metrics.getUnfetchedCount());
        relAttrs.setAttribute("maxScore", metrics.getMaxScore());
        if (metrics.getLastFetchTime() > 0) {
            relAttrs.setAttribute("lastFetchTime", new Date(metrics.getLastFetchTime()));
        }
        if (metrics.getDnsFailures() != null) {
            relAttrs.setAttribute("dnsFailures", metrics.getDnsFailures());
        }
        if (metrics.getConnectionFailures() != null) {
            relAttrs.setAttribute("connectionFailures", metrics.getConnectionFailures());
        }
        if (metrics.getGoneCount() != null) {
            relAttrs.setAttribute("goneCount", metrics.getGoneCount());
        }
        if (metrics.getRedirPermCount() != null) {
            relAttrs.setAttribute("redirPermCount", metrics.getRedirPermCount());
        }
        if (metrics.getRedirTempCount() != null) {
            relAttrs.setAttribute("redirTempCount", metrics.getRedirTempCount());
        }
        if (StringUtils.isNotBlank(metrics.getHomepageUrl())) {
            relAttrs.setAttribute("homepageUrl", metrics.getHomepageUrl());
        }

        AtlasRelatedObjectId crawlRel = new AtlasRelatedObjectId();
        crawlRel.setTypeName(NutchDataTypes.NUTCH_CRAWL.getName());
        crawlRel.setGuid(crawl.getGuid());
        crawlRel.setRelationshipType("nutch_crawl_hosts");
        crawlRel.setRelationshipAttributes(relAttrs);

        List<AtlasRelatedObjectId> crawls = new ArrayList<>();
        crawls.add(crawlRel);
        host.setRelationshipAttribute("crawls", crawls);

        save(host, hostExisting);
    }

    @VisibleForTesting
    String qualifiedName(String crawlId) {
        return NutchQualifiedNames.crawl(crawlId, clusterName);
    }

    private AtlasEntityWithExtInfo save(AtlasEntity entity, AtlasEntityWithExtInfo existing) throws Exception {
        AtlasEntityWithExtInfo payload = new AtlasEntityWithExtInfo(entity);

        if (existing == null) {
            return createEntityInAtlas(payload);
        }

        return updateEntityInAtlas(payload);
    }

    private AtlasObjectId objectId(AtlasEntity entity) {
        if (entity.getGuid() != null) {
            return new AtlasObjectId(entity.getGuid(), entity.getTypeName());
        }

        return new AtlasObjectId(entity.getTypeName(), QUALIFIED_NAME, entity.getAttribute(QUALIFIED_NAME));
    }

    AtlasEntityWithExtInfo findEntity(String typeName, String qualifiedName) {
        try {
            return atlasClientV2.getEntityByAttribute(typeName, Collections.singletonMap(QUALIFIED_NAME, qualifiedName));
        } catch (Exception e) {
            LOG.debug("Entity not found: {} {}", typeName, qualifiedName);
            return null;
        }
    }

    AtlasEntityWithExtInfo createEntityInAtlas(AtlasEntityWithExtInfo entity) throws Exception {
        EntityMutationResponse  response = atlasClientV2.createEntity(entity);
        List<AtlasEntityHeader> created  = response.getCreatedEntities();

        if (CollectionUtils.isNotEmpty(created)) {
            return atlasClientV2.getEntityByGuid(created.get(0).getGuid());
        }

        return entity;
    }

    AtlasEntityWithExtInfo updateEntityInAtlas(AtlasEntityWithExtInfo entity) throws Exception {
        atlasClientV2.updateEntity(entity);
        return entity;
    }

    @VisibleForTesting
    Map<String, HostMetrics> resolveHosts(String crawldbPath, String hostDbPath, List<CrawlRecord> crawlRecords) throws Exception {
        Path hostDb = resolveHostDbPath(crawldbPath, hostDbPath);

        if (hostDb != null) {
            try {
                Map<String, HostMetrics> hosts = HostCatalogBuilder.fromHostDb(HostDbReader.read(hostDb));
                LOG.info("hostSource=hostdb path={}", hostDb);
                return hosts;
            } catch (IOException e) {
                if (StringUtils.isNotBlank(hostDbPath)) {
                    throw e;
                }
                LOG.warn("Unable to read discovered HostDB {}; falling back to CrawlDb host rollup", hostDb, e);
            }
        }

        LOG.info("hostSource=crawldb");
        return HostCatalogBuilder.rollup(crawlRecords);
    }

    private static Path resolveHostDbPath(String crawldbPath, String hostDbPath) throws IOException {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();

        if (StringUtils.isNotBlank(hostDbPath)) {
            Path explicit = resolveCurrentDir(hostDbPath);
            if (explicit.getFileSystem(conf).exists(explicit)) {
                return explicit;
            }
            throw new IOException("HostDB path does not exist: " + hostDbPath);
        }

        Path crawlCurrent = resolveCurrentDir(crawldbPath);
        Path sibling = new Path(crawlCurrent.getParent().getParent(), "hostdb/current");
        if (sibling.getFileSystem(conf).exists(sibling)) {
            return sibling;
        }

        return null;
    }

    private static Path resolveCurrentDir(String pathStr) {
        Path path = new Path(pathStr);
        if (!pathStr.endsWith("current")) {
            path = new Path(path, "current");
        }
        return path;
    }

    private static List<String> listSegmentNames(String segmentsDir) throws Exception {
        if (StringUtils.isBlank(segmentsDir)) {
            return Collections.emptyList();
        }

        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        Path                                 dir  = new Path(segmentsDir);
        FileSystem                           fs   = dir.getFileSystem(conf);

        if (!fs.exists(dir)) {
            return Collections.emptyList();
        }

        List<String> names = new ArrayList<>();
        for (FileStatus status : fs.listStatus(dir)) {
            if (status.isDirectory()) {
                names.add(status.getPath().getName());
            }
        }
        return names;
    }

    private static long countSeedLines(String seedDir) throws Exception {
        if (StringUtils.isBlank(seedDir)) {
            return 0;
        }

        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        Path                                 dir  = new Path(seedDir);
        FileSystem                           fs   = dir.getFileSystem(conf);

        if (!fs.exists(dir)) {
            return 0;
        }

        long count = 0;
        FileStatus[] files = fs.isDirectory(dir) ? fs.listStatus(dir) : new FileStatus[] {fs.getFileStatus(dir)};
        for (FileStatus file : files) {
            if (file.isFile()) {
                try (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(fs.open(file.getPath()), java.nio.charset.StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (StringUtils.isNotBlank(line) && !line.startsWith("#")) {
                            count++;
                        }
                    }
                }
            }
        }
        return count;
    }

    private static void printUsage() {
        System.out.println("Usage: import-nutch.sh -c <crawlId> -d <crawldb> [-H <hostdb>] [-s <seedDir>] [-g <segmentsDir>]");
    }
}
