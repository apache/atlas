// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.apache.atlas.runner;

import org.apache.atlas.ApplicationProperties;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class LocalSolrRunner {

    protected static final String[] COLLECTIONS        = readCollections();
    private   static final String   TEMPLATE_DIRECTORY = "core-template";
    private   static final String   TARGET_DIRECTORY   = System.getProperty("user.dir") + "/target";
    private   static final String   COLLECTIONS_FILE   = "/collections.txt";
    public    static final String   ZOOKEEPER_URLS     = System.getProperty("index.search.solr.zookeeper-url");

    private static final Logger LOG = LoggerFactory.getLogger(LocalSolrRunner.class);

    private static MiniSolrCloudCluster miniSolrCloudCluster;

    public static void start() throws Exception {
       if (ZOOKEEPER_URLS != null) {
            return;
        }

        String solrHome          = TARGET_DIRECTORY + "/local_solr/solr";
        File   templateDirectory = new File(solrHome + File.separator + TEMPLATE_DIRECTORY);
        File   temp              = new File(TARGET_DIRECTORY + "/data/index/" + getRandomString());

        temp.mkdirs();
        temp.deleteOnExit();

        final String solrXml = readXmlFile(solrHome + "/solr.xml", Charset.forName("UTF-8"));

        miniSolrCloudCluster = new MiniSolrCloudCluster(1, null, temp.toPath(), solrXml, null, null);

        for (String coreName : COLLECTIONS) {
            File coreDirectory = new File(temp.getAbsolutePath() + File.separator + coreName);

            coreDirectory.mkdirs();

            FileUtils.copyDirectory(templateDirectory, coreDirectory);

            Path coreConfigPath = Paths.get(coreDirectory.getAbsolutePath());

            miniSolrCloudCluster.uploadConfigSet(coreConfigPath, coreName);
        }

        Configuration configuration = ApplicationProperties.get();
        configuration.setProperty("atlas.graph.index.search.solr.zookeeper-url", getZookeeperUrls());
    }

    public static String getZookeeperUrls() {
        final String zookeeperUrls;
        if (ZOOKEEPER_URLS == null) {
            zookeeperUrls = miniSolrCloudCluster.getZkServer().getZkAddress();
        } else {
            zookeeperUrls = ZOOKEEPER_URLS;
        }
        return zookeeperUrls;
    }

    public static void stop() throws Exception {
        if (ZOOKEEPER_URLS != null) {
            return;
        }
        System.clearProperty("solr.solrxml.location");
        System.clearProperty("zkHost");
    }

    private static String[] readCollections() {
        try (InputStream inputStream = LocalSolrRunner.class.getResourceAsStream("/solr" + COLLECTIONS_FILE);
             BufferedReader buffer = new BufferedReader(new InputStreamReader(inputStream))) {
            return Pattern.compile("\\s+").split(buffer.lines().collect(Collectors.joining("\n")));
        } catch (IOException e) {
            throw new RuntimeException("Unable to read collections file", e);
        }
    }

    public static String readXmlFile(String path, Charset encoding) throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, encoding);
    }

    private static String getRandomString() {
        return UUID.randomUUID().toString();
    }

}