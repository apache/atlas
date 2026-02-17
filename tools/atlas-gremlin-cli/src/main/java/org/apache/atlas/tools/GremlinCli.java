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

import org.apache.atlas.repository.graphdb.janus.AtlasJanusGraphDatabase;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.janusgraph.core.JanusGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import javax.script.ScriptException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Small embedded Gremlin CLI for Atlas' JanusGraph backend.
 *
 * Requires Atlas configuration directory to be provided via system property:
 *   -Datlas.conf=/path/to/atlas/conf
 *
 * Examples:
 *   -q "g.V().limit(5).valueMap(true).toList()"
 *   -f /tmp/query.groovy
 */
public class GremlinCli {
    private static final Logger LOG = LoggerFactory.getLogger(GremlinCli.class);

    public static void main(String[] args) throws Exception {
        CommandLine cmd = parseArgs(args);

        if (cmd.hasOption("h")) {
            printHelp();
            return;
        }

        final String query = getQuery(cmd);
        final boolean commit = cmd.hasOption("commit");

        // Ensure AtlasJanusGraphDatabase constructor runs (it registers GraphSON/Janus registries)
        // even though we primarily use getGraphInstance().
        new AtlasJanusGraphDatabase();

        JanusGraph graph = AtlasJanusGraphDatabase.getGraphInstance();

        try {
            GraphTraversalSource g = graph.traversal();
            // DefaultImportCustomizer isn't available in some Gremlin distributions used by this project.
            // Use the no-arg constructor and bind helpful symbols directly.
            GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();

            Bindings bindings = engine.createBindings();
            bindings.put("graph", graph);
            bindings.put("g", g);
            bindings.put("__", __.class);
            bindings.put("P", P.class);

            Object result = eval(engine, bindings, query);

            // If user returns a Traversal (e.g. g.V()), materialize it for convenience.
            if (result instanceof Traversal) {
                result = ((Traversal<?, ?>) result).toList();
            }

            System.out.println(String.valueOf(result));

            finishTx(graph, commit);
        } catch (ScriptException se) {
            safeRollback(graph);
            throw se;
        } catch (Throwable t) {
            safeRollback(graph);
            throw t;
        } finally {
            try {
                graph.close();
            } catch (Exception e) {
                LOG.warn("Failed to close graph", e);
            }
        }
    }

    private static Object eval(GremlinGroovyScriptEngine engine, Bindings bindings, String query) throws ScriptException {
        LOG.info("Executing Gremlin ({} chars)", query != null ? query.length() : 0);
        return engine.eval(query, bindings);
    }

    private static void finishTx(JanusGraph graph, boolean commit) {
        // Default behavior is rollback to protect against accidental writes.
        if (!graph.tx().isOpen()) {
            return;
        }

        if (commit) {
            graph.tx().commit();
        } else {
            graph.tx().rollback();
        }
    }

    private static void safeRollback(JanusGraph graph) {
        try {
            if (graph != null && graph.tx().isOpen()) {
                graph.tx().rollback();
            }
        } catch (Exception ignored) {
        }
    }

    // Build Options in a single place so parsing and help use the same definitions.
    private static Options buildOptions() {
        Options options = new Options();

        options.addOption(Option.builder("q")
                .longOpt("query")
                .hasArg()
                .argName("gremlin")
                .desc("Gremlin-Groovy to evaluate. Example: g.V().limit(5).valueMap(true).toList()")
                .build());

        options.addOption(Option.builder("f")
                .longOpt("file")
                .hasArg()
                .argName("path")
                .desc("Read Gremlin-Groovy script from file")
                .build());

        options.addOption(Option.builder()
                .longOpt("commit")
                .desc("Commit the transaction after evaluation (default: rollback)")
                .build());

        options.addOption(Option.builder("h")
                .longOpt("help")
                .desc("Print help")
                .build());

        return options;
    }

    private static CommandLine parseArgs(String[] args) throws ParseException {
        return new DefaultParser().parse(buildOptions(), args);
    }

    private static void printHelp() {
        HelpFormatter formatter = new HelpFormatter();

        String header = "\nEmbedded Gremlin CLI for Apache Atlas (JanusGraph).\n\n" +
                "Requires: -Datlas.conf=/path/to/atlas/conf\n";
        String footer = "\nExamples:\n" +
                "  -q \"g.V().limit(5).valueMap(true).toList()\"\n" +
                "  -f /tmp/query.groovy\n";

        formatter.printHelp("GremlinCli", header, buildOptions(), footer, true);
    }

    private static String getQuery(CommandLine cmd) throws IOException {
        String q = cmd.getOptionValue("q");
        String f = cmd.getOptionValue("f");

        if ((q == null || q.trim().isEmpty()) && (f == null || f.trim().isEmpty())) {
            throw new IllegalArgumentException("Missing query. Provide -q/--query or -f/--file. Use -h for help.");
        }

        if (q != null && f != null) {
            throw new IllegalArgumentException("Provide only one of -q/--query or -f/--file.");
        }

        if (q != null) {
            return q;
        }

        return new String(Files.readAllBytes(Paths.get(f)), StandardCharsets.UTF_8);
    }
}
