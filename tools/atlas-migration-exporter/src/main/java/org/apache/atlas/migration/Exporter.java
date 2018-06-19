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

package org.apache.atlas.migration;

import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONMode;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.ha.HAConfiguration;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graphdb.titan0.Titan0GraphDatabase;
import org.apache.atlas.repository.store.graph.v1.AtlasTypeDefGraphStoreV1;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashSet;


public class Exporter {
    private static final Logger LOG = LoggerFactory.getLogger(Exporter.class);

    private static final String MIGRATION_TYPESDEF_FILENAME = "atlas-migration-typesdef.json";
    private static final String MIGRATION_DATA_FILENAME     = "atlas-migration-data.json";
    private static final String LOG_MSG_PREFIX              = "atlas-migration-export: ";
    private static final int    PROGRAM_ERROR_STATUS        = -1;
    private static final int    PROGRAM_SUCCESS_STATUS      = 0;

    private final String            typesDefFileName;
    private final String            dataFileName;
    private final AtlasTypeRegistry typeRegistry;

    public static void main(String args[]) {
        int result;

        try {
            String logFileName = System.getProperty("atlas.log.dir") + File.separatorChar + System.getProperty("atlas.log.file");

            displayMessage("starting migration export. Log file location " + logFileName);

            Options options = new Options();
            options.addOption("d", "outputdir", true, "Output directory");

            CommandLine cmd       = (new BasicParser()).parse(options, args);
            String      outputDir = cmd.getOptionValue("d");

            if (StringUtils.isEmpty(outputDir)) {
                outputDir = System.getProperty("user.dir");
            }

            String typesDefFileName = outputDir + File.separatorChar + MIGRATION_TYPESDEF_FILENAME;
            String dataFileName     = outputDir + File.separatorChar + MIGRATION_DATA_FILENAME;

            Exporter exporter = new Exporter(typesDefFileName, dataFileName);

            exporter.perform();

            result = PROGRAM_SUCCESS_STATUS;

            displayMessage("completed migration export!");
        } catch (Exception e) {
            displayError("Failed", e);

            result = PROGRAM_ERROR_STATUS;
        }

        System.exit(result);
    }

    public Exporter(String typesDefFileName, String dataFileName) throws Exception {
        validate(typesDefFileName, dataFileName);

        displayMessage("initializing");

        resetHAMode();

        this.typesDefFileName = typesDefFileName;
        this.dataFileName     = dataFileName;

        typeRegistry                                 = new AtlasTypeRegistry();
        AtlasTypeDefGraphStoreV1 typeDefGraphStoreV1 = createAtlasTypeDefGraphStoreV1(typeRegistry);
        if(typeDefGraphStoreV1 == null) {
            displayMessage("createAtlasTypeDefGraphStoreV1 returned null. Initialization failed!");
            return;
        }

        typeDefGraphStoreV1.init();
        displayMessage("initialized");
    }

    private AtlasTypeDefGraphStoreV1 createAtlasTypeDefGraphStoreV1(AtlasTypeRegistry typeRegistry) {
        Constructor[] ctors = AtlasTypeDefGraphStoreV1.class.getDeclaredConstructors();
        if (ctors.length == 0 || (ctors[0].getParameterTypes().length == 0)) {
            LOG.error("Appropriate ctors not found!");
            return null;
        }

        Constructor ctor = ctors[0];
        Class<?>[] parameterTypes = ctor.getParameterTypes();
        try {
            ctor.setAccessible(true);
            displayMessage(String.format("ctor: parameters: %s", parameterTypes.length));
            return (AtlasTypeDefGraphStoreV1) ((parameterTypes.length == 2) ?
                    ctor.newInstance(typeRegistry, new HashSet<>()) :
                    ctor.newInstance(typeRegistry, new HashSet<>(), AtlasGraphProvider.getGraphInstance()));
        } catch (InstantiationException e) {
            displayError("ctor", e);
        } catch (IllegalAccessException e) {
            displayError("ctor", e);
        } catch (InvocationTargetException e) {
            displayError("ctor", e);
        }

        return null;
    }

    private void resetHAMode() throws AtlasException {
        ApplicationProperties applicationProperties = (ApplicationProperties) ApplicationProperties.get();
        applicationProperties.setProperty(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY, false);
    }

    public void perform() throws Exception {
        exportTypes();
        exportData();
    }

    private void validate(String typesDefFileName, String dataFileName) throws Exception {
        File typesDefFile = new File(typesDefFileName);
        File dataFile     = new File(dataFileName);

        if (typesDefFile.exists()) {
            throw new Exception("output file " + typesDefFileName + " already exists");
        }

        if (dataFile.exists()) {
            throw new Exception("output file " + dataFileName + " already exists");
        }
    }

    private void exportTypes() throws Exception {
        displayMessage("exporting typesDef to file " + typesDefFileName);

        AtlasTypesDef typesDef = getTypesDef(typeRegistry);

        FileUtils.write(new File(typesDefFileName), AtlasType.toJson(typesDef));

        displayMessage("exported  typesDef to file " + typesDefFileName);
    }

    private void exportData() throws Exception {
        displayMessage("exporting data to file " + dataFileName);

        FileOutputStream os = null;

        try {
            os = new FileOutputStream(dataFileName);

            Graph graph = getTitan0GraphDatabase();

            AtlasGraphSONWriter.outputGraph(graph, os, GraphSONMode.EXTENDED);
        } finally {
            if (os != null) {
                try {
                    os.close();
                } catch (Exception excp) {
                    // ignore
                }
            }
        }

        displayMessage("exported  data to file " + dataFileName);
    }

    private AtlasTypesDef getTypesDef(AtlasTypeRegistry registry) {
        return new AtlasTypesDef(new ArrayList<>(registry.getAllEnumDefs()),
                                 new ArrayList<>(registry.getAllStructDefs()),
                                 new ArrayList<>(registry.getAllClassificationDefs()),
                                 new ArrayList<>(registry.getAllEntityDefs()));
    }

    private TitanGraph getTitan0GraphDatabase() {
        return Titan0GraphDatabase.getGraphInstance();
    }

    private static void displayMessage(String msg) {
        LOG.info(LOG_MSG_PREFIX + msg);

        System.out.println(LOG_MSG_PREFIX + msg);
        System.out.flush();
    }

    private static void displayError(String msg, Throwable t) {
        LOG.error(LOG_MSG_PREFIX + msg, t);

        System.out.println(LOG_MSG_PREFIX + msg);
        System.out.flush();

        if (t != null) {
            System.out.println("ERROR: " + t.getMessage());
        }

        System.out.flush();
    }
}
