/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.security;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.zookeeper.Environment;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.webapp.WebAppContext;
import org.testng.Assert;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.util.Locale;
import java.util.Properties;

/**
 *
 */
public class BaseSecurityTest {
    private static final String JAAS_ENTRY = "%s { \n" + " %s required\n"
            // kerberos module
            + " keyTab=\"%s\"\n" + " debug=true\n" + " principal=\"%s\"\n" + " useKeyTab=true\n"
            + " useTicketCache=false\n" + " doNotPrompt=true\n" + " storeKey=true;\n" + "}; \n";
    protected MiniKdc kdc;

    protected String getWarPath() {
        return String.format("/target/atlas-webapp-%s.war",
                System.getProperty("release.version"));
    }

    protected void generateTestProperties(Properties props) throws ConfigurationException, IOException {
        PropertiesConfiguration config =
                new PropertiesConfiguration(System.getProperty("user.dir") + "/../src/conf/application.properties");
        for (String propName : props.stringPropertyNames()) {
            config.setProperty(propName, props.getProperty(propName));
        }
        File file = new File(System.getProperty("user.dir"), "application.properties");
        file.deleteOnExit();
        Writer fileWriter = new FileWriter(file);
        config.save(fileWriter);
    }

    protected void startEmbeddedServer(Server server) throws Exception {
        WebAppContext webapp = new WebAppContext();
        webapp.setContextPath("/");
        webapp.setWar(System.getProperty("user.dir") + getWarPath());
        server.setHandler(webapp);

        server.start();
    }

    protected File startKDC() throws Exception {
        File target = Files.createTempDirectory("sectest").toFile();
        File kdcWorkDir = new File(target, "kdc");
        Properties kdcConf = MiniKdc.createConf();
        kdcConf.setProperty(MiniKdc.DEBUG, "true");
        kdc = new MiniKdc(kdcConf, kdcWorkDir);
        kdc.start();

        Assert.assertNotNull(kdc.getRealm());
        return kdcWorkDir;
    }

    public String createJAASEntry(String context, String principal, File keytab) {
        String keytabpath = keytab.getAbsolutePath();
        // fix up for windows; no-op on unix
        keytabpath = keytabpath.replace('\\', '/');
        return String.format(Locale.ENGLISH, JAAS_ENTRY, context, getKerberosAuthModuleForJVM(), keytabpath, principal);
    }

    protected String getKerberosAuthModuleForJVM() {
        if (System.getProperty("java.vendor").contains("IBM")) {
            return "com.ibm.security.auth.module.Krb5LoginModule";
        } else {
            return "com.sun.security.auth.module.Krb5LoginModule";
        }
    }

    protected void bindJVMtoJAASFile(File jaasFile) {
        String path = jaasFile.getAbsolutePath();
        System.setProperty(Environment.JAAS_CONF_KEY, path);
    }

    protected File createKeytab(MiniKdc kdc, File kdcWorkDir, String principal, String filename) throws Exception {
        File keytab = new File(kdcWorkDir, filename);
        kdc.createPrincipal(keytab, principal, principal + "/localhost", principal + "/127.0.0.1");
        return keytab;
    }
}
