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

package org.apache.atlas.utils;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Properties;

import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class KafkaUtilsTest {

    @Test
    public void testSetKafkaJAASPropertiesForAllProperValues() {
        Properties properties = new Properties();
        Configuration configuration = new PropertiesConfiguration();

        final String loginModuleName = "com.sun.security.auth.module.Krb5LoginModule";
        final String loginModuleControlFlag = "required";
        final String optionUseKeyTab = "false";
        final String optionStoreKey = "true";
        final String optionServiceName = "kafka";

        configuration.setProperty("atlas.jaas.KafkaClient.loginModuleName",loginModuleName);
        configuration.setProperty("atlas.jaas.KafkaClient.loginModuleControlFlag", loginModuleControlFlag);
        configuration.setProperty("atlas.jaas.KafkaClient.option.useKeyTab", optionUseKeyTab);
        configuration.setProperty("atlas.jaas.KafkaClient.option.storeKey", optionStoreKey);
        configuration.setProperty("atlas.jaas.KafkaClient.option.serviceName",optionServiceName);

        KafkaUtils.setKafkaJAASProperties(configuration, properties);
        String newPropertyValue = properties.getProperty(KafkaUtils.KAFKA_SASL_JAAS_CONFIG_PROPERTY);

        assertTrue(newPropertyValue.contains(loginModuleName), "loginModuleName not present in new property");
        assertTrue(newPropertyValue.contains(loginModuleControlFlag),"loginModuleControlFlag not present in new property");
        assertTrue(newPropertyValue.contains("useKeyTab=" + optionUseKeyTab), "useKeyTab not present in new property or value doesn't match");
        assertTrue(newPropertyValue.contains("storeKey="+ optionStoreKey), "storeKey not present in new property or value doesn't match");
        assertTrue(newPropertyValue.contains("serviceName=" + optionServiceName), "serviceName not present in new property or value doesn't match");

    }

    @Test
    public void testSetKafkaJAASPropertiesForMissingControlFlag() {
        Properties properties = new Properties();
        Configuration configuration = new PropertiesConfiguration();

        final String loginModuleName = "com.sun.security.auth.module.Krb5LoginModule";
        final String loginModuleControlFlag = "required";
        final String optionUseKeyTab = "false";
        final String optionStoreKey = "true";
        final String optionServiceName = "kafka";

        configuration.setProperty("atlas.jaas.KafkaClient.loginModuleName",loginModuleName);
        configuration.setProperty("atlas.jaas.KafkaClient.option.useKeyTab", optionUseKeyTab);
        configuration.setProperty("atlas.jaas.KafkaClient.option.storeKey", optionStoreKey);
        configuration.setProperty("atlas.jaas.KafkaClient.option.serviceName",optionServiceName);

        KafkaUtils.setKafkaJAASProperties(configuration, properties);
        String newPropertyValue = properties.getProperty(KafkaUtils.KAFKA_SASL_JAAS_CONFIG_PROPERTY);

        assertTrue(newPropertyValue.contains(loginModuleName), "loginModuleName not present in new property");
        assertTrue(newPropertyValue.contains(loginModuleControlFlag),"loginModuleControlFlag not present in new property");
        assertTrue(newPropertyValue.contains("useKeyTab=" + optionUseKeyTab), "useKeyTab not present in new property or value doesn't match");
        assertTrue(newPropertyValue.contains("storeKey="+ optionStoreKey), "storeKey not present in new property or value doesn't match");
        assertTrue(newPropertyValue.contains("serviceName=" + optionServiceName), "serviceName not present in new property or value doesn't match");

    }

    @Test
    public void testSetKafkaJAASPropertiesForMissingLoginModuleName() {
        Properties properties = new Properties();
        Configuration configuration = new PropertiesConfiguration();

        final String loginModuleControlFlag = "required";
        final String optionUseKeyTab = "false";
        final String optionStoreKey = "true";
        final String optionServiceName = "kafka";

        configuration.setProperty("atlas.jaas.KafkaClient.loginModuleControlFlag", loginModuleControlFlag);
        configuration.setProperty("atlas.jaas.KafkaClient.option.useKeyTab", optionUseKeyTab);
        configuration.setProperty("atlas.jaas.KafkaClient.option.storeKey", optionStoreKey);
        configuration.setProperty("atlas.jaas.KafkaClient.option.serviceName",optionServiceName);

        KafkaUtils.setKafkaJAASProperties(configuration, properties);
        String newPropertyValue = properties.getProperty(KafkaUtils.KAFKA_SASL_JAAS_CONFIG_PROPERTY);

        assertNull(newPropertyValue);

    }

    @Test
    public void testSetKafkaJAASPropertiesWithSpecialCharacters() {
        Properties properties = new Properties();
        Configuration configuration = new PropertiesConfiguration();

        final String loginModuleName = "com.sun.security.auth.module.Krb5LoginModule";
        final String loginModuleControlFlag = "required";
        final String optionKeyTabPath = "/path/to/file.keytab";
        final String optionPrincipal = "test/_HOST@EXAMPLE.COM";

        configuration.setProperty("atlas.jaas.KafkaClient.loginModuleName",loginModuleName);
        configuration.setProperty("atlas.jaas.KafkaClient.loginModuleControlFlag", loginModuleControlFlag);
        configuration.setProperty("atlas.jaas.KafkaClient.option.keyTabPath", optionKeyTabPath);
        configuration.setProperty("atlas.jaas.KafkaClient.option.principal", optionPrincipal);

        try {
            KafkaUtils.setKafkaJAASProperties(configuration, properties);
            String newPropertyValue = properties.getProperty(KafkaUtils.KAFKA_SASL_JAAS_CONFIG_PROPERTY);
            String updatedPrincipalValue = org.apache.hadoop.security.SecurityUtil.getServerPrincipal(optionPrincipal, (String) null);

            assertTrue(newPropertyValue.contains(loginModuleName), "loginModuleName not present in new property");
            assertTrue(newPropertyValue.contains(loginModuleControlFlag),"loginModuleControlFlag not present in new property");
            assertTrue(newPropertyValue.contains("keyTabPath=\"" + optionKeyTabPath + "\""));
            assertTrue(newPropertyValue.contains("principal=\""+ updatedPrincipalValue + "\""));

        } catch (IOException e) {
            fail("Failed while getting updated principal value with exception : " + e.getMessage());
        }

    }

    @Test
    public void testSetKafkaJAASPropertiesForTicketBasedLoginConfig() {
        Properties properties = new Properties();
        Configuration configuration = new PropertiesConfiguration();

        final String loginModuleName = "com.sun.security.auth.module.Krb5LoginModule";
        final String loginModuleControlFlag = "required";
        final String optionUseKeyTab = "false";
        final String optionStoreKey = "true";
        final String optionServiceName = "kafka";

        configuration.setProperty("atlas.kafka.bootstrap.servers", "localhost:9100");
        configuration.setProperty("atlas.jaas.ticketBased-KafkaClient.loginModuleName",loginModuleName);
        configuration.setProperty("atlas.jaas.ticketBased-KafkaClient.loginModuleControlFlag", loginModuleControlFlag);
        configuration.setProperty("atlas.jaas.ticketBased-KafkaClient.option.useKeyTab", optionUseKeyTab);
        configuration.setProperty("atlas.jaas.ticketBased-KafkaClient.option.storeKey", optionStoreKey);
        configuration.setProperty("atlas.jaas.ticketBased-KafkaClient.option.serviceName",optionServiceName);

        try (MockedStatic mockedKafkaUtilsClass = Mockito.mockStatic(KafkaUtils.class)) {
            mockedKafkaUtilsClass.when(KafkaUtils::isLoginKeytabBased).thenReturn(false);
            mockedKafkaUtilsClass.when(KafkaUtils::isLoginTicketBased).thenReturn(true);
            mockedKafkaUtilsClass.when(() -> KafkaUtils.surroundWithQuotes(Mockito.anyString())).thenCallRealMethod();
            mockedKafkaUtilsClass.when(() -> KafkaUtils.setKafkaJAASProperties(configuration, properties)).thenCallRealMethod();

            KafkaUtils.setKafkaJAASProperties(configuration, properties);

            String newPropertyValue = properties.getProperty(KafkaUtils.KAFKA_SASL_JAAS_CONFIG_PROPERTY);
            assertTrue(newPropertyValue.contains(loginModuleName), "loginModuleName not present in new property");
            assertTrue(newPropertyValue.contains(loginModuleControlFlag), "loginModuleControlFlag not present in new property");
            assertTrue(newPropertyValue.contains("useKeyTab=" + optionUseKeyTab), "useKeyTab not present in new property or value doesn't match");
            assertTrue(newPropertyValue.contains("storeKey=" + optionStoreKey), "storeKey not present in new property or value doesn't match");
            assertTrue(newPropertyValue.contains("serviceName=" + optionServiceName), "serviceName not present in new property or value doesn't match");
        }
    }

    @Test
    public void testSetKafkaJAASPropertiesForTicketBasedLoginFallback() {
        Properties properties = new Properties();
        Configuration configuration = new PropertiesConfiguration();

        final String loginModuleName = "com.sun.security.auth.module.Krb5LoginModule";
        final String loginModuleControlFlag = "required";
        final String optionUseKeyTab = "false";
        final String optionStoreKey = "true";
        final String optionServiceName = "kafka";

        configuration.setProperty("atlas.jaas.KafkaClient.loginModuleName",loginModuleName);
        configuration.setProperty("atlas.jaas.KafkaClient.loginModuleControlFlag", loginModuleControlFlag);
        configuration.setProperty("atlas.jaas.KafkaClient.option.useKeyTab", optionUseKeyTab);
        configuration.setProperty("atlas.jaas.KafkaClient.option.storeKey", optionStoreKey);
        configuration.setProperty("atlas.jaas.KafkaClient.option.serviceName",optionServiceName);

        try (MockedStatic mockedKafkaUtilsClass = Mockito.mockStatic(KafkaUtils.class)) {
            mockedKafkaUtilsClass.when(KafkaUtils::isLoginKeytabBased).thenReturn(false);
            mockedKafkaUtilsClass.when(KafkaUtils::isLoginTicketBased).thenReturn(true);
            mockedKafkaUtilsClass.when(() -> KafkaUtils.surroundWithQuotes(Mockito.anyString())).thenCallRealMethod();
            mockedKafkaUtilsClass.when(() -> KafkaUtils.setKafkaJAASProperties(configuration, properties)).thenCallRealMethod();

            KafkaUtils.setKafkaJAASProperties(configuration, properties);

            String newPropertyValue = properties.getProperty(KafkaUtils.KAFKA_SASL_JAAS_CONFIG_PROPERTY);
            assertTrue(newPropertyValue.contains(loginModuleName), "loginModuleName not present in new property");
            assertTrue(newPropertyValue.contains(loginModuleControlFlag), "loginModuleControlFlag not present in new property");
            assertTrue(newPropertyValue.contains("useKeyTab=" + optionUseKeyTab), "useKeyTab not present in new property or value doesn't match");
            assertTrue(newPropertyValue.contains("storeKey=" + optionStoreKey), "storeKey not present in new property or value doesn't match");
            assertTrue(newPropertyValue.contains("serviceName=" + optionServiceName), "serviceName not present in new property or value doesn't match");
        }
    }


}

