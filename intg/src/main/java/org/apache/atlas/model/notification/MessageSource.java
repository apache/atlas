/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.model.notification;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.Properties;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * Base class of hook information.
 */
@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class MessageSource implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(MessageSource.class);

    private static final String BUILDINFO_PROPERTIES       = "/atlas-buildinfo.properties";
    private static final String BUILD_VERSION_PROPERTY_KEY = "build.version";
    private static final String BUILD_VERSION_DEFAULT      = "UNKNOWN";
    private static final String storedVersion;

    private String name;
    private String version;

    public MessageSource() {
    }

    public MessageSource(String name) {
        this.version = storedVersion;
        this.name    = name;
    }

    public String getSource() {
        return name;
    }

    public void setSource(String name) {
        this.name = name;
    }

    public String getVersion() {
        return version;
    }

    private static String fetchBuildVersion() {
        Properties        properties        = new java.util.Properties();
        InputStream       inputStream       = MessageSource.class.getResourceAsStream(BUILDINFO_PROPERTIES);
        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);

        try {
            properties.load(inputStreamReader);
        } catch (IOException e) {
            LOG.error("Failed to load atlas-buildinfo properties. Will use default version.", e);
        }

        return properties.getProperty(BUILD_VERSION_PROPERTY_KEY, BUILD_VERSION_DEFAULT);
    }

    static {
        storedVersion = fetchBuildVersion();
    }
}
