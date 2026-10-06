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
package org.apache.atlas.runner;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * Unit tests for {@link OpenSearchTestContainerRunner#resolveImageName()}. OpenSearch server version is
 * configurable via system properties without code changes to validate 3.7 (default), 3.8, and 2.x
 * compatibility targets. Docker-free — exercises only the package-private string-resolution rules; this test class
 * lives in the same package specifically so it can call {@code resolveImageName()} directly (no reflection).
 */
public class OpenSearchTestContainerRunnerTest {
    @AfterMethod
    public void clearSystemProperties() {
        System.clearProperty(OpenSearchTestContainerRunner.IMAGE_PROPERTY);
        System.clearProperty(OpenSearchTestContainerRunner.VERSION_PROPERTY);
    }

    @Test
    public void defaultsTo37WhenNoPropertiesSet() {
        System.clearProperty(OpenSearchTestContainerRunner.IMAGE_PROPERTY);
        System.clearProperty(OpenSearchTestContainerRunner.VERSION_PROPERTY);

        assertEquals(OpenSearchTestContainerRunner.resolveImageName(), "opensearchproject/opensearch:3.7.0",
                "default image must remain 3.7.0 so existing coverage is preserved when nothing is overridden");
    }

    @Test
    public void versionPropertySelects38() {
        System.setProperty(OpenSearchTestContainerRunner.VERSION_PROPERTY, "3.8.0");

        assertEquals(OpenSearchTestContainerRunner.resolveImageName(), "opensearchproject/opensearch:3.8.0",
                "version override must select the 3.8 image so the CI matrix can validate 3.8 support");
    }

    @Test
    public void versionPropertySelects2xCompatibilityTarget() {
        // The secondary/compatibility target: the resolution mechanism must support arbitrary version tags,
        // including 2.x, without any code change — even though the CI matrix does not currently exercise it.
        System.setProperty(OpenSearchTestContainerRunner.VERSION_PROPERTY, "2.11.0");

        assertEquals(OpenSearchTestContainerRunner.resolveImageName(), "opensearchproject/opensearch:2.11.0",
                "version override must select a 2.x image tag for the 2.x compatibility target");
    }

    @Test
    public void fullImageOverrideTakesPrecedenceOverVersionProperty() {
        System.setProperty(OpenSearchTestContainerRunner.VERSION_PROPERTY, "3.8.0");
        System.setProperty(OpenSearchTestContainerRunner.IMAGE_PROPERTY, "myrepo/custom-opensearch:9.9.9");

        assertEquals(OpenSearchTestContainerRunner.resolveImageName(), "myrepo/custom-opensearch:9.9.9",
                "a full image override must win over a version-only override");
    }

    @Test
    public void blankImagePropertyFallsBackToVersionResolution() {
        System.setProperty(OpenSearchTestContainerRunner.IMAGE_PROPERTY, "   ");
        System.setProperty(OpenSearchTestContainerRunner.VERSION_PROPERTY, "3.8.0");

        assertEquals(OpenSearchTestContainerRunner.resolveImageName(), "opensearchproject/opensearch:3.8.0",
                "a blank image override must not be treated as set; version resolution must still apply");
    }

    @Test
    public void blankVersionPropertyFallsBackToDefaultVersion() {
        System.setProperty(OpenSearchTestContainerRunner.VERSION_PROPERTY, "   ");

        assertEquals(OpenSearchTestContainerRunner.resolveImageName(), "opensearchproject/opensearch:3.7.0",
                "a blank version override must not be treated as set; default 3.7.0 must be used");
    }

    @Test
    public void imageAndVersionPropertiesAreTrimmed() {
        System.setProperty(OpenSearchTestContainerRunner.VERSION_PROPERTY, "  3.8.0  ");

        assertEquals(OpenSearchTestContainerRunner.resolveImageName(), "opensearchproject/opensearch:3.8.0",
                "surrounding whitespace in the version property must be trimmed");

        System.clearProperty(OpenSearchTestContainerRunner.VERSION_PROPERTY);
        System.setProperty(OpenSearchTestContainerRunner.IMAGE_PROPERTY, "  myrepo/custom-opensearch:1.2.3  ");

        assertEquals(OpenSearchTestContainerRunner.resolveImageName(), "myrepo/custom-opensearch:1.2.3",
                "surrounding whitespace in the image property must be trimmed");
    }
}
