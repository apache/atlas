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
package org.apache.atlas.repository.patches;

import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.testng.Assert.assertTrue;

public class PatchHandlerTerminalStatusGuardTest {
    private static final String GUARD_MARKER = "ALLOW_TERMINAL_UNKNOWN_WITH_OPERATOR_FLAG";

    @Test
    public void patchHandlersShouldNotUseUnknownAsTerminalStatusWithoutGuard() throws IOException {
        Path root = Paths.get("src/main/java/org/apache/atlas/repository/patches");
        List<String> violations = new ArrayList<>();

        try (Stream<Path> stream = Files.walk(root)) {
            stream.filter(Files::isRegularFile)
                    .filter(path -> path.toString().endsWith(".java"))
                    .forEach(path -> {
                        String source = read(path);

                        if (!source.contains("extends AtlasPatchHandler")) {
                            return;
                        }

                        if (source.contains("setStatus(UNKNOWN)") && !source.contains(GUARD_MARKER)) {
                            violations.add(path.toString());
                        }
                    });
        }

        assertTrue(violations.isEmpty(),
                "Patch handlers with terminal UNKNOWN status must include explicit operator-flag guard marker ("
                        + GUARD_MARKER + "). Violations: " + violations);
    }

    private static String read(Path path) {
        try {
            return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Unable to read patch source: " + path, e);
        }
    }
}
