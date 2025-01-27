package org.apache.atlas.repository.util;

import org.junit.Test;

import static org.apache.atlas.repository.util.FilterUtil.validateFilePath;
import static org.junit.Assert.*;

public class FilterUtilTest {
    @Test
    public void testValidateFilePath() {
        // Array of test cases, each containing the file path and the expected boolean result
        Object[][] testCases = {
                {"/var/app/allowed/file.txt", true, "Should return true for a valid path within the allowed directory."},
                {"/tmp/../notallowed/file.txt", false, "Should return false for a path attempting directory traversal."},
                {"/var/app/allowed/./file.txt", false, "Should return false for a path with relative current directory notation."},
                {"/Users/username/repos/repo0/.\\file.txt", false, "Should return false for a path with mixed slash types potentially bypassing checks."},
                {"tmp/file.txt", false, "Should return false for non-absolute paths."},
                {"", false, "Should return false for empty paths"},
                {"/var/app/allowed/..\\file.txt", false, "Should return false for paths with unusual characters aiming to navigate directories."},
                {"/Users/username/repos/repo0/%2e%2e/notallowed/file.txt", false, "Should return false for paths with URL-encoded traversal sequences."},
                {"/var/app/allowed/\0file.txt", false, "Should return false for paths that cause exceptions, like those containing null bytes."}
        };

        for (Object[] testCase : testCases) {
            String path = (String) testCase[0];
            boolean expected = (Boolean) testCase[1];
            String message = (String) testCase[2];

            if (expected) {
                assertTrue(message, validateFilePath(path));
            } else {
                assertFalse(message, validateFilePath(path));
            }
        }
    }
}
