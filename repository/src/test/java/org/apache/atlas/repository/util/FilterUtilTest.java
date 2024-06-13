package org.apache.atlas.repository.util;

import org.junit.Test;

import static org.apache.atlas.repository.util.FilterUtil.validateFilePath;
import static org.junit.Assert.*;

public class FilterUtilTest {
    @Test
    public void testValidateFilePath_ValidPath() {
        assertTrue("Should return true for a valid path within the allowed directory.",
                validateFilePath("/var/app/allowed/file.txt"));
    }

    @Test
    public void testValidateFilePath_RelativeTraversal() {
        assertFalse("Should return false for a path attempting directory traversal.",
                validateFilePath("/var/app/allowed/../notallowed/file.txt"));
    }

    @Test
    public void testValidateFilePath_DotSlash() {
        assertFalse("Should return false for a path with relative current directory notation.",
                validateFilePath("/var/app/allowed/./file.txt"));
    }

    @Test
    public void testValidateFilePath_BackSlash() {
        assertFalse("Should return false for a path with mixed slash types potentially bypassing checks.",
                validateFilePath("/var/app/allowed/.\\file.txt"));
    }

    @Test
    public void testValidateFilePath_NotAbsolute() {
        assertFalse("Should return false for non-absolute paths.",
                validateFilePath("var/app/allowed/file.txt"));
    }


    @Test
    public void testValidateFilePath_WithUnusualCharacters() {
        assertFalse("Should return false for paths with unusual characters aiming to navigate directories.",
                validateFilePath("/var/app/allowed/..\\file.txt"));
    }

    @Test
    public void testValidateFilePath_WithEncodedTraversal() {
        assertFalse("Should return false for paths with URL-encoded traversal sequences.",
                validateFilePath("/var/app/allowed/%2e%2e/notallowed/file.txt"));
    }

    @Test
    public void testValidateFilePath_CatchException() {
        assertFalse("Should return false for paths that cause exceptions, like those containing null bytes.",
                validateFilePath("/var/app/allowed/\0file.txt"));
    }
}
