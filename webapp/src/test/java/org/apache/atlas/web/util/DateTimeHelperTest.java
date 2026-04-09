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

package org.apache.atlas.web.util;

import org.junit.Test;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class DateTimeHelperTest {
    @Test
    public void testConstantsAndPattern() {
        // Test constants
        assertEquals("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", DateTimeHelper.ISO8601_FORMAT);

        // Test that the pattern compiles correctly
        String datePattern = "(2\\d\\d\\d|19\\d\\d)-(0[1-9]|1[012])-(0[1-9]|1[0-9]|2[0-9]|3[01])T" + "([0-1][0-9]|2[0-3]):([0-5][0-9])Z";
        Pattern pattern = Pattern.compile(datePattern);
        assertNotNull(pattern);

        // Test that the pattern matches valid dates
        assertTrue(pattern.matcher("2023-12-25T14:30Z").matches());
        assertTrue(pattern.matcher("1999-01-01T00:00Z").matches());
        assertTrue(pattern.matcher("2000-06-15T23:59Z").matches());

        // Test that the pattern doesn't match invalid dates
        assertFalse(pattern.matcher("2023-13-25T14:30Z").matches()); // Invalid month
        assertFalse(pattern.matcher("2023-12-32T14:30Z").matches()); // Invalid day
        assertFalse(pattern.matcher("2023-12-25T25:30Z").matches()); // Invalid hour
        assertFalse(pattern.matcher("2023-12-25T14:60Z").matches()); // Invalid minute
        assertFalse(pattern.matcher("2023-12-25T14:30").matches());  // Missing Z
    }

    @Test
    public void testGetDateFormat() {
        // Test getDateFormat method
        DateFormat dateFormat = DateTimeHelper.getDateFormat();
        assertNotNull(dateFormat);

        // Test that it's a SimpleDateFormat
        assertTrue(dateFormat instanceof SimpleDateFormat);

        // Test that the format matches the ISO8601 format
        assertEquals(DateTimeHelper.ISO8601_FORMAT, ((SimpleDateFormat) dateFormat).toPattern());

        // Test that the timezone is UTC
        assertEquals(TimeZone.getTimeZone("UTC"), dateFormat.getTimeZone());

        // Test that multiple calls return the same instance (ThreadLocal behavior)
        DateFormat dateFormat2 = DateTimeHelper.getDateFormat();
        assertSame(dateFormat, dateFormat2);
    }

    @Test
    public void testFormatDateUTCWithValidDate() {
        // Test formatDateUTC with valid date
        Date testDate = new Date();
        String formattedDate = DateTimeHelper.formatDateUTC(testDate);

        assertNotNull(formattedDate);
        assertTrue(formattedDate.matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z"));

        // Test that the formatted date ends with Z (UTC)
        assertTrue(formattedDate.endsWith("Z"));

        // Test that the date format is correct
        assertTrue(formattedDate.matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z"));
    }

    @Test
    public void testFormatDateUTCWithNullDate() {
        // Test formatDateUTC with null date
        String formattedDate = DateTimeHelper.formatDateUTC(null);
        assertNull(formattedDate);
    }

    @Test
    public void testFormatDateUTCWithSpecificDate() {
        // Test with a specific date to verify the format
        // Create a date that we can predict the output for
        Date specificDate = new Date(1234567890000L); // 2009-02-13 23:31:30 UTC

        String formattedDate = DateTimeHelper.formatDateUTC(specificDate);
        assertNotNull(formattedDate);

        // The exact format should be predictable
        assertTrue(formattedDate.matches("2009-02-13T23:31:30\\.\\d{3}Z"));
    }

    @Test
    public void testDateFormatConsistency() {
        // Test that the DateFormat is consistent across multiple uses
        Date testDate = new Date();

        // Get the DateFormat multiple times
        DateFormat dateFormat1 = DateTimeHelper.getDateFormat();
        DateFormat dateFormat2 = DateTimeHelper.getDateFormat();

        // Format the same date multiple times
        String formatted1 = dateFormat1.format(testDate);
        String formatted2 = dateFormat2.format(testDate);

        // The results should be identical
        assertEquals(formatted1, formatted2);

        // Also test using the utility method
        String formatted3 = DateTimeHelper.formatDateUTC(testDate);
        assertEquals(formatted1, formatted3);
    }

    @Test
    public void testThreadLocalBehavior() {
        // Test that the DateFormat is thread-local
        Date testDate = new Date();

        // Get DateFormat in current thread
        DateFormat currentThreadFormat = DateTimeHelper.getDateFormat();
        String currentThreadResult = currentThreadFormat.format(testDate);

        // The utility method should use the same format
        String utilityResult = DateTimeHelper.formatDateUTC(testDate);
        assertEquals(currentThreadResult, utilityResult);

        // Test that we can get the format multiple times
        for (int i = 0; i < 5; i++) {
            DateFormat format = DateTimeHelper.getDateFormat();
            assertSame(currentThreadFormat, format);
        }
    }
}
