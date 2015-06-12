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

package org.apache.atlas.web.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Support function to parse and format date.
 */
public final class DateTimeHelper {

    public static final String ISO8601_FORMAT = "yyyy-MM-dd'T'HH:mm'Z'";
    private static final String DATE_PATTERN =
            "(2\\d\\d\\d|19\\d\\d)-(0[1-9]|1[012])-(0[1-9]|1[0-9]|2[0-9]|3[01])T" + "([0-1][0-9]|2[0-3]):([0-5][0-9])Z";
    private static final Pattern PATTERN = Pattern.compile(DATE_PATTERN);

    private DateTimeHelper() {
    }

    public static String getTimeZoneId(TimeZone tz) {
        return tz.getID();
    }

    public static DateFormat getDateFormat() {
        DateFormat dateFormat = new SimpleDateFormat(ISO8601_FORMAT);
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        return dateFormat;
    }

    public static String formatDateUTC(Date date) {
        return (date != null) ? getDateFormat().format(date) : null;
    }

    public static Date parseDateUTC(String dateStr) {
        if (!validate(dateStr)) {
            throw new IllegalArgumentException(dateStr + " is not a valid UTC string");
        }
        try {
            return getDateFormat().parse(dateStr);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public static String formatDateUTCToISO8601(final String dateString, final String dateStringFormat) {

        try {
            DateFormat dateFormat = new SimpleDateFormat(dateStringFormat.substring(0, dateString.length()));
            dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
            return DateTimeHelper.formatDateUTC(dateFormat.parse(dateString));
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Validate date format with regular expression.
     *
     * @param date date address for validation
     * @return true valid date fromat, false invalid date format
     */
    public static boolean validate(final String date) {

        Matcher matcher = PATTERN.matcher(date);

        if (matcher.matches()) {

            matcher.reset();

            if (matcher.find()) {

                int year = Integer.parseInt(matcher.group(1));
                String month = matcher.group(2);
                String day = matcher.group(3);

                if (day.equals("31") && (month.equals("4") || month.equals("6") || month.equals("9") || month
                        .equals("11") || month.equals("04") || month.equals("06") || month.equals("09"))) {
                    return false; // only 1,3,5,7,8,10,12 has 31 days
                } else if (month.equals("2") || month.equals("02")) {
                    // leap year
                    if (year % 4 == 0) {
                        return !(day.equals("30") || day.equals("31"));
                    } else {
                        return !(day.equals("29") || day.equals("30") || day.equals("31"));
                    }
                } else {
                    return true;
                }
            } else {
                return false;
            }
        } else {
            return false;
        }
    }
}