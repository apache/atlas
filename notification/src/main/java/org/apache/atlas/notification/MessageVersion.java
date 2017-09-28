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

package org.apache.atlas.notification;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Represents the version of a notification message.
 */
public class MessageVersion implements Comparable<MessageVersion> {
    /**
     * Used for message with no version (old format).
     */
    public static final MessageVersion NO_VERSION = new MessageVersion("0");
    public static final MessageVersion VERSION_1  = new MessageVersion("1.0.0");

    public static final MessageVersion CURRENT_VERSION = VERSION_1;

    private final String version;


    // ----- Constructors ----------------------------------------------------

    /**
     * Create a message version.
     *
     * @param version  the version string
     */
    public MessageVersion(String version) {
        this.version = version;

        try {
            getVersionParts();
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(String.format("Invalid version string : %s.", version), e);
        }
    }


    // ----- Comparable ------------------------------------------------------

    @Override
    public int compareTo(MessageVersion that) {
        if (that == null) {
            return 1;
        }

        Integer[] thisParts = getVersionParts();
        Integer[] thatParts = that.getVersionParts();

        int length = Math.max(thisParts.length, thatParts.length);

        for (int i = 0; i < length; i++) {

            int comp = getVersionPart(thisParts, i) - getVersionPart(thatParts, i);

            if (comp != 0) {
                return comp;
            }
        }
        return 0;
    }


    // ----- Object overrides ------------------------------------------------

    @Override
    public boolean equals(Object that) {
        if (this == that){
            return true;
        }

        if (that == null || getClass() != that.getClass()) {
            return false;
        }

        return compareTo((MessageVersion) that) == 0;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(getVersionParts());
    }


    @Override
    public String toString() {
        return "MessageVersion[version=" + version + "]";
    }

    // ----- helper methods --------------------------------------------------

    /**
     * Get the version parts array by splitting the version string.
     * Strip the trailing zeros (i.e. '1.0.0' equals '1').
     *
     * @return  the version parts array
     */
    protected Integer[] getVersionParts() {

        String[] sParts = version.split("\\.");
        ArrayList<Integer> iParts = new ArrayList<>();
        int trailingZeros = 0;

        for (String sPart : sParts) {
            Integer iPart = new Integer(sPart);

            if (iPart == 0) {
                ++trailingZeros;
            } else {
                for (int i = 0; i < trailingZeros; ++i) {
                    iParts.add(0);
                }
                trailingZeros = 0;
                iParts.add(iPart);
            }
        }
        return iParts.toArray(new Integer[iParts.size()]);
    }

    private Integer getVersionPart(Integer[] versionParts, int i) {
        return i < versionParts.length ? versionParts[i] : 0;
    }
}
