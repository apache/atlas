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
package org.apache.atlas.utils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5Utils {

    private static final ThreadLocal<MessageDigest> DIGESTER_FACTORY =
        new ThreadLocal<MessageDigest>() {
            @Override
            protected MessageDigest initialValue() {
                try {
                    return MessageDigest.getInstance("MD5");
                } catch (NoSuchAlgorithmException e) {
                    throw new RuntimeException(e);
                }
            }
        };

    /**
     * Create a thread local MD5 digester
     */
    public static MessageDigest getDigester() {
        MessageDigest digester = DIGESTER_FACTORY.get();
        digester.reset();
        return digester;
    }

    private static final char[] HEX_DIGITS =
        {'0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'};

    public static String toString(byte[] digest) {
        StringBuilder buf = new StringBuilder(MD5_LEN*2);
        for (int i = 0; i < MD5_LEN; i++) {
            int b = digest[i];
            buf.append(HEX_DIGITS[(b >> 4) & 0xf]);
            buf.append(HEX_DIGITS[b & 0xf]);
        }
        return buf.toString();
    }

    public static final int MD5_LEN = 16;
}
