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
package org.apache.atlas.web.filters;

import org.apache.commons.lang3.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CspNonceHtmlProcessor {
    private static final String CSP_NONCE_META_NAME = "csp-nonce";

    private static final Pattern HEAD_OPEN_TAG_PATTERN       = Pattern.compile("<head\\b[^>]*>", Pattern.CASE_INSENSITIVE);
    private static final Pattern CSP_NONCE_META_TAG_PATTERN  = Pattern.compile("<meta\\b[^>]*\\bname\\s*=\\s*(['\"])csp-nonce\\1[^>]*>", Pattern.CASE_INSENSITIVE);
    private static final Pattern META_CONTENT_ATTR_PATTERN   = Pattern.compile("\\bcontent\\s*=\\s*(['\"])(.*?)\\1", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern SCRIPT_STYLE_OPEN_TAG       = Pattern.compile("<(script|style)\\b[^>]*>", Pattern.CASE_INSENSITIVE);
    private static final Pattern NONCE_ATTR_PATTERN          = Pattern.compile("\\bnonce\\s*=\\s*(['\"]).*?\\1", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    public String injectNonce(String html, String nonce) {
        if (StringUtils.isBlank(html) || StringUtils.isBlank(nonce)) {
            return html;
        }

        String htmlWithMetaNonce = ensureMetaNonce(html, nonce);
        return addNonceToScriptAndStyleTags(htmlWithMetaNonce, nonce);
    }

    private String ensureMetaNonce(String html, String nonce) {
        Matcher metaMatcher = CSP_NONCE_META_TAG_PATTERN.matcher(html);

        if (metaMatcher.find()) {
            String metaTag      = metaMatcher.group();
            String updatedTag   = updateMetaContentAttribute(metaTag, nonce);

            return html.substring(0, metaMatcher.start()) + updatedTag + html.substring(metaMatcher.end());
        }

        Matcher headMatcher = HEAD_OPEN_TAG_PATTERN.matcher(html);
        if (headMatcher.find()) {
            String metaTag = "<meta name=\"" + CSP_NONCE_META_NAME + "\" content=\"" + nonce + "\" />";
            String headTagWithMeta = headMatcher.group() + System.lineSeparator() + "    " + metaTag;

            return headMatcher.replaceFirst(Matcher.quoteReplacement(headTagWithMeta));
        }

        return html;
    }

    private String updateMetaContentAttribute(String metaTag, String nonce) {
        Matcher contentMatcher = META_CONTENT_ATTR_PATTERN.matcher(metaTag);

        if (contentMatcher.find()) {
            String replaced = contentMatcher.replaceFirst("content=\"" + Matcher.quoteReplacement(nonce) + "\"");
            return replaced;
        }

        int insertAt = metaTag.endsWith("/>") ? metaTag.length() - 2 : metaTag.length() - 1;
        return metaTag.substring(0, insertAt) + " content=\"" + nonce + "\"" + metaTag.substring(insertAt);
    }

    private String addNonceToScriptAndStyleTags(String html, String nonce) {
        Matcher      tagMatcher = SCRIPT_STYLE_OPEN_TAG.matcher(html);
        StringBuffer output     = new StringBuffer();

        while (tagMatcher.find()) {
            String tag        = tagMatcher.group();
            String updatedTag = tag;

            if (!NONCE_ATTR_PATTERN.matcher(tag).find()) {
                int insertAt = tag.endsWith("/>") ? tag.length() - 2 : tag.length() - 1;
                updatedTag = tag.substring(0, insertAt) + " nonce=\"" + nonce + "\"" + tag.substring(insertAt);
            }

            tagMatcher.appendReplacement(output, Matcher.quoteReplacement(updatedTag));
        }

        tagMatcher.appendTail(output);

        return output.toString();
    }
}
