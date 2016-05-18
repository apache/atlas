/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.catalog;

/**
 * Term path information.
 */
//todo: split between Term and TermPath
public class TermPath {
    private final String m_taxonomy;
    private final String m_fqn;
    private final String m_name;
    private final String[] m_paths;

    public TermPath(String fullyQualifiedName) {
        m_fqn = fullyQualifiedName;
        //todo: validation
        int idx = fullyQualifiedName.indexOf('.');
        if (idx != -1) {
            m_taxonomy = fullyQualifiedName.substring(0, idx);
            m_name = fullyQualifiedName.substring(idx + 1);
            m_paths = m_name.split("\\.");
        } else {
            m_taxonomy = fullyQualifiedName;
            m_name = null;
            m_paths = new String[0];
        }
    }

    public TermPath(String taxonomyName, String termName) {
        m_taxonomy = taxonomyName;
        m_name = termName != null && termName.isEmpty() ? null : termName;

        if (m_name != null) {
            m_fqn = String.format("%s.%s", taxonomyName, termName);
            m_paths = termName.split("\\.");
        } else {
            m_fqn = taxonomyName;
            m_paths = new String[0];
        }
    }

    /**
     * Get the absolute term name which is in the form of TAXONOMY_NAME.TERM_NAME
     *
     * @return absolute term name which includes the taxonomy name
     */
    public String getFullyQualifiedName() {
        return m_fqn;
    }

    /**
     * Get the term name.  This differs from the absolute name in that it doesn't
     * include the taxonomy name.
     *
     * @return the term name
     */
    public String getName() {
        return m_name;
    }

    /**
     * Get the short name for the term which doesn't include any taxonomy or parent information.
     * @return term short name
     */
    public String getShortName() {
        return m_paths[m_paths.length - 1];
    }

    public String getPath() {
        if (m_name == null) {
            return "/";
        } else {
            int idx = m_fqn.indexOf('.');
            int lastIdx = m_fqn.lastIndexOf('.');

            return idx == lastIdx ? "/" :
                    m_fqn.substring(idx, lastIdx).replaceAll("\\.", "/");
        }
    }

    public TermPath getParent() {
        //todo: if this is the root path, throw exception
        return new TermPath(m_taxonomy, m_name.substring(0, m_name.lastIndexOf('.')));
    }


    public String getTaxonomyName() {
        return m_taxonomy;
    }

    public String[] getPathSegments() {
        return m_paths;
    }
}
