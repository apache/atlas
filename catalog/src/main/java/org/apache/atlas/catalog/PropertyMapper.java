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
 * Translates property names to/from name exposed in API to internal fully qualified name.
 */
public interface PropertyMapper {
    /**
     * Translate a qualified name to a clean name.
     *
     * @param propName  property name to translate
     * @param type  resource type
     *
     * @return  clean property name
     */
    String toCleanName(String propName, String type);

    /**
     * Translate a clean name to a fully qualified name.
     *
     * @param propName  property name to translate
     * @param type  resource type
     *
     * @return fully qualified property name
     */
    String toFullyQualifiedName(String propName, String type);
}
