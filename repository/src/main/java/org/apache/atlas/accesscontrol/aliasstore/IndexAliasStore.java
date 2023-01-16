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
package org.apache.atlas.accesscontrol.aliasstore;

import org.apache.atlas.accesscontrol.persona.PersonaContext;
import org.apache.atlas.accesscontrol.purpose.PurposeContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.springframework.stereotype.Component;

@Component
public interface IndexAliasStore {

    public boolean createAlias(PersonaContext personaContext) throws AtlasBaseException;

    public boolean createAlias(PurposeContext purposeContext) throws AtlasBaseException;

    public boolean updateAlias(PersonaContext personaContext) throws AtlasBaseException;

    public boolean updateAlias(PurposeContext purposeContext) throws AtlasBaseException;

    public boolean deleteAlias(String aliasName) throws AtlasBaseException;
}
