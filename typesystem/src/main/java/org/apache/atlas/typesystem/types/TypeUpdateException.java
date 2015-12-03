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

package org.apache.atlas.typesystem.types;

import org.apache.atlas.AtlasException;

public class TypeUpdateException extends AtlasException {
    public TypeUpdateException(IDataType newType) {
        super(newType.getName() + " can't be updated");
    }

    public TypeUpdateException(IDataType newType, Exception e) {
        super(newType.getName() + " can't be updated - " + e.getMessage(), e);
    }

    public TypeUpdateException(String message) {
        super(message);
    }

    public TypeUpdateException(IDataType newType, String message) {
        super(newType.getName() + " can't be updated - " + message);
    }
}
