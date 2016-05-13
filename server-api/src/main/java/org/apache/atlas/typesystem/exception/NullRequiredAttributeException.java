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
package org.apache.atlas.typesystem.exception;

import org.apache.atlas.AtlasException;
import org.apache.atlas.typesystem.types.Multiplicity;


/**
 * Thrown when a repository operation attempts to
 * unset an attribute that is defined as required in the
 * type system.  A required attribute has a non-zero
 * lower bound in its multiplicity.
 *
 * @see Multiplicity#REQUIRED
 * @see Multiplicity#COLLECTION
 * @see Multiplicity#SET
 *
 */
public class NullRequiredAttributeException extends AtlasException {

    private static final long serialVersionUID = 4023597038462910948L;

    public NullRequiredAttributeException() {
        super();
    }

    public NullRequiredAttributeException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public NullRequiredAttributeException(String message, Throwable cause) {
        super(message, cause);
    }

    public NullRequiredAttributeException(String message) {
        super(message);
    }

    public NullRequiredAttributeException(Throwable cause) {
        super(cause);
    }

}
