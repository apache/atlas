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

package org.apache.atlas.typesystem.types;

import org.apache.atlas.AtlasException;

public class ValueConversionException extends AtlasException {

    public ValueConversionException(IDataType typ, Object val) {
        this(typ, val, (Throwable) null);
    }

    public ValueConversionException(IDataType typ, Object val, Throwable t) {
        super(String.format("Cannot convert value '%s' to datatype %s", val.toString(), typ.getName()), t);
    }

    public ValueConversionException(IDataType typ, Object val, String msg) {
        super(String
                .format("Cannot convert value '%s' to datatype %s because: %s", val.toString(), typ.getName(), msg));
    }

    public ValueConversionException(String typeName, Object val, String msg) {
        super(String.format("Cannot convert value '%s' to datatype %s because: %s", val.toString(), typeName, msg));
    }

    protected ValueConversionException(String msg) {
        super(msg);
    }

    protected ValueConversionException(String msg, Exception e) {
        super(msg, e);
    }

    public static class NullConversionException extends ValueConversionException {
        public NullConversionException(Multiplicity m) {
            super(String.format("Null value not allowed for multiplicty %s", m));
        }

        public NullConversionException(Multiplicity m, String msg){
            super(String.format("Null value not allowed for multiplicty %s . Message %s", m, msg));
        }

        public NullConversionException(String msg, Exception e) {
            super(msg, e);
        }
    }
}
