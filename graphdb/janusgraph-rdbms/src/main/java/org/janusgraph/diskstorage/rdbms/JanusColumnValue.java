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
package org.janusgraph.diskstorage.rdbms;

import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;

/**
 * ColumnValue stored in RDBMS
 *
 */
public class JanusColumnValue {
    private final byte[] column;
    private final byte[] value;

    public JanusColumnValue(byte[] column, byte[] value) {
        this.column = column;
        this.value  = value;
    }

    public byte[] getColumn() {
        return column;
    }

    public byte[] getValue() {
        return value;
    }

    public StaticBuffer getColumnAsStaticBuffer() {
        return StaticArrayBuffer.of(column);
    }

    public StaticBuffer getValueAsStaticBuffer() {
        return StaticArrayBuffer.of(value);
    }
}
