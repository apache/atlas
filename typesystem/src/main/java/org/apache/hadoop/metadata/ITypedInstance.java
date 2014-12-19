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

package org.apache.hadoop.metadata;

import org.apache.hadoop.metadata.types.FieldMapping;

/**
 * An instance whose structure is associated with a IDataType.
 * This is obtained by a call to 'createInstance' or the result of a Query.
 * A ITypedInstance can only contain information on attributes of the associated Type.
 * Instance can still be invalid because of missing required fields or incorrect multiplicity.
 * But user can only get/set on a known field of the associated type. Type values have to match
 * the IDataType of the associated attribute.
 */
public interface ITypedInstance extends IInstance {

    FieldMapping fieldMapping();
}
