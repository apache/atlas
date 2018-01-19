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

package org.apache.atlas.ocf.properties;


/**
 * Lineage shows the origin of the connected asset.  It covers:
 * <ul>
 *     <li>Design lineage - the known data movement and data stores that can supply data to this asset.</li>
 *     <li>Operational lineage - showing the jobs that ran to create this asset</li>
 * </ul>
 *
 * Currently lineage is not implemented in the ConnectedAssetProperties interface because more design work is needed.
 * This class is therefore a placeholder for lineage information.
 */
public class Lineage extends AssetPropertyBase
{
    /**
     * Typical constructor.
     *
     * @param parentAsset - description of the asset that this lineage is attached to.
     */
    public Lineage(AssetDescriptor  parentAsset)
    {
        /*
         * Save descriptor of the asset that this lineage is attached to
         */
        super(parentAsset);
    }


    /**
     * Copy/clone constructor - - the parentAsset is passed separately to the template because it is also
     * likely to be being cloned in the same operation and we want the lineage clone to point to the
     * asset clone and not the original asset.
     *
     * @param parentAsset - description of the asset that this lineage is attached to.
     * @param templateLineage - lineage object to copy.
     */
    public Lineage(AssetDescriptor  parentAsset, Lineage   templateLineage)
    {
        super(parentAsset, templateLineage);

        /*
         * The open lineage design is still in progress so for the time being, this object does not do anything
         * useful
         */
    }


    /**
     * Standard toString method.
     *
     * @return print out of variables in a JSON-style
     */
    @Override
    public String toString()
    {
        return "Lineage{}";
    }
}