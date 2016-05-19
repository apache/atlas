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

define(['require',
    'utils/Globals',
    'collection/BaseCollection',
    'models/VCatalog',
], function(require, Globals, BaseCollection, VCatalog) {
    'use strict';
    var VCatalogList = BaseCollection.extend(
        //Prototypal attributes
        {
            url: Globals.baseURL + '/api/atlas/v1/taxonomies',

            model: VCatalog,

            initialize: function() {
                this.modelName = 'VCatalog';
                this.modelAttrName = '';
                this.bindErrorEvents();
                this.bindLoader();
            },
            fetch: function(options) {
                //Call Backbone's fetch
                return Backbone.Collection.prototype.fetch.call(this, options);
            },
            parseRecords: function(resp, options) {
                try {
                    /* var arr = [];
                     arr.push({
                         taxonomies: resp
                     });*/
                    return resp;
                } catch (e) {
                    console.log(e);
                }
            },
        },
        //Static Class Members
        {
            /**
             * Table Cols to be passed to Backgrid
             * UI has to use this as base and extend this.
             *
             */
            tableCols: {}
        }
    );
    return VCatalogList;
});
