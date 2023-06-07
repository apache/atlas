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
    'models/VDownload',
    'utils/UrlLinks',
    'utils/Utils'
], function(require, Globals, BaseCollection, VDownload, UrlLinks, Utils) {
    'use strict';
    var VDownloadLists = BaseCollection.extend(
        //Prototypal attributes
        {
            url: UrlLinks.downloadBasicSearchResultsCSV(),
            model: VDownload,
            initialize: function() {
                this.modelName = 'VDownloads';
                this.modelAttrName = 'generateSearchResultsCSV';
            },
            parseRecords: function(resp, options) {
                try {
                    if (!this.modelAttrName) {
                        throw new Error("this.modelAttrName not defined for " + this);
                    }
                    if (resp[this.modelAttrName]) {
                        return resp[this.modelAttrName];
                    } else {
                        return resp
                    }

                } catch (e) {
                    console.log(e);
                }
            },
            getDownloadsList: function(options) {
                var url = UrlLinks.getDownloadsList();

                options = _.extend({
                    contentType: 'application/json',
                    dataType: 'json'
                }, options);

                return this.constructor.nonCrudOperation.call(this, url, 'GET', options);
            },
            startDownloading: function(options) {
                var queryParams = Utils.getUrlState.getQueryParams(),
                    url = queryParams.searchType === "basic" ? UrlLinks.downloadBasicSearchResultsCSV() : UrlLinks.downloadAdvanceSearchResultsCSV(),
                    options = _.extend({
                        contentType: 'application/json',
                        dataType: 'json'
                    }, options);

                return this.constructor.nonCrudOperation.call(this, url, 'POST', options);
            }
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
    return VDownloadLists;
});