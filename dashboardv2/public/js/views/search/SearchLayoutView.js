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
    'backbone',
    'hbs!tmpl/search/SearchLayoutView_tmpl',
    'collection/VSearchList',
    'utils/Utils',
    'collection/VTagList'
], function(require, Backbone, SearchLayoutViewTmpl, VSearchList, Utils, VTagList) {
    'use strict';

    var SearchLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends SearchLayoutView */
        {
            _viewName: 'SearchLayoutView',

            template: SearchLayoutViewTmpl,

            /** Layout sub regions */
            regions: {},

            /** ui selector cache */
            ui: {
                searchButton: '[data-id="searchButton"]',
                searchInput: '[data-id="searchInput"]',
                searchType: 'input[name="queryType"]',
                advanceSearch: '[data-id="advanceSearch"]',
                tagList: '[data-id="tagList"]',
                tagListInput: '[data-id="tagListInput"]',
                termListInput: '[data-id="termListInput"]',
                searchBtn: '[data-id="searchBtn"]',
                clearSearch: '[data-id="clearSearch"]'
            },
            /** ui events hash */
            events: function() {
                var events = {},
                    that = this;
                events["keyup " + this.ui.searchInput] = function(e) {
                    this.ui.searchBtn.removeAttr("disabled");
                    var code = e.which;
                    if (code == 13) {
                        that.findSearchResult();
                    }
                    if (code == 8 && this.ui.searchInput.val() == "") {
                        this.ui.searchBtn.attr("disabled", "true");
                    }
                };
                events["change " + this.ui.searchType] = 'dslFulltextToggle';
                events["click " + this.ui.searchBtn] = 'findSearchResult';
                events["click " + this.ui.clearSearch] = 'clearSearchData';
                return events;
            },
            /**
             * intialize a new SearchLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'globalVent', 'value'));
                this.searchCollection = new VSearchList([], {
                    state: {
                        firstPage: 0,
                        pageSize: 10
                    }
                });
                this.type = "fulltext";
                this.tagCollection = new VTagList();
                $.extend(this.tagCollection.queryParams, { type: 'TRAIT' });
                this.typeCollection = new VTagList();
                $.extend(this.typeCollection.queryParams, { type: 'CLASS' });
                this.bindEvents();
            },
            bindEvents: function(param) {
                this.listenTo(this.searchCollection, "reset", function(value) {
                    this.renderTree();
                }, this);
            },
            onRender: function() {
                // array of tags which is coming from url
                this.ui.searchBtn.attr("disabled", "true");
                this.setValues();
            },
            manualRender: function(paramObj) {
                this.setValues(paramObj);
            },
            setValues: function(paramObj) {
                var arr = [];
                if (paramObj) {
                    this.value = paramObj;
                }
                if (this.value) {
                    if (this.value.query.length) {
                        // get only search value and append it to input box
                        this.ui.searchInput.val(this.value.query);
                        this.ui.searchBtn.removeAttr("disabled");
                    }
                    if (this.value.dslChecked == "true") {
                        this.ui.searchType.prop("checked", true).trigger("change");
                    } else {
                        this.ui.searchType.prop("checked", false).trigger("change");
                    }
                }
                this.bindEvents(arr);
            },
            findSearchResult: function() {
                this.triggerSearch(this.ui.searchInput.val());
            },
            triggerSearch: function(value) {
                if (!this.ui.searchType.is(':checked')) {
                    this.type = "dsl";
                } else if (!this.ui.searchType.is(':checked')) {
                    this.type = "fulltext";
                }

                var advancedSearchValue = value;
                this.type = "fulltext";
                Utils.setUrl({
                    url: '#!/search/searchResult',
                    urlParams: {
                        query: advancedSearchValue,
                        searchType: this.type,
                        dslChecked: this.ui.searchType.is(':checked')
                    },
                    updateTabState: function() {
                        return { searchUrl: this.url, stateChanged: true };
                    },
                    mergeBrowserUrl: false,
                    trigger: true
                });
            },
            fetchCollection: function(value) {
                if (value) {
                    this.searchCollection.url = "/api/atlas/discovery/search/" + this.type;
                    $.extend(this.searchCollection.queryParams, { 'query': value });
                }
                this.searchCollection.fetch({ reset: true });
            },
            dslFulltextToggle: function(e) {
                if (e.currentTarget.checked) {
                    this.type = "dsl";
                } else {
                    this.type = "fulltext";
                }
                if (this.ui.searchInput.val() !== "") {
                    Utils.setUrl({
                        url: '#!/search/searchResult',
                        urlParams: {
                            query: this.ui.searchInput.val(),
                            searchType: this.type,
                            dslChecked: this.ui.searchType.is(':checked')
                        },
                        updateTabState: function() {
                            return { searchUrl: this.url, stateChanged: true };
                        },
                        mergeBrowserUrl: false,
                        trigger: true
                    });
                    //this.findSearchResult();
                }
                this.ui.searchInput.attr("placeholder", this.type == "dsl" ? 'Search using a DSL query: e.g. DataSet where name="sales_fact "' : 'Search using a query string: e.g. sales_fact');
            },
            clearSearchData: function() {
                this.ui.searchInput.val("");
                this.ui.searchBtn.attr("disabled", "true");
                Utils.setUrl({
                    url: '#!/search',
                    mergeBrowserUrl: false,
                    trigger: true
                });
            }
        });
    return SearchLayoutView;
});
