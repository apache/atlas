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
    'utils/Utils',
], function(require, Backbone, SearchLayoutViewTmpl, Utils) {
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
                searchType: 'input[name="queryType"]'
            },
            /** ui events hash */
            events: function() {
                var events = {},
                    that = this;
                events["click " + this.ui.searchButton] = 'onSearchButtonClick';
                events["keyup " + this.ui.searchInput] = function(e) {
                    var code = e.which;
                    if (code == 13) {
                        Utils.setUrl({
                            url: '#!/dashboard/assetPage',
                            urlParams: {
                                query: this.ui.searchInput.val(),
                                searchType: this.type
                            },
                            mergeBrowserUrl: false,
                            trigger: false
                        });
                        that.onSearchButtonClick(e.currentTarget.value);
                    }
                };
                events["change " + this.ui.searchType] = function(e) {
                    this.type = e.currentTarget.value;
                    if (this.ui.searchInput.val() !== "") {
                        this.onSearchButtonClick();
                    }
                    this.ui.searchInput.attr("placeholder", this.type == "dsl" ? 'Search using a DSL query: e.g. DataSet where name="sales_fact "' : 'Search using a query string: e.g. sales_fact');
                    Utils.setUrl({
                        url: '#!/dashboard/assetPage',
                        urlParams: {
                            query: this.ui.searchInput.val(),
                            searchType: this.type
                        },
                        mergeBrowserUrl: false,
                        trigger: false
                    });
                };
                return events;
            },
            /**
             * intialize a new SearchLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'globalVent', 'vent'));
                this.bindEvents();
                this.type = "fulltext";
            },
            bindEvents: function() {
                this.listenTo(this.vent, "tag:click", function(tagvalues) {
                    if (tagvalues.searchType) {
                        this.type = tagvalues.searchType;
                        this.$('input[name="queryType"][value=' + this.type + ']').prop('checked', true);
                    }
                    if (tagvalues.query) {
                        this.ui.searchInput.val(tagvalues.query);
                        this.triggerSearch(tagvalues.query);
                    }
                }, this);
            },
            onRender: function() {},
            onSearchButtonClick: function(e) {
                this.triggerSearch(this.ui.searchInput.val());
            },
            triggerSearch: function(value) {
                this.vent.trigger("search:click", { 'query': value, type: this.type });
            }
        });
    return SearchLayoutView;
});
