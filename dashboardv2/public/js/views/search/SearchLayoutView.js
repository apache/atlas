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
    'collection/VTagList',
    'utils/Utils',
], function(require, Backbone, SearchLayoutViewTmpl, VTagList, Utils) {
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
                searchInput: '[data-id="searchInput"]',
                searchType: 'input[name="queryType"]',
                searchBtn: '[data-id="searchBtn"]',
                clearSearch: '[data-id="clearSearch"]',
                typeLov: '[data-id="typeLOV"]'
            },
            /** ui events hash */
            events: function() {
                var events = {},
                    that = this;
                events["keyup " + this.ui.searchInput] = function(e) {
                    var code = e.which;
                    this.ui.searchBtn.removeAttr("disabled");
                    if (code == 13) {
                        that.findSearchResult();
                    }
                    if (code == 8 && this.ui.searchInput.val() == "" && this.ui.typeLov.val() == "") {
                        this.ui.searchBtn.attr("disabled", "true");
                    }
                };
                events["change " + this.ui.searchType] = 'dslFulltextToggle';
                events["click " + this.ui.searchBtn] = 'findSearchResult';
                events["click " + this.ui.clearSearch] = 'clearSearchData';
                events["change " + this.ui.typeLov] = 'onChangeTypeList';
                return events;
            },
            /**
             * intialize a new SearchLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'globalVent', 'value'));
                this.typecollection = new VTagList([], {});
                this.type = "fulltext";
                var param = Utils.getUrlState.getQueryParams();
                this.query = {
                    dsl: {
                        query: ""
                    },
                    fulltext: {
                        query: ""
                    }
                };
                this.dsl = false;
                if (param && param.query && param.searchType) {
                    this.query[param.searchType].query = param.query;
                }
                this.bindEvents();
            },
            bindEvents: function(param) {
                this.listenTo(this.typecollection, "reset", function(value) {
                    this.renderTypeList();
                    this.setValues();
                    this.ui.typeLov.select2({
                        placeholder: "Search For",
                        allowClear: true
                    });
                }, this);
            },
            onRender: function() {
                // array of tags which is coming from url
                this.$('.typeLOV').hide();
                this.fetchCollection();
                this.ui.searchBtn.attr("disabled", "true");
            },
            fetchCollection: function(value) {
                $.extend(this.typecollection.queryParams, { type: 'CLASS' });
                this.typecollection.fetch({ reset: true });
            },
            manualRender: function(paramObj) {
                this.setValues(paramObj);
            },
            renderTypeList: function() {
                var that = this;
                this.ui.typeLov.empty();
                var str = '<option></option>';
                this.typecollection.fullCollection.comparator = function(model) {
                    return model.get('tags');
                }
                this.typecollection.fullCollection.sort().each(function(model) {
                    str += '<option>' + model.get("tags") + '</option>';
                });
                that.ui.typeLov.html(str);
            },
            onChangeTypeList: function(e) {
                if (this.ui.typeLov.select2('val') !== "") {
                    this.ui.searchBtn.removeAttr("disabled");
                } else if (this.ui.searchInput.val() === "") {
                    this.ui.searchBtn.attr("disabled", "true");
                }
            },
            setValues: function(paramObj) {
                var arr = [],
                    that = this;
                if (paramObj) {
                    this.value = paramObj;
                }
                if (this.value) {
                    if (this.value.dslChecked == "true" && this.dsl == false) {
                        this.ui.searchType.prop("checked", true).trigger("change");
                    } else if (this.value.dslChecked == "false" && this.dsl == true) {
                        this.ui.searchType.prop("checked", false).trigger("change");
                    }
                    if (this.value.query !== undefined) {
                        // get only search value and append it to input box
                        if (this.dsl) {
                            var query = this.value.query.split(" ");
                            if (query.length > 1) {
                                var typeList = query.shift();
                            } else {
                                var typeList = "";
                            }
                            if (this.ui.typeLov.data('select2')) {
                                this.ui.typeLov.val(typeList).trigger('change');
                            } else {
                                this.ui.typeLov.val(typeList);
                            }
                            this.ui.searchInput.val(query.join(" "));
                        } else {
                            this.ui.searchInput.val(this.value.query);
                        }
                        this.ui.searchBtn.removeAttr("disabled");
                    }

                }
                this.bindEvents(arr);
            },
            findSearchResult: function() {
                this.triggerSearch(this.ui.searchInput.val());
            },
            triggerSearch: function(value) {
                if (this.ui.searchType.is(':checked')) {
                    this.type = "dsl";
                } else if (!this.ui.searchType.is(':checked')) {
                    this.type = "fulltext";
                }
                if (this.ui.typeLov.select2('val') !== null && this.dsl === true) {
                    this.query[this.type].query = this.ui.typeLov.select2('val') + ' ' + value;
                } else {
                    this.query[this.type].query = value
                }
                Utils.setUrl({
                    url: '#!/search/searchResult',
                    urlParams: {
                        query: this.query[this.type].query,
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
            dslFulltextToggle: function(e) {
                var paramQuery = "";
                if (e.currentTarget.checked) {
                    this.type = "dsl";
                    this.dsl = true;
                    this.$('.typeLOV').show();
                } else {
                    this.dsl = false;
                    this.$('.typeLOV').hide();
                    this.type = "fulltext";
                }
                Utils.setUrl({
                    url: '#!/search/searchResult',
                    urlParams: {
                        query: this.query[this.type].query,
                        searchType: this.type,
                        dslChecked: this.ui.searchType.is(':checked')
                    },
                    updateTabState: function() {
                        return { searchUrl: this.url, stateChanged: true };
                    },
                    mergeBrowserUrl: false,
                    trigger: true
                });
                this.ui.searchInput.attr("placeholder", this.type == "dsl" ? 'Optional conditions' : 'Search using a query string: e.g. sales_fact');
            },
            clearSearchData: function() {
                this.query[this.type].query = "";
                this.ui.typeLov.val("").trigger("change");
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
