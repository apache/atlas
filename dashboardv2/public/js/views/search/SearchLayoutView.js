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
    'utils/UrlLinks',
    'utils/Globals',
], function(require, Backbone, SearchLayoutViewTmpl, Utils, UrlLinks, Globals) {
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
                typeLov: '[data-id="typeLOV"]',
                tagLov: '[data-id="tagLOV"]',
                refreshBtn: '[data-id="refreshBtn"]',
                advancedInfoBtn: '[data-id="advancedInfo"]',
                typeAttrFilter: '[data-id="typeAttrFilter"]',
                tagAttrFilter: '[data-id="tagAttrFilter"]'
            },

            /** ui events hash */
            events: function() {
                var events = {},
                    that = this;
                events["keyup " + this.ui.searchInput] = function(e) {
                    var code = e.which;
                    if (code == 13) {
                        that.findSearchResult();
                    }
                    this.checkForButtonVisiblity();
                };
                events["change " + this.ui.searchType] = 'dslFulltextToggle';
                events["click " + this.ui.searchBtn] = 'findSearchResult';
                events["click " + this.ui.clearSearch] = 'clearSearchData';
                events["change " + this.ui.typeLov] = 'checkForButtonVisiblity';
                events["change " + this.ui.tagLov] = 'checkForButtonVisiblity';
                events["click " + this.ui.refreshBtn] = 'onRefreshButton';
                events["click " + this.ui.advancedInfoBtn] = 'advancedInfo';
                events["click " + this.ui.typeAttrFilter] = function() {
                    this.openAttrFilter('type');
                };
                events["click " + this.ui.tagAttrFilter] = function() {
                    this.openAttrFilter('tag');
                };
                return events;
            },
            /**
             * intialize a new SearchLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'value', 'typeHeaders', 'searchVent', 'entityDefCollection', 'enumDefCollection', 'classificationDefCollection', 'filterObj'));
                this.type = "basic";
                var param = Utils.getUrlState.getQueryParams();
                this.query = {
                    dsl: {
                        query: null,
                        type: null
                    },
                    basic: {
                        query: null,
                        type: null,
                        typeFilter: null,
                        tagFilter: null,
                        tag: null
                    }
                };
                if (!this.value) {
                    this.value = {};
                }
                this.dsl = false;
                if (param && param.searchType) {
                    this.type = param.searchType;
                    this.updateQueryObject(param);
                }
                this.bindEvents();
            },
            bindEvents: function(param) {
                this.listenTo(this.typeHeaders, "reset", function(value) {
                    this.renderTypeTagList();
                    this.setValues();
                    this.ui.typeLov.select2({
                        placeholder: "Select",
                        allowClear: true
                    });
                    this.ui.tagLov.select2({
                        placeholder: "Select",
                        allowClear: true
                    });
                    this.checkForButtonVisiblity();
                }, this);
            },
            bindSelect2Events: function(argument) {
                var that = this;
                this.ui.typeLov.on('select2:select', function(argument) {
                    // this function calles after checkForButtonVisiblity that is why disabled flter here
                    that.ui.typeAttrFilter.prop('disabled', false);
                    _.extend(that.value, { 'type': this.value });
                    that.makeFilterButtonActive('type');
                });
                this.ui.tagLov.on('select2:select', function(argument) {
                    // this function calles after checkForButtonVisiblity that is why disabled flter here
                    that.ui.tagAttrFilter.prop('disabled', false);
                    _.extend(that.value, { 'tag': this.value });
                    that.makeFilterButtonActive('tag');
                });
                this.ui.typeLov.on('select2:unselect', function(argument) {
                    _.extend(that.value, { 'type': null });
                });
                this.ui.tagLov.on('select2:unselect', function(argument) {
                    _.extend(that.value, { 'tag': null });
                });
            },
            makeFilterButtonActive: function(type) {
                if (this.filterObj) {
                    var tagFilters = this.filterObj.tagFilters,
                        entityFilters = this.filterObj.entityFilters;
                    if (type == "type") {
                        if (_.has(entityFilters, this.value[type])) {
                            this.query[this.type]['entityFilters'] = +new Date();
                            this.ui.typeAttrFilter.addClass('active');
                        } else {
                            this.query[this.type]['entityFilters'] = null;
                            this.ui.typeAttrFilter.removeClass('active');
                        }
                    }
                    if (type == "tag") {
                        if (_.has(tagFilters, this.value[type])) {
                            this.query[this.type]['tagFilters'] = +new Date();
                            this.ui.tagAttrFilter.addClass('active');
                        } else {
                            this.query[this.type]['tagFilters'] = null;
                            this.ui.tagAttrFilter.removeClass('active');
                        }
                    }
                }
            },
            checkForButtonVisiblity: function() {
                var that = this,
                    value = this.ui.searchInput.val() || this.ui.typeLov.val();
                if (!this.dsl && !value) {
                    value = this.ui.tagLov.val();
                }
                if (value && value.length) {
                    this.ui.searchBtn.removeAttr("disabled");
                    setTimeout(function() {
                        that.ui.searchInput.focus();
                    }, 0);
                } else {
                    this.ui.searchBtn.attr("disabled", "true");
                }
                if (this.value) {
                    if (this.value.tag) {
                        this.ui.tagAttrFilter.prop('disabled', false);
                    } else {
                        this.ui.tagAttrFilter.prop('disabled', true);
                    }
                    if (this.value.type) {
                        this.ui.typeAttrFilter.prop('disabled', false);
                    } else {
                        this.ui.typeAttrFilter.prop('disabled', true);
                    }
                    this.makeFilterButtonActive('type');
                    this.makeFilterButtonActive('tag');
                } else {
                    this.ui.tagAttrFilter.prop('disabled', true);
                    this.ui.typeAttrFilter.prop('disabled', true);
                }

            },
            onRender: function() {
                // array of tags which is coming from url
                this.renderTypeTagList();
                this.setValues();
                this.ui.typeLov.select2({
                    placeholder: "Select",
                    allowClear: true
                });
                this.ui.tagLov.select2({
                    placeholder: "Select",
                    allowClear: true
                });
                this.bindSelect2Events();
                this.checkForButtonVisiblity();
            },
            updateQueryObject: function(param) {
                if (param && param.searchType) {
                    this.type = param.searchType;
                }
                _.extend(this.query[this.type],
                    (this.type == "dsl" ? {
                        query: null,
                        type: null
                    } : {
                        query: null,
                        type: null,
                        tag: null,
                        entityFilters: null,
                        tagFilters: null
                    }), param);
            },
            fetchCollection: function(value) {
                this.typeHeaders.fetch({ reset: true });
            },
            onRefreshButton: function() {
                this.fetchCollection();
                //to check url query param contain type or not 
                var checkURLValue = Utils.getUrlState.getQueryParams(this.url);
                if (this.searchVent && (_.has(checkURLValue, "tag") || _.has(checkURLValue, "type") || _.has(checkURLValue, "query"))) {
                    this.searchVent.trigger('search:refresh');
                }
            },
            advancedInfo: function(e) {
                require([
                    'views/search/AdvancedSearchInfoView',
                    'modules/Modal'
                ], function(AdvancedSearchInfoView, Modal) {
                    var view = new AdvancedSearchInfoView();
                    var modal = new Modal({
                        title: 'Advanced Search Queries',
                        content: view,
                        okCloses: true,
                        showFooter: true,
                        allowCancel: false
                    }).open();
                    view.on('closeModal', function() {
                        modal.trigger('cancel');
                    });
                });
            },
            openAttrFilter: function(filterType) {
                var that = this;
                require(['views/search/SearchQueryView'], function(SearchQueryView) {
                    that.attrModal = new SearchQueryView({
                        value: that.value,
                        tag: (filterType === "tag" ? true : false),
                        type: (filterType === "type" ? true : false),
                        searchVent: that.searchVent,
                        typeHeaders: that.typeHeaders,
                        entityDefCollection: that.entityDefCollection,
                        enumDefCollection: that.enumDefCollection,
                        filterObj: that.filterObj,
                        classificationDefCollection: that.classificationDefCollection
                    });
                    that.attrModal.on('ok', function(e) {
                        that.okAttrFilterButton();
                    });
                });
            },
            okAttrFilterButton: function() {
                var filtertype = this.attrModal.tag ? 'tagFilters' : 'entityFilters',
                    rule = this.attrModal.RQueryBuilder.currentView.ui.builder.queryBuilder('getRules'),
                    result = this.getQueryBuilderParsData(rule);

                if (result) {
                    if (!_.isEmpty(result.criterion)) {
                        this.query[this.type][filtertype] = +new Date();
                        if (result) {
                            var filterObj = this.filterObj ? this.filterObj[filtertype] : null;
                            if (!filterObj) {
                                filterObj = {};
                            }
                            var temp = {}; // IE fix
                            temp[(this.attrModal.tag ? this.value.tag : this.value.type)] = { 'result': result, 'rule': rule };
                            _.extend(filterObj, temp);
                            this.filterObj[filtertype] = filterObj;
                            this.makeFilterButtonActive(this.attrModal.tag ? 'tag' : 'type');
                            Utils.localStorage.setValue((filtertype), JSON.stringify(filterObj));
                        } else {
                            this.filterObj[filtertype] = null;
                            this.query[this.type][filtertype] = null;
                            this.makeFilterButtonActive(this.attrModal.tag ? 'tag' : 'type');
                            Utils.localStorage.removeValue(filtertype);
                        }

                    }
                    this.attrModal.modal.close();
                } else {
                    this.filterObj[filtertype] = null;
                    this.query[this.type][filtertype] = null;
                    this.makeFilterButtonActive(this.attrModal.tag ? 'tag' : 'type');
                    Utils.localStorage.removeValue(filtertype);
                }
            },
            getQueryBuilderParsData: function(obj) {
                if (obj) {
                    var parsObj = {
                        "condition": obj.condition,
                        "criterion": convertKeyAndExtractObj(obj.rules)
                    }
                }

                function convertKeyAndExtractObj(rules) {
                    var convertObj = [];
                    _.each(rules, function(rulObj) {
                        var tempObj = {}
                        if (rulObj.rules) {
                            tempObj = {
                                "condition": rulObj.condition,
                                "criterion": convertKeyAndExtractObj(rulObj.rules)
                            }
                        } else {
                            tempObj = {
                                "attributeName": rulObj.id,
                                "operator": rulObj.operator,
                                "attributeValue": (rulObj.type === "date" ? Date.parse(rulObj.value) : rulObj.value)
                            }
                        }
                        convertObj.push(tempObj);
                    });
                    return convertObj;
                }
                return parsObj;
            },
            manualRender: function(paramObj) {
                this.updateQueryObject(paramObj);
                this.setValues(paramObj);
            },
            renderTypeTagList: function() {
                var that = this;
                this.ui.typeLov.empty();
                var typeStr = '<option></option>',
                    tagStr = typeStr;
                this.typeHeaders.fullCollection.comparator = function(model) {
                    return Utils.getName(model.toJSON(), 'name').toLowerCase();
                }
                this.typeHeaders.fullCollection.sort().each(function(model) {
                    var name = Utils.getName(model.toJSON(), 'name');
                    if (model.get('category') == 'ENTITY') {
                        typeStr += '<option>' + (name) + '</option>';
                    }
                    if (model.get('category') == 'CLASSIFICATION') {
                        var checkTagOrTerm = Utils.checkTagOrTerm(name);
                        if (checkTagOrTerm.tag) {
                            tagStr += '<option>' + (name) + '</option>';
                        }
                    }
                });
                that.ui.typeLov.html(typeStr);
                that.ui.tagLov.html(tagStr);
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
                    this.ui.typeLov.val(this.value.type);
                    if (this.ui.typeLov.data('select2')) {
                        if (this.ui.typeLov.val() !== this.value.type) {
                            this.value.type = null;
                            this.ui.typeLov.val("").trigger("change");
                        } else {
                            this.ui.typeLov.trigger("change");
                        }
                    }

                    if (!this.dsl) {
                        this.ui.tagLov.val(this.value.tag);
                        if (this.ui.tagLov.data('select2')) {
                            // To handle delete scenario.
                            if (this.ui.tagLov.val() !== this.value.tag) {
                                this.value.tag = null;
                                this.ui.tagLov.val("").trigger("change");
                            } else {
                                this.ui.tagLov.trigger("change");
                            }
                        }
                    }
                    this.ui.searchInput.val(this.value.query || "");
                    setTimeout(function() {
                        that.ui.searchInput.focus();
                    }, 0);
                    //this.searchVent.trigger('searchAttribute', this.value);
                }
            },
            findSearchResult: function() {
                this.triggerSearch(this.ui.searchInput.val());
            },
            triggerSearch: function(value) {
                this.query[this.type].query = value || null;
                var params = {
                    searchType: this.type,
                    dslChecked: this.ui.searchType.is(':checked')
                }
                this.query[this.type].type = this.ui.typeLov.select2('val') || null;
                if (!this.dsl) {
                    this.query[this.type].tag = this.ui.tagLov.select2('val') || null;
                }
                if (this.dsl) {
                    params['attributes'] = null;
                } else {
                    var columnList = JSON.parse(Utils.localStorage.getValue('columnList'));
                    if (columnList) {
                        params['attributes'] = columnList[this.query[this.type].type];
                    } else {
                        params['attributes'] = null;
                    }
                }

                Utils.setUrl({
                    url: '#!/search/searchResult',
                    urlParams: _.extend(this.query[this.type], params),
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
                    this.$('.tagBox').hide();
                    this.$('.temFilterBtn').hide();
                    this.$('.temFilter').toggleClass('col-sm-10 col-sm-12');
                } else {
                    this.$('.temFilter').toggleClass('col-sm-10 col-sm-12');
                    this.$('.temFilterBtn').show();
                    this.$('.tagBox').show();
                    this.dsl = false;
                    this.type = "basic";
                }
                if (Utils.getUrlState.getQueryParams() && this.type == Utils.getUrlState.getQueryParams().searchType) {
                    this.updateQueryObject(Utils.getUrlState.getQueryParams());
                }
                Utils.setUrl({
                    url: '#!/search/searchResult',
                    urlParams: _.extend(this.query[this.type], {
                        searchType: this.type,
                        dslChecked: this.ui.searchType.is(':checked')
                    }),
                    updateTabState: function() {
                        return { searchUrl: this.url, stateChanged: true };
                    },
                    mergeBrowserUrl: false,
                    trigger: true
                });
            },
            clearSearchData: function() {
                this.updateQueryObject();
                this.ui.typeLov.val("").trigger("change");
                this.ui.tagLov.val("").trigger("change");
                this.ui.searchInput.val("");
                this.checkForButtonVisiblity();
                Utils.localStorage.removeValue('tagFilters');
                Utils.localStorage.removeValue('entityFilters');
                this.filterObj.tagFilters = null;
                this.filterObj.entityFilters = null;
                Utils.setUrl({
                    url: '#!/search/searchResult',
                    urlParams: {
                        searchType: this.type,
                        dslChecked: this.ui.searchType.is(':checked')
                    },
                    mergeBrowserUrl: false,
                    trigger: true
                });
            }
        });
    return SearchLayoutView;
});