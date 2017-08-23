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
    'utils/CommonViewFunction'
], function(require, Backbone, SearchLayoutViewTmpl, Utils, UrlLinks, Globals, CommonViewFunction) {
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
                _.extend(this, _.pick(options, 'value', 'typeHeaders', 'searchVent', 'entityDefCollection', 'enumDefCollection', 'classificationDefCollection', 'searchTableColumns', 'searchTableFilters'));
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
                        tag: null,
                        attributes: null
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
            makeFilterButtonActive: function(filtertypeParam) {
                var filtertype = ['entityFilters', 'tagFilters'],
                    that = this;
                if (filtertypeParam) {
                    if (_.isArray(filtertypeParam)) {
                        filtertype = filtertypeParam;
                    } else if (_.isString(filtertypeParam)) {
                        filtertype = [filtertypeParam];
                    }
                }
                var typeCheck = function(filterObj, type) {
                    if (that.value.type) {
                        if (filterObj && filterObj.length) {
                            that.ui.typeAttrFilter.addClass('active');
                        } else {
                            that.ui.typeAttrFilter.removeClass('active');
                        }
                        that.ui.typeAttrFilter.prop('disabled', false);
                    } else {
                        that.ui.typeAttrFilter.removeClass('active');
                        that.ui.typeAttrFilter.prop('disabled', true);
                    }
                }
                var tagCheck = function(filterObj, type) {
                    if (that.value.tag) {
                        that.ui.tagAttrFilter.prop('disabled', false);
                        if (filterObj && filterObj.length) {
                            that.ui.tagAttrFilter.addClass('active');
                        } else {
                            that.ui.tagAttrFilter.removeClass('active');
                        }
                    } else {
                        that.ui.tagAttrFilter.removeClass('active');
                        that.ui.tagAttrFilter.prop('disabled', true);
                    }
                }
                _.each(filtertype, function(type) {
                    var filterObj = that.searchTableFilters[type];
                    if (type == "entityFilters") {
                        typeCheck(filterObj[that.value.type], type);
                    }
                    if (type == "tagFilters") {
                        tagCheck(filterObj[that.value.tag], type);
                    }
                });
            },
            checkForButtonVisiblity: function(e) {
                if (this.type == "basic" && e && e.currentTarget) {
                    var $el = $(e.currentTarget),
                        isTagEl = $el.data('id') == "tagLOV" ? true : false;
                    if (e.type == "change" && $el.select2('data')) {
                        var value = $el.val(),
                            key = (isTagEl ? 'tag' : 'type'),
                            filterType = (isTagEl ? 'tagFilters' : 'entityFilters'),
                            value = value.length ? value : null;
                        if (this.value) {
                            //On Change handle
                            if (this.value[key] !== value || (!value && !this.value[key])) {
                                var temp = {};
                                temp[key] = value;
                                _.extend(this.value, temp);
                            } else {
                                // Initial loading handle.
                                var filterObj = this.searchTableFilters[filterType];
                                if (filterObj && this.value[key]) {
                                    this.searchTableFilters[filterType][this.value[key]] = this.value[filterType] ? this.value[filterType] : null;
                                }
                            }
                            this.makeFilterButtonActive(filterType);
                        } else {
                            this.ui.tagAttrFilter.prop('disabled', true);
                            this.ui.typeAttrFilter.prop('disabled', true);
                        }
                    }
                }
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
                        attributes: null
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
                        classificationDefCollection: that.classificationDefCollection,
                        searchTableFilters: that.searchTableFilters
                    });
                    that.attrModal.on('ok', function(scope, e) {
                        that.okAttrFilterButton(e);
                    });
                });
            },
            okAttrFilterButton: function(e) {
                var isTag = this.attrModal.tag ? true : false,
                    filtertype = isTag ? 'tagFilters' : 'entityFilters',
                    queryBuilderRef = this.attrModal.RQueryBuilder.currentView.ui.builder;
                if (queryBuilderRef.data('queryBuilder')) {
                    var rule = queryBuilderRef.queryBuilder('getRules');
                }
                if (rule) {
                    var ruleUrl = CommonViewFunction.attributeFilter.generateUrl(rule.rules);
                    this.searchTableFilters[filtertype][(isTag ? this.value.tag : this.value.type)] = ruleUrl;
                    this.makeFilterButtonActive(filtertype);
                    if (!isTag && this.value && this.value.type && this.searchTableColumns) {
                        if (!this.searchTableColumns[this.value.type]) {
                            this.searchTableColumns[this.value.type] = ["selected", "name", "owner", "description", "tag", "typeName"]
                        }
                        this.searchTableColumns[this.value.type] = _.sortBy(_.union(this.searchTableColumns[this.value.type], _.pluck(rule.rules, 'id')));
                    }
                    this.attrModal.modal.close();
                    if ($(e.currentTarget).hasClass('search')) {
                        this.findSearchResult();
                    }
                }
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
                    if (this.value.dslChecked == "true") {
                        if (!this.ui.searchType.prop("checked")) {
                            this.ui.searchType.prop("checked", true).trigger("change");
                        }
                    } else {
                        if (this.ui.searchType.prop("checked")) {
                            this.ui.searchType.prop("checked", false).trigger("change");
                        }
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
                }
            },
            findSearchResult: function() {
                this.triggerSearch(this.ui.searchInput.val());
            },
            triggerSearch: function(value) {
                this.query[this.type].query = value || null;
                var params = {
                    searchType: this.type,
                    dslChecked: this.ui.searchType.is(':checked'),
                    tagFilters: null,
                    entityFilters: null,
                    attributes: null,
                    includeDE: null
                }
                this.query[this.type].type = this.ui.typeLov.select2('val') || null;
                if (!this.dsl) {
                    this.query[this.type].tag = this.ui.tagLov.select2('val') || null;
                    var entityFilterObj = this.searchTableFilters['entityFilters'],
                        tagFilterObj = this.searchTableFilters['tagFilters'];
                    if (this.value.tag) {
                        params['tagFilters'] = tagFilterObj[this.value.tag]
                    }
                    if (this.value.type) {
                        params['entityFilters'] = entityFilterObj[this.value.type]
                    }
                    var columnList = this.value && this.value.type && this.searchTableColumns ? this.searchTableColumns[this.value.type] : null;
                    if (columnList) {
                        params['attributes'] = columnList.join(',');
                    }
                    if (this.value.includeDE) {
                        params['includeDE'] = this.value.includeDE
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
                var paramObj = Utils.getUrlState.getQueryParams();
                if (e.currentTarget.checked) {
                    this.type = "dsl";
                    this.dsl = true;
                    this.$('.tagBox').hide();
                    this.$('.temFilterBtn').hide();
                    this.$('.temFilter').addClass('col-sm-12');
                    this.$('.temFilter').removeClass('col-sm-10');
                } else {
                    this.$('.temFilter').addClass('col-sm-10');
                    this.$('.temFilter').removeClass('col-sm-12');
                    this.$('.temFilterBtn').show();
                    this.$('.tagBox').show();
                    this.dsl = false;
                    this.type = "basic";
                }
                if (paramObj && this.type == paramObj.searchType) {
                    this.updateQueryObject(paramObj);
                }
                if (paramObj && this.type == "basic") {
                    this.query[this.type].attributes = paramObj.attributes ? paramObj.attributes : null;
                }
                if (Utils.getUrlState.isSearchTab()) {
                    Utils.setUrl({
                        url: '#!/search/searchResult',
                        urlParams: _.extend(this.query[this.type], {
                            searchType: this.type,
                            dslChecked: this.ui.searchType.is(':checked'),
                            includeDE: this.value.includeDE
                        }),
                        updateTabState: function() {
                            return { searchUrl: this.url, stateChanged: true };
                        },
                        mergeBrowserUrl: false,
                        trigger: true
                    });
                }
            },
            clearSearchData: function() {
                this.updateQueryObject();
                this.ui.typeLov.val("").trigger("change");
                this.ui.tagLov.val("").trigger("change");
                this.ui.searchInput.val("");
                if (!this.dsl) {
                    this.searchTableFilters.tagFilters = {};
                    this.searchTableFilters.entityFilters = {};
                }
                this.checkForButtonVisiblity();
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