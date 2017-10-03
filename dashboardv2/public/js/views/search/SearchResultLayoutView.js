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
    'hbs!tmpl/search/SearchResultLayoutView_tmpl',
    'modules/Modal',
    'models/VEntity',
    'utils/Utils',
    'utils/Globals',
    'collection/VSearchList',
    'models/VCommon',
    'utils/CommonViewFunction',
    'utils/Messages',
    'utils/Enums',
    'utils/UrlLinks'
], function(require, Backbone, SearchResultLayoutViewTmpl, Modal, VEntity, Utils, Globals, VSearchList, VCommon, CommonViewFunction, Messages, Enums, UrlLinks) {
    'use strict';

    var SearchResultLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends SearchResultLayoutView */
        {
            _viewName: 'SearchResultLayoutView',

            template: SearchResultLayoutViewTmpl,

            /** Layout sub regions */
            regions: {
                RTagLayoutView: "#r_tagLayoutView",
                RSearchLayoutView: "#r_searchLayoutView",
                REntityTableLayoutView: "#r_searchResultTableLayoutView",
                RSearchQuery: '#r_searchQuery'
            },

            /** ui selector cache */
            ui: {
                tagClick: '[data-id="tagClick"]',
                addTag: '[data-id="addTag"]',
                addTerm: '[data-id="addTerm"]',
                showMoreLess: '[data-id="showMoreLess"]',
                showMoreLessTerm: '[data-id="showMoreLessTerm"]',
                paginationDiv: '[data-id="paginationDiv"]',
                previousData: "[data-id='previousData']",
                nextData: "[data-id='nextData']",
                pageRecordText: "[data-id='pageRecordText']",
                addAssignTag: "[data-id='addAssignTag']",
                createEntity: "[data-id='createEntity']",
                checkDeletedEntity: "[data-id='checkDeletedEntity']",
                colManager: "[data-id='colManager']",
                containerCheckBox: "[data-id='containerCheckBox']",
                columnEmptyInfo: "[data-id='columnEmptyInfo']",
                showPage: "[data-id='showPage']",
                gotoPage: "[data-id='gotoPage']",
                gotoPagebtn: "[data-id='gotoPagebtn']",
                activePage: "[data-id='activePage']",
            },
            templateHelpers: function() {
                return {
                    entityCreate: Globals.entityCreate,
                    searchType: this.searchType,
                    taxonomy: Globals.taxonomy
                };
            },
            /** ui events hash */
            events: function() {
                var events = {},
                    that = this;
                events["click " + this.ui.tagClick] = function(e) {
                    var scope = $(e.currentTarget);
                    if (e.target.nodeName.toLocaleLowerCase() == "i") {
                        this.onClickTagCross(e);
                    } else {
                        if (scope.hasClass('term')) {
                            var url = scope.data('href').split(".").join("/terms/");
                            this.triggerUrl({
                                url: '#!/taxonomy/detailCatalog' + UrlLinks.taxonomiesApiUrl() + '/' + url,
                                urlParams: null,
                                mergeBrowserUrl: false,
                                trigger: true,
                                updateTabState: null
                            });
                        } else {
                            this.triggerUrl({
                                url: '#!/tag/tagAttribute/' + scope.text(),
                                urlParams: null,
                                mergeBrowserUrl: false,
                                trigger: true,
                                updateTabState: null
                            });
                        }
                    }
                };
                events["keyup " + this.ui.gotoPage] = function(e) {
                    var code = e.which,
                        goToPage = parseInt(e.currentTarget.value);
                    if (e.currentTarget.value) {
                        that.ui.gotoPagebtn.attr('disabled', false);
                    } else {
                        that.ui.gotoPagebtn.attr('disabled', true);
                    }
                    if (code == 13) {
                        if (e.currentTarget.value) {
                            that.gotoPagebtn();
                        }
                    }
                };
                events["change " + this.ui.showPage] = 'changePageLimit';
                events["click " + this.ui.gotoPagebtn] = 'gotoPagebtn';
                events["click " + this.ui.addTag] = 'checkedValue';
                events["click " + this.ui.addTerm] = 'checkedValue';
                events["click " + this.ui.addAssignTag] = 'checkedValue';
                events["click " + this.ui.showMoreLessTerm] = function(e) {
                    e.stopPropagation();
                    $(e.currentTarget).find('i').toggleClass('fa fa-angle-right fa fa-angle-up');
                    $(e.currentTarget).parents('.searchTerm').find('div.termTableBreadcrumb>div.showHideDiv').toggleClass('hide');
                    if ($(e.currentTarget).find('i').hasClass('fa-angle-right')) {
                        $(e.currentTarget).find('span').text('Show More');
                    } else {
                        $(e.currentTarget).find('span').text('Show less');
                    }
                };
                events["click " + this.ui.nextData] = "onClicknextData";
                events["click " + this.ui.previousData] = "onClickpreviousData";
                events["click " + this.ui.createEntity] = 'onClickCreateEntity';
                events["click " + this.ui.checkDeletedEntity] = 'onCheckDeletedEntity';
                return events;
            },
            /**
             * intialize a new SearchResultLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'value', 'initialView', 'isTypeTagNotExists', 'entityDefCollection', 'typeHeaders', 'searchVent', 'enumDefCollection', 'tagCollection', 'searchTableColumns'));
                this.entityModel = new VEntity();
                this.searchCollection = new VSearchList();
                this.limit = 25;
                this.asyncFetchCounter = 0;
                this.offset = 0;
                this.bindEvents();
                this.bradCrumbList = [];
                this.arr = [];
                this.searchType = 'Basic Search';
                if (this.value) {
                    if (this.value.searchType && this.value.searchType == 'dsl') {
                        this.searchType = 'Advanced Search';
                    }
                    if (this.value.pageLimit) {
                        var pageLimit = parseInt(this.value.pageLimit, 10);
                        if (_.isNaN(pageLimit) || pageLimit == 0 || pageLimit <= -1) {
                            this.value.pageLimit = this.limit;
                            this.triggerUrl();
                        } else {
                            this.limit = pageLimit;
                        }
                    }
                    if (this.value.pageOffset) {
                        var pageOffset = parseInt(this.value.pageOffset, 10);
                        if (_.isNaN(pageOffset) || pageLimit <= -1) {
                            this.value.pageOffset = this.offset;
                            this.triggerUrl();
                        } else {
                            this.offset = pageOffset;
                        }
                    }
                }
            },
            bindEvents: function() {
                var that = this;
                this.listenTo(this.searchCollection, 'backgrid:selected', function(model, checked) {
                    this.arr = [];
                    if (checked === true) {
                        model.set("isEnable", true);
                    } else {
                        model.set("isEnable", false);
                    }
                    this.searchCollection.find(function(item) {
                        if (item.get('isEnable')) {
                            var term = [];
                            var obj = item.toJSON();
                            that.arr.push({
                                id: obj.guid,
                                model: obj
                            });
                        }
                    });

                    if (this.arr.length > 0) {
                        if (Globals.taxonomy) {
                            this.$('.multiSelectTerm').show();
                        }
                        this.$('.multiSelectTag').show();
                    } else {
                        if (Globals.taxonomy) {
                            this.$('.multiSelectTerm').hide();
                        }
                        this.$('.multiSelectTag').hide();
                    }
                });
                this.listenTo(this.searchCollection, "error", function(model, response) {
                    this.$('.fontLoader').hide();
                    this.$('.tableOverlay').hide();
                    var responseJSON = response && response.responseJSON ? response.responseJSON : null;
                    if (responseJSON && (responseJSON.errorMessage || responseJSON.message || responseJSON.error)) {
                        Utils.notifyError({
                            content: responseJSON.errorMessage || responseJSON.message || responseJSON.error
                        });
                    } else {
                        if (response.statusText !== "abort") {
                            Utils.notifyError({
                                content: "Invalid Expression"
                            });
                        }
                    }
                }, this);
                this.listenTo(this.searchCollection, "state-changed", function(state) {
                    if (Utils.getUrlState.isSearchTab()) {
                        this.updateColumnList(state);
                        var excludeDefaultColumn = [];
                        if (this.value && this.value.type) {
                            excludeDefaultColumn = _.without(this.searchTableColumns[this.value.type], "selected", "name", "description", "typeName", "owner", "tag", "terms");
                            if (this.searchTableColumns[this.value.type] === null) {
                                this.ui.columnEmptyInfo.show();
                            } else {
                                this.ui.columnEmptyInfo.hide();
                            }
                        }
                        this.triggerUrl();
                        if (excludeDefaultColumn.length > this.searchCollection.filterObj.attributes.length) {
                            this.fetchCollection(this.value);
                        }

                    }
                }, this);
                this.listenTo(this.searchVent, "search:refresh", function(model, response) {
                    this.fetchCollection();
                }, this);
            },
            onRender: function() {
                var that = this;
                this.commonTableOptions = {
                    collection: this.searchCollection,
                    includePagination: false,
                    includeFooterRecords: false,
                    includeColumnManager: (Utils.getUrlState.isSearchTab() && this.value && this.value.searchType === "basic" && !this.value.profileDBView ? true : false),
                    includeOrderAbleColumns: false,
                    includeSizeAbleColumns: false,
                    includeTableLoader: false,
                    columnOpts: {
                        opts: {
                            initialColumnsVisible: null,
                            saveState: false
                        },
                        visibilityControlOpts: {
                            buttonTemplate: _.template("<button class='btn btn-action btn-md pull-right'>Columns&nbsp<i class='fa fa-caret-down'></i></button>")
                        },
                        el: this.ui.colManager
                    },
                    gridOpts: {
                        emptyText: 'No Record found!',
                        className: 'table table-hover backgrid table-quickMenu'
                    },
                    filterOpts: {},
                    paginatorOpts: {}
                };
                if (!this.initialView) {
                    var value = {},
                        that = this;
                    if (this.value) {
                        value = this.value;
                        if (value && value.includeDE) {
                            this.ui.checkDeletedEntity.prop('checked', true);
                        }
                    } else {
                        value = {
                            'query': null,
                            'searchType': 'basic'
                        };
                    }
                    this.updateColumnList();
                    if (this.value && this.searchTableColumns && this.searchTableColumns[this.value.type] === null) {
                        this.ui.columnEmptyInfo.show();
                    } else {
                        this.ui.columnEmptyInfo.hide();
                    }
                    this.fetchCollection(value, _.extend({ 'fromUrl': true }, (this.value && this.value.pageOffset ? { 'next': true } : null)));
                    this.ui.showPage.select2({
                        data: _.sortBy(_.union([25, 50, 100, 150, 200, 250, 300, 350, 400, 450, 500], [this.limit])),
                        tags: true,
                        dropdownCssClass: "number-input",
                        multiple: false
                    });
                    if (this.value && this.value.pageLimit) {
                        this.ui.showPage.val(this.limit).trigger('change', { "skipViewChange": true });
                    }
                } else {
                    if (Globals.entityTypeConfList) {
                        this.$(".entityLink").show();
                    }
                    if (this.isTypeTagNotExists) {
                        Utils.notifyError({
                            content: Messages.search.notExists
                        });
                    }
                }
            },
            triggerUrl: function(options) {
                Utils.setUrl(_.extend({
                    url: Utils.getUrlState.getQueryUrl().queyParams[0],
                    urlParams: this.value,
                    mergeBrowserUrl: false,
                    trigger: false,
                    updateTabState: true
                }, options));
            },
            updateColumnList: function(updatedList) {
                if (updatedList) {
                    var listOfColumns = [];
                    _.map(updatedList, function(obj) {
                        var key = obj.name;
                        if (obj.renderable) {
                            listOfColumns.push(obj.name);
                        }
                    });
                    listOfColumns = _.sortBy(listOfColumns);
                    this.value.attributes = listOfColumns.length ? listOfColumns.join(",") : null;
                    if (this.value && this.value.type && this.searchTableColumns) {
                        this.searchTableColumns[this.value.type] = listOfColumns.length ? listOfColumns : null;
                    }
                } else if (this.value && this.value.type && this.searchTableColumns && this.value.attributes) {
                    this.searchTableColumns[this.value.type] = this.value.attributes.split(",");
                }
            },
            fetchCollection: function(value, options) {
                var that = this,
                    isPostMethod = (this.value && this.value.searchType === "basic"),
                    isSearchTab = Utils.getUrlState.isSearchTab(),
                    tagFilters = null,
                    entityFilters = null;
                if (isSearchTab) {
                    tagFilters = CommonViewFunction.attributeFilter.generateAPIObj(this.value.tagFilters);
                    entityFilters = CommonViewFunction.attributeFilter.generateAPIObj(this.value.entityFilters);
                }

                if (isPostMethod && isSearchTab) {
                    var excludeDefaultColumn = this.value.type && this.searchTableColumns ? _.without(this.searchTableColumns[this.value.type], "selected", "name", "description", "typeName", "owner", "tag", "terms") : null,
                        filterObj = {
                            'entityFilters': entityFilters,
                            'tagFilters': tagFilters,
                            'attributes': excludeDefaultColumn ? excludeDefaultColumn : null
                        };
                }

                this.showLoader();
                if (Globals.searchApiCallRef && Globals.searchApiCallRef.readyState === 1) {
                    Globals.searchApiCallRef.abort();
                }
                var apiObj = {
                    skipDefaultError: true,
                    sort: false,
                    success: function(dataOrCollection, response) {
                        Globals.searchApiCallRef = undefined;
                        var isFirstPage = that.offset === 0,
                            dataLength = 0,
                            goToPage = that.ui.gotoPage.val();
                        if (!(that.ui.pageRecordText instanceof jQuery)) {
                            return;
                        }
                        if (isPostMethod && dataOrCollection && dataOrCollection.entities) {
                            dataLength = dataOrCollection.entities.length;
                        } else {
                            dataLength = dataOrCollection.length;
                        }

                        if (!dataLength && that.offset >= that.limit && ((options && options.next) || goToPage) && (options && !options.fromUrl)) {
                            /* User clicks on next button and server returns 
                            empty response then disabled the next button without rendering table*/

                            that.hideLoader();
                            var pageNumber = that.activePage + 1;
                            if (goToPage) {
                                pageNumber = goToPage;
                                that.offset = (that.activePage - 1) * that.limit;
                            } else {
                                that.ui.nextData.attr('disabled', true);
                                that.offset = that.offset - that.limit;
                            }
                            if (that.value) {
                                that.value.pageOffset = that.offset;
                                that.triggerUrl();
                            }
                            Utils.notifyInfo({
                                html: true,
                                content: Messages.search.noRecordForPage + '<b>' + Utils.getNumberSuffix({ number: pageNumber, sup: true }) + '</b> page'
                            });
                            return;
                        }
                        if (isPostMethod) {
                            that.searchCollection.referredEntities = dataOrCollection.rnoRecordFoeferredEntities;
                            that.searchCollection.reset(dataOrCollection.entities, { silent: true });
                        }

                        /*Next button check.
                        It's outside of Previous button else condition 
                        because when user comes from 2 page to 1 page than we need to check next button.*/
                        if (dataLength < that.limit) {
                            that.ui.nextData.attr('disabled', true);
                        } else {
                            that.ui.nextData.attr('disabled', false);
                        }

                        if (isFirstPage && (!dataLength || dataLength < that.limit)) {
                            that.ui.paginationDiv.hide();
                        } else {
                            that.ui.paginationDiv.show();
                        }

                        // Previous button check.
                        if (isFirstPage) {
                            that.ui.previousData.attr('disabled', true);
                            that.pageFrom = 1;
                            that.pageTo = that.limit;
                        } else {
                            that.ui.previousData.attr('disabled', false);
                        }

                        if (options && options.next) {
                            //on next click, adding "1" for showing the another records.
                            that.pageTo = that.offset + that.limit;
                            that.pageFrom = that.offset + 1;
                        } else if (!isFirstPage && options && options.previous) {
                            that.pageTo = that.pageTo - that.limit;
                            that.pageFrom = (that.pageTo - that.limit) + 1;
                        }
                        that.ui.pageRecordText.html("Showing  <u>" + that.searchCollection.models.length + " records</u> From " + that.pageFrom + " - " + that.pageTo);
                        that.activePage = Math.round(that.pageTo / that.limit);
                        that.ui.activePage.attr('title', "Page " + that.activePage);
                        that.ui.activePage.text(that.activePage);
                        that.renderTableLayoutView();

                        if (value && !value.profileDBView) {
                            var searchString = 'Results for: <span class="filterQuery">' + CommonViewFunction.generateQueryOfFilter(that.value) + "</span>";
                            if (Globals.entityCreate && Globals.entityTypeConfList && Utils.getUrlState.isSearchTab()) {
                                searchString += "<p>If you do not find the entity in search result below then you can" + '<a href="javascript:void(0)" data-id="createEntity"> create new entity</a></p>';
                            }
                            that.$('.searchResult').html(searchString);
                        }
                    },
                    silent: true,
                    reset: true
                }
                if (value) {
                    if (value.searchType) {
                        this.searchCollection.url = UrlLinks.searchApiUrl(value.searchType);
                    }
                    _.extend(this.searchCollection.queryParams, { 'limit': this.limit, 'offset': this.offset, 'query': (value.query ? value.query.trim() : null), 'typeName': value.type || null, 'classification': value.tag || null });
                    if (value.profileDBView && value.guid) {
                        var profileParam = {};
                        profileParam['guid'] = value.guid;
                        profileParam['relation'] = '__hive_table.db';
                        profileParam['sortBy'] = 'name';
                        profileParam['sortOrder'] = 'ASCENDING';
                        _.extend(this.searchCollection.queryParams, profileParam);
                    }
                    if (isPostMethod) {
                        this.searchCollection.filterObj = _.extend({}, filterObj);
                        apiObj['data'] = _.extend({
                            'excludeDeletedEntities': (this.value && this.value.includeDE ? false : true)
                        }, filterObj, _.pick(this.searchCollection.queryParams, 'query', 'excludeDeletedEntities', 'limit', 'offset', 'typeName', 'classification'))
                        Globals.searchApiCallRef = this.searchCollection.getBasicRearchResult(apiObj);
                    } else {
                        apiObj.data = null;
                        this.searchCollection.filterObj = null;
                        if (this.value.profileDBView) {
                            _.extend(this.searchCollection.queryParams, { 'excludeDeletedEntities': (this.value && this.value.includeDE ? false : true) });
                        }
                        Globals.searchApiCallRef = this.searchCollection.fetch(apiObj);
                    }
                } else {
                    if (isPostMethod) {
                        apiObj['data'] = _.extend({
                            'excludeDeletedEntities': (this.value && this.value.includeDE ? false : true)
                        }, filterObj, _.pick(this.searchCollection.queryParams, 'query', 'excludeDeletedEntities', 'limit', 'offset', 'typeName', 'classification'));
                        Globals.searchApiCallRef = this.searchCollection.getBasicRearchResult(apiObj);
                    } else {
                        apiObj.data = null;
                        if (this.value.profileDBView) {
                            _.extend(this.searchCollection.queryParams, { 'excludeDeletedEntities': (this.value && this.value.includeDE ? false : true) });
                        }
                        Globals.searchApiCallRef = this.searchCollection.fetch(apiObj);
                    }
                }
            },
            renderSearchQueryView: function() {
                var that = this;
                require(['views/search/SearchQueryView'], function(SearchQueryView) {
                    that.RSearchQuery.show(new SearchQueryView({
                        value: that.value,
                        searchVent: that.searchVent
                    }));
                });
            },
            renderTableLayoutView: function(col) {
                var that = this;
                require(['utils/TableLayout'], function(TableLayout) {
                    // displayOrder added for column manager
                    var columnCollection = Backgrid.Columns.extend({
                        sortKey: "displayOrder",
                        comparator: function(item) {
                            return item.get(this.sortKey) || 999;
                        },
                        setPositions: function() {
                            _.each(this.models, function(model, index) {
                                model.set("displayOrder", index + 1, { silent: true });
                            });
                            return this;
                        }
                    });
                    that.bradCrumbList = [];
                    var columns = new columnCollection((that.searchCollection.dynamicTable ? that.getDaynamicColumns(that.searchCollection.toJSON()) : that.getFixedDslColumn()));
                    columns.setPositions().sort();
                    that.REntityTableLayoutView.show(new TableLayout(_.extend({}, that.commonTableOptions, {
                        columns: columns
                    })));
                    if (that.value.searchType !== "dsl") {
                        that.ui.containerCheckBox.show();
                    } else {
                        that.ui.containerCheckBox.hide();
                    }
                    that.$(".ellipsis .inputAssignTag").hide();
                    that.renderBreadcrumb();
                    that.checkTableFetch();
                });
            },
            renderBreadcrumb: function() {
                var that = this;
                _.each(this.bradCrumbList, function(object) {
                    _.each(object.value, function(subObj) {
                        var scopeObject = that.$('[dataterm-id="' + object.scopeId + '"]').find('[dataterm-name="' + subObj.name + '"] .liContent');
                        CommonViewFunction.breadcrumbMaker({ urlList: subObj.valueUrl, scope: scopeObject });
                    });
                });
            },
            checkTableFetch: function() {
                if (this.asyncFetchCounter <= 0) {
                    this.hideLoader();
                    Utils.generatePopover({
                        el: this.$('[data-id="showMoreLess"]'),
                        container: this.$el,
                        contentClass: 'popover-tag',
                        popoverOptions: {
                            content: function() {
                                return $(this).find('.popup-tag').children().clone();
                            }
                        }
                    });
                }
            },
            getFixedDslColumn: function() {
                var that = this,
                    nameCheck = 0,
                    columnToShow = null,
                    col = {};
                if (this.value && this.searchTableColumns && (this.searchTableColumns[this.value.type] !== undefined)) {
                    columnToShow = this.searchTableColumns[this.value.type] == null ? [] : this.searchTableColumns[this.value.type];
                }
                col['Check'] = {
                    name: "selected",
                    label: "Select",
                    cell: "select-row",
                    resizeable: false,
                    orderable: false,
                    renderable: (columnToShow ? _.contains(columnToShow, 'selected') : true),
                    headerCell: "select-all"
                };

                col['name'] = {
                    label: this.value && this.value.profileDBView ? "Table Name" : "Name",
                    cell: "html",
                    editable: false,
                    sortable: false,
                    resizeable: true,
                    orderable: true,
                    renderable: true,
                    className: "searchTableName",
                    formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                        fromRaw: function(rawValue, model) {
                            var obj = model.toJSON(),
                                nameHtml = "",
                                name = Utils.getName(obj);
                            if (obj.guid) {
                                nameHtml = '<a title="' + name + '" href="#!/detailPage/' + obj.guid + '">' + name + '</a>';
                            } else {
                                nameHtml = '<span title="' + name + '">' + name + '</span>';
                            }
                            if (obj.status && Enums.entityStateReadOnly[obj.status]) {
                                nameHtml += '<button type="button" title="Deleted" class="btn btn-action btn-md deleteBtn"><i class="fa fa-trash"></i></button>';
                                return '<div class="readOnly readOnlyLink">' + nameHtml + '</div>';
                            }
                            return nameHtml;
                        }
                    })
                };
                col['owner'] = {
                    label: "Owner",
                    cell: "String",
                    editable: false,
                    sortable: false,
                    resizeable: true,
                    orderable: true,
                    renderable: true,
                    formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                        fromRaw: function(rawValue, model) {
                            var obj = model.toJSON();
                            if (obj && obj.attributes && obj.attributes.owner) {
                                return obj.attributes.owner;
                            }
                        }
                    })
                };
                if (this.value && this.value.profileDBView) {
                    col['createTime'] = {
                        label: "Date Created",
                        cell: "Html",
                        editable: false,
                        sortable: false,
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                            fromRaw: function(rawValue, model) {
                                var obj = model.toJSON();
                                if (obj && obj.attributes && obj.attributes.createTime) {
                                    return new Date(obj.attributes.createTime);
                                } else {
                                    return '-'
                                }
                            }
                        })
                    }
                }
                if (this.value && !this.value.profileDBView) {
                    col['description'] = {
                        label: "Description",
                        cell: "String",
                        editable: false,
                        sortable: false,
                        resizeable: true,
                        orderable: true,
                        renderable: true,
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                            fromRaw: function(rawValue, model) {
                                var obj = model.toJSON();
                                if (obj && obj.attributes && obj.attributes.description) {
                                    return obj.attributes.description;
                                }
                            }
                        })
                    };
                    col['typeName'] = {
                        label: "Type",
                        cell: "Html",
                        editable: false,
                        sortable: false,
                        resizeable: true,
                        orderable: true,
                        renderable: (columnToShow ? _.contains(columnToShow, 'typeName') : true),
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                            fromRaw: function(rawValue, model) {
                                var obj = model.toJSON();
                                if (obj && obj.typeName) {
                                    return '<a title="Search ' + obj.typeName + '" href="#!/search/searchResult?query=' + obj.typeName + ' &searchType=dsl&dslChecked=true">' + obj.typeName + '</a>';
                                }
                            }
                        })
                    };
                    this.getTagTermCol({ 'col': col, 'columnToShow': columnToShow });

                    if (this.value && this.value.searchType === "basic") {
                        var def = this.entityDefCollection.fullCollection.find({ name: this.value.type });
                        if (def) {
                            var attrObj = Utils.getNestedSuperTypeObj({ data: def.toJSON(), collection: this.entityDefCollection, attrMerge: true });
                            _.each(attrObj, function(obj, key) {
                                var key = obj.name,
                                    isRenderable = _.contains(columnToShow, key)
                                if (key == "name" || key == "description" || key == "owner") {
                                    if (columnToShow) {
                                        col[key].renderable = isRenderable;
                                    }
                                    return;
                                }
                                col[obj.name] = {
                                    label: obj.name.capitalize(),
                                    cell: "Html",
                                    editable: false,
                                    sortable: false,
                                    resizeable: true,
                                    orderable: true,
                                    renderable: isRenderable,
                                    formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                                        fromRaw: function(rawValue, model) {
                                            var modelObj = model.toJSON();

                                            if (modelObj && modelObj.attributes && !_.isUndefined(modelObj.attributes[key])) {
                                                var tempObj = {
                                                    'scope': that,
                                                    'attributeDefs': [obj],
                                                    'valueObject': {},
                                                    'isTable': false
                                                }

                                                tempObj.valueObject[key] = modelObj.attributes[key]
                                                Utils.findAndMergeRefEntity(tempObj.valueObject, that.searchCollection.referredEntities);
                                                return CommonViewFunction.propertyTable(tempObj);
                                            }
                                        }
                                    })
                                };
                            });
                        }
                    }
                }
                return this.searchCollection.constructor.getTableCols(col, this.searchCollection);
            },
            getDaynamicColumns: function(valueObj) {
                var that = this,
                    col = {};
                if (valueObj && valueObj.length) {
                    var firstObj = _.first(valueObj);
                    _.each(_.keys(firstObj), function(key) {
                        if (key !== 'guid') {
                            col[key] = {
                                label: key.capitalize(),
                                cell: "Html",
                                editable: false,
                                sortable: false,
                                resizeable: true,
                                orderable: true,
                                formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                                    fromRaw: function(rawValue, model) {
                                        var modelObj = model.toJSON();
                                        if (key == "name") {
                                            var nameHtml = "",
                                                name = modelObj[key];
                                            if (modelObj.guid) {
                                                nameHtml = '<a title="' + name + '" href="#!/detailPage/' + modelObj.guid + '">' + name + '</a>';
                                            } else {
                                                nameHtml = '<span title="' + name + '">' + name + '</span>';
                                            }
                                            if (modelObj.status && Enums.entityStateReadOnly[modelObj.status]) {
                                                nameHtml += '<button type="button" title="Deleted" class="btn btn-action btn-md deleteBtn"><i class="fa fa-trash"></i></button>';
                                                return '<div class="readOnly readOnlyLink">' + nameHtml + '</div>';
                                            }
                                            return nameHtml;
                                        } else if (modelObj && !_.isUndefined(modelObj[key])) {
                                            var tempObj = {
                                                'scope': that,
                                                // 'attributeDefs':
                                                'valueObject': {},
                                                'isTable': false
                                            }

                                            tempObj.valueObject[key] = modelObj[key]
                                            Utils.findAndMergeRefEntity(tempObj.valueObject, that.searchCollection.referredEntities);
                                            return CommonViewFunction.propertyTable(tempObj);
                                        }
                                    }
                                })
                            };
                        }
                    });
                }
                return this.searchCollection.constructor.getTableCols(col, this.searchCollection);
            },
            getTagTermCol: function(options) {
                var that = this,
                    columnToShow = options.columnToShow,
                    col = options.col;
                if (col) {
                    col['tag'] = {
                        label: "Tags",
                        cell: "Html",
                        editable: false,
                        sortable: false,
                        resizeable: true,
                        orderable: true,
                        renderable: (columnToShow ? _.contains(columnToShow, 'tag') : true),
                        className: 'searchTag',
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                            fromRaw: function(rawValue, model) {
                                var obj = model.toJSON();
                                if (obj.status && Enums.entityStateReadOnly[obj.status]) {
                                    return '<div class="readOnly">' + CommonViewFunction.tagForTable(obj); + '</div>';
                                } else {
                                    return CommonViewFunction.tagForTable(obj);
                                }

                            }
                        })
                    };
                    if (Globals.taxonomy) {
                        col['terms'] = {
                            label: "Terms",
                            cell: "Html",
                            editable: false,
                            sortable: false,
                            resizeable: true,
                            orderable: true,
                            renderable: (columnToShow ? _.contains(columnToShow, 'terms') : true),
                            className: 'searchTerm',
                            formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                                fromRaw: function(rawValue, model) {
                                    var obj = model.toJSON();
                                    var returnObject = CommonViewFunction.termTableBreadcrumbMaker(obj);
                                    if (returnObject.object) {
                                        that.bradCrumbList.push(returnObject.object);
                                    }
                                    if (obj.status && Enums.entityStateReadOnly[obj.status]) {
                                        return '<div class="readOnly">' + returnObject.html + '</div>';
                                    } else {
                                        return returnObject.html;
                                    }
                                }
                            })
                        };
                    }
                }
            },
            addTagModalView: function(guid, multiple) {
                var that = this;
                require(['views/tag/addTagModalView'], function(AddTagModalView) {
                    var view = new AddTagModalView({
                        guid: guid,
                        multiple: multiple,
                        callback: function() {
                            that.fetchCollection();
                            that.arr = [];
                        },
                        tagList: that.getTagList(guid, multiple),
                        showLoader: that.showLoader.bind(that),
                        hideLoader: that.hideLoader.bind(that),
                        enumDefCollection: that.enumDefCollection
                    });
                });
            },
            getTagList: function(guid, multiple) {
                var that = this;
                if (!multiple || multiple.length === 0) {
                    var model = this.searchCollection.find(function(item) {
                        var obj = item.toJSON();
                        if (obj.guid === guid) {
                            return true;
                        }
                    });
                    if (model) {
                        var obj = model.toJSON();
                    } else {
                        return [];
                    }
                    return obj.classificationNames;
                } else {
                    return [];
                }
            },
            showLoader: function() {
                this.$('.fontLoader:not(.for-ignore)').show();
                this.$('.tableOverlay').show();
            },
            hideLoader: function() {
                this.$('.fontLoader:not(.for-ignore)').hide();
                this.$('.ellipsis,.pagination-box').show(); // only for first time
                this.$('.tableOverlay').hide();
            },
            checkedValue: function(e) {
                var guid = "",
                    that = this,
                    isTagMultiSelect = $(e.currentTarget).hasClass('multiSelectTag'),
                    isTermMultiSelect = $(e.currentTarget).hasClass('multiSelectTerm'),
                    isTagButton = $(e.currentTarget).hasClass('assignTag');
                if (isTagButton) {
                    if (isTagMultiSelect && this.arr && this.arr.length) {
                        that.addTagModalView(guid, this.arr);
                    } else {
                        guid = that.$(e.currentTarget).data("guid");
                        that.addTagModalView(guid);
                    }
                } else {
                    if (isTermMultiSelect && this.arr && this.arr.length) {
                        that.addTermModalView(guid, this.arr);
                    } else {
                        guid = that.$(e.currentTarget).data("guid");
                        that.addTermModalView(guid);
                    }
                }
            },
            addTermModalView: function(guid, multiple) {
                var that = this;
                require([
                    'views/business_catalog/AddTermToEntityLayoutView',
                ], function(AddTermToEntityLayoutView) {
                    var view = new AddTermToEntityLayoutView({
                        guid: guid,
                        multiple: multiple,
                        callback: function() {
                            that.fetchCollection();
                            that.arr = [];
                        },
                        showLoader: that.showLoader.bind(that),
                        hideLoader: that.hideLoader.bind(that)
                    });
                });
            },
            onClickTagCross: function(e) {
                var tagName = $(e.target).data("name"),
                    guid = $(e.target).data("guid"),
                    assetName = $(e.target).data("assetname"),
                    tagOrTerm = $(e.target).data("type"),
                    that = this;
                if (tagOrTerm === "term") {
                    var modal = CommonViewFunction.deleteTagModel({
                        msg: "<div class='ellipsis'>Remove: " + "<b>" + _.escape(tagName) + "</b> assignment from" + " " + "<b>" + assetName + " ?</b></div>",
                        titleMessage: Messages.removeTerm,
                        buttonText: "Remove"
                    });
                } else if (tagOrTerm === "tag") {
                    var modal = CommonViewFunction.deleteTagModel({
                        msg: "<div class='ellipsis'>Remove: " + "<b>" + _.escape(tagName) + "</b> assignment from" + " " + "<b>" + assetName + " ?</b></div>",
                        titleMessage: Messages.removeTag,
                        buttonText: "Remove"
                    });
                }
                if (modal) {
                    modal.on('ok', function() {
                        that.deleteTagData(e, tagOrTerm);
                    });
                    modal.on('closeModal', function() {
                        modal.trigger('cancel');
                    });
                }
            },
            deleteTagData: function(e, tagOrTerm) {
                var that = this,
                    tagName = $(e.target).data("name"),
                    guid = $(e.target).data("guid");
                CommonViewFunction.deleteTag({
                    'tagName': tagName,
                    'guid': guid,
                    'tagOrTerm': tagOrTerm,
                    showLoader: that.showLoader.bind(that),
                    hideLoader: that.hideLoader.bind(that),
                    callback: function() {
                        that.fetchCollection();
                    }
                });
            },
            onClicknextData: function() {
                this.offset = this.offset + this.limit;
                _.extend(this.searchCollection.queryParams, {
                    offset: this.offset
                });
                if (this.value) {
                    this.value.pageOffset = this.offset;
                    this.triggerUrl();
                }
                this.ui.gotoPage.val('');
                this.ui.gotoPage.parent().removeClass('has-error');
                this.fetchCollection(null, {
                    next: true
                });
            },
            onClickpreviousData: function() {
                this.offset = this.offset - this.limit;
                if (this.offset <= -1) {
                    this.offset = 0;
                }
                _.extend(this.searchCollection.queryParams, {
                    offset: this.offset
                });
                if (this.value) {
                    this.value.pageOffset = this.offset;
                    this.triggerUrl();
                }
                this.ui.gotoPage.val('');
                this.ui.gotoPage.parent().removeClass('has-error');
                this.fetchCollection(null, {
                    previous: true
                });
            },
            onClickCreateEntity: function(e) {
                var that = this;
                $(e.currentTarget).blur();
                require([
                    'views/entity/CreateEntityLayoutView'
                ], function(CreateEntityLayoutView) {
                    var view = new CreateEntityLayoutView({
                        entityDefCollection: that.entityDefCollection,
                        typeHeaders: that.typeHeaders,
                        callback: function() {
                            that.fetchCollection();
                        }
                    });
                });
            },
            onCheckDeletedEntity: function(e) {
                var includeDE = false;
                if (e.target.checked) {
                    includeDE = true;
                }
                if (this.value) {
                    this.value.includeDE = includeDE;
                    this.triggerUrl();
                }
                _.extend(this.searchCollection.queryParams, { limit: this.limit, offset: this.offset });
                this.fetchCollection();
            },
            changePageLimit: function(e, obj) {
                if (!obj || (obj && !obj.skipViewChange)) {
                    var limit = parseInt(this.ui.showPage.val());
                    if (limit == 0) {
                        this.ui.showPage.data('select2').$container.addClass('has-error');
                        return;
                    } else {
                        this.ui.showPage.data('select2').$container.removeClass('has-error');
                    }
                    this.limit = limit;
                    this.offset = 0;
                    if (this.value) {
                        this.value.pageLimit = this.limit;
                        this.value.pageOffset = this.offset;
                        this.triggerUrl();
                    }
                    this.ui.gotoPage.val('');
                    this.ui.gotoPage.parent().removeClass('has-error');
                    _.extend(this.searchCollection.queryParams, { limit: this.limit, offset: this.offset });
                    this.fetchCollection();
                }
            },
            gotoPagebtn: function(e) {
                var that = this;
                var goToPage = parseInt(this.ui.gotoPage.val());
                if (!(_.isNaN(goToPage) || goToPage <= -1)) {
                    this.offset = (goToPage - 1) * this.limit;
                    if (this.offset <= -1) {
                        this.offset = 0;
                    }
                    _.extend(this.searchCollection.queryParams, { limit: this.limit, offset: this.offset });
                    if (this.offset == (this.pageFrom - 1)) {
                        Utils.notifyInfo({
                            content: Messages.search.onSamePage
                        });
                    } else {
                        if (this.value) {
                            this.value.pageOffset = this.offset;
                            this.triggerUrl();
                        }
                        // this.offset is updated in gotoPagebtn function so use next button calculation.
                        this.fetchCollection(null, { 'next': true });
                    }
                }
            }
        });
    return SearchResultLayoutView;
});