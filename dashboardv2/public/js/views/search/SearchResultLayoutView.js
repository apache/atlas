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
                editEntityButton: "[data-id='editEntityButton']",
                createEntity: "[data-id='createEntity']",
                checkDeletedEntity: "[data-id='checkDeletedEntity']",
                containerCheckBox: "[data-id='containerCheckBox']",
                columnEmptyInfo: "[data-id='columnEmptyInfo']"
            },
            templateHelpers: function() {
                return {
                    entityCreate: Globals.entityCreate,
                    searchType: this.searchType
                };
            },
            /** ui events hash */
            events: function() {
                var events = {};
                events["click " + this.ui.tagClick] = function(e) {
                    var scope = $(e.currentTarget);
                    if (e.target.nodeName.toLocaleLowerCase() == "i") {
                        this.onClickTagCross(e);
                    } else {
                        if (scope.hasClass('term')) {
                            var url = scope.data('href').split(".").join("/terms/");
                            Globals.saveApplicationState.tabState.stateChanged = false;
                            Utils.setUrl({
                                url: '#!/taxonomy/detailCatalog' + UrlLinks.taxonomiesApiUrl() + '/' + url,
                                mergeBrowserUrl: false,
                                trigger: true
                            });
                        } else {
                            Utils.setUrl({
                                url: '#!/tag/tagAttribute/' + scope.text(),
                                mergeBrowserUrl: false,
                                trigger: true
                            });
                        }
                    }
                };
                events["click " + this.ui.addTag] = 'checkedValue';
                events["click " + this.ui.addTerm] = 'checkedValue';
                events["click " + this.ui.addAssignTag] = 'checkedValue';
                events["click " + this.ui.showMoreLess] = function(e) {
                    $(e.currentTarget).parents('tr').siblings().find("div.popover.popoverTag").hide();
                    $(e.currentTarget).parent().find("div.popover").toggle();
                    var positionContent = $(e.currentTarget).position();
                    positionContent.top = positionContent.top + 26;
                    positionContent.left = positionContent.left - 67;
                    $(e.currentTarget).parent().find("div.popover").css(positionContent);
                };
                events["click " + this.ui.showMoreLessTerm] = function(e) {
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
                events["click " + this.ui.editEntityButton] = "onClickEditEntity";
                events["click " + this.ui.createEntity] = 'onClickCreateEntity';
                events["click " + this.ui.checkDeletedEntity] = 'onCheckDeletedEntity';
                return events;
            },
            /**
             * intialize a new SearchResultLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'value', 'initialView', 'entityDefCollection', 'typeHeaders', 'searchVent', 'enumDefCollection', 'tagCollection', 'searchTableColumns'));
                this.entityModel = new VEntity();
                this.searchCollection = new VSearchList();
                this.limit = 25;
                this.asyncFetchCounter = 0;
                this.offset = 0;
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
                            buttonTemplate: _.template("<button class='btn btn-atlasAction btn-atlas'>Columns&nbsp<i class='fa fa-caret-down'></i></button>")
                        }
                    },
                    gridOpts: {
                        emptyText: 'No Record found!',
                        className: 'table table-hover backgrid table-quickMenu'
                    },
                    filterOpts: {},
                    paginatorOpts: {}
                };
                this.bindEvents();
                this.bradCrumbList = [];
                this.arr = [];
                this.searchType = 'Basic Search';
                if (this.value && this.value.searchType && this.value.searchType == 'dsl') {
                    this.searchType = 'Advanced Search';
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

                        Utils.setUrl({
                            url: '#!/search/searchResult',
                            urlParams: this.value,
                            mergeBrowserUrl: false,
                            trigger: false,
                            updateTabState: function() {
                                return { searchUrl: this.url, stateChanged: true };
                            }
                        });
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
                    this.fetchCollection(value);
                    $('body').click(function(e) {
                        var iconEvnt = e.target.nodeName;
                        if (that.$('.popoverContainer').length) {
                            if ($(e.target).hasClass('tagDetailPopover') || iconEvnt == "I") {
                                return;
                            }
                            that.$('.popover.popoverTag').hide();
                        }
                    });
                } else {
                    if (Globals.entityTypeConfList) {
                        this.$(".entityLink").show();
                    }
                }
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
            generateQueryOfFilter: function() {
                var value = this.value,
                    entityFilters = CommonViewFunction.attributeFilter.extractUrl(value.entityFilters),
                    tagFilters = CommonViewFunction.attributeFilter.extractUrl(value.tagFilters),
                    queryArray = [],
                    objToString = function(filterObj) {
                        var tempObj = [];
                        _.each(filterObj, function(obj) {
                            tempObj.push('<span class="key">' + _.escape(obj.id) + '</span>&nbsp<span class="operator">' + _.escape(obj.operator) + '</span>&nbsp<span class="value">' + _.escape(obj.value) + "</span>")
                        });
                        return tempObj.join('&nbsp<span class="operator">AND</span>&nbsp');
                    }
                if (value.type) {
                    var typeKeyValue = '<span class="key">Type:</span>&nbsp<span class="value">' + _.escape(value.type) + '</span>';
                    if (entityFilters) {
                        typeKeyValue += '&nbsp<span class="operator">AND</span>&nbsp' + objToString(entityFilters);
                    }
                    queryArray.push(typeKeyValue)
                }
                if (value.tag) {
                    var tagKeyValue = '<span class="key">Tag:</span>&nbsp<span class="value">' + _.escape(value.tag) + '</span>';
                    if (tagFilters) {
                        tagKeyValue += '&nbsp<span class="operator">AND</span>&nbsp' + objToString(tagFilters);
                    }
                    queryArray.push(tagKeyValue);
                }
                if (value.query) {
                    queryArray.push('<span class="key">Query:</span>&nbsp<span class="value">' + _.escape(value.query) + '</span>&nbsp');
                }
                if (queryArray.length == 1) {
                    return queryArray.join();
                } else {
                    return "<span>(</span>&nbsp" + queryArray.join('<span>&nbsp)</span>&nbsp<span>AND</span>&nbsp<span>(</span>&nbsp') + "&nbsp<span>)</span>";

                }
            },
            fetchCollection: function(value, clickObj) {
                var that = this,
                    isPostMethod = this.value.searchType === "basic" && Utils.getUrlState.isSearchTab(),
                    tagFilters = CommonViewFunction.attributeFilter.generateAPIObj(this.value.tagFilters),
                    entityFilters = CommonViewFunction.attributeFilter.generateAPIObj(this.value.entityFilters);
                if (isPostMethod) {
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
                        if (!(that.ui.pageRecordText instanceof jQuery)) {
                            return;
                        }
                        if ((isPostMethod ? !dataOrCollection.entities : !dataOrCollection.length) && that.offset >= that.limit) {
                            that.ui.nextData.attr('disabled', true);
                            that.offset = that.offset - that.limit;
                            that.hideLoader();
                            return;
                        }
                        if (isPostMethod) {
                            that.searchCollection.referredEntities = dataOrCollection.referredEntities;
                            that.searchCollection.reset(dataOrCollection.entities);
                        }
                        if (that.searchCollection.models.length < that.limit) {
                            that.ui.nextData.attr('disabled', true);
                        } else {
                            that.ui.nextData.attr('disabled', false);
                        }
                        if (that.offset === 0) {
                            that.pageFrom = 1;
                            that.pageTo = that.limit;
                        } else if (clickObj && clickObj.next) {
                            //on next click, adding "1" for showing the another records.
                            that.pageTo = that.offset + that.limit;
                            that.pageFrom = that.offset + 1;
                        } else if (clickObj && clickObj.previous) {
                            that.pageTo = that.pageTo - that.limit;
                            that.pageFrom = (that.pageTo - that.limit) + 1;
                        }
                        that.ui.pageRecordText.html("Showing  <u>" + that.searchCollection.models.length + " records</u> From " + that.pageFrom + " - " + that.pageTo);
                        if (that.offset < that.limit && that.pageFrom < 26) {
                            that.ui.previousData.attr('disabled', true);
                        }
                        that.renderTableLayoutView();

                        if (value && !value.profileDBView) {
                            var searchString = 'Results for: <span class="filterQuery">' + that.generateQueryOfFilter() + "</span>";
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
                    _.extend(this.searchCollection.queryParams, { 'limit': this.limit, 'query': (value.query ? value.query.trim() : null), 'typeName': value.type || null, 'classification': value.tag || null });
                    if (value.profileDBView && value.guid) {
                        var profileParam = {};
                        profileParam['guid'] = value.guid;
                        profileParam['relation'] = '__hive_table.db';
                        profileParam['sortBy'] = 'name';
                        profileParam['sortOrder'] = 'ASCENDING';
                        $.extend(this.searchCollection.queryParams, profileParam);
                    }
                    if (isPostMethod) {
                        this.searchCollection.filterObj = _.extend({}, filterObj);
                        apiObj['data'] = _.extend({
                            'excludeDeletedEntities': (this.value && this.value.includeDE ? false : true)
                        }, filterObj, _.pick(this.searchCollection.queryParams, 'query', 'limit', 'offset', 'typeName', 'classification'))
                        Globals.searchApiCallRef = this.searchCollection.getBasicRearchResult(apiObj);
                    } else {
                        apiObj.data = null;
                        this.searchCollection.filterObj = null;
                        Globals.searchApiCallRef = this.searchCollection.fetch(apiObj);
                    }
                } else {
                    if (isPostMethod) {
                        apiObj['data'] = _.extend({
                            'excludeDeletedEntities': (this.value && this.value.includeDE ? false : true)
                        }, filterObj, _.pick(this.searchCollection.queryParams, 'query', 'limit', 'offset', 'typeName', 'classification'));
                        Globals.searchApiCallRef = this.searchCollection.getBasicRearchResult(apiObj);
                    } else {
                        apiObj.data = null;
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
                    var columns = new columnCollection(that.getFixedDslColumn());
                    columns.setPositions().sort();
                    that.REntityTableLayoutView.show(new TableLayout(_.extend({}, that.commonTableOptions, {
                        columns: columns
                    })));
                    if (that.value.searchType !== "dsl") {
                        that.ui.containerCheckBox.show();
                    } else {
                        that.ui.containerCheckBox.hide();
                    }
                    that.ui.paginationDiv.show();
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
                                nameHtml = '<a title="' + name + '">' + name + '</a>';
                            }
                            if (obj.status && Enums.entityStateReadOnly[obj.status]) {
                                nameHtml += '<button type="button" title="Deleted" class="btn btn-atlasAction btn-atlas deleteBtn"><i class="fa fa-trash"></i></button>';
                                return '<div class="readOnly readOnlyLink">' + nameHtml + '</div>';
                            } else {
                                if (Globals.entityUpdate) {
                                    if (Globals.entityTypeConfList && _.isEmptyArray(Globals.entityTypeConfList)) {
                                        nameHtml += '<button title="Edit" data-id="editEntityButton"  data-giud= "' + obj.guid + '" class="btn btn-atlasAction btn-atlas editBtn"><i class="fa fa-pencil"></i></button>'
                                    } else {
                                        if (_.contains(Globals.entityTypeConfList, obj.typeName)) {
                                            nameHtml += '<button title="Edit" data-id="editEntityButton"  data-giud= "' + obj.guid + '" class="btn btn-atlasAction btn-atlas editBtn"><i class="fa fa-pencil"></i></button>'
                                        }
                                    }
                                }
                                return nameHtml;
                            }
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
                this.$('.ellipsis,.labelShowRecord').show(); // only for first time
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
                var that = this;
                this.ui.previousData.removeAttr("disabled");
                that.offset = that.offset + that.limit;
                $.extend(this.searchCollection.queryParams, {
                    offset: that.offset
                });
                this.fetchCollection(null, {
                    next: true
                });
            },
            onClickpreviousData: function() {
                var that = this;
                this.ui.nextData.removeAttr("disabled");
                that.offset = that.offset - that.limit;
                $.extend(this.searchCollection.queryParams, {
                    offset: that.offset
                });
                this.fetchCollection(null, {
                    previous: true
                });
            },

            onClickEditEntity: function(e) {
                var that = this;
                $(e.currentTarget).blur();
                var guid = $(e.currentTarget).data('giud');
                require([
                    'views/entity/CreateEntityLayoutView'
                ], function(CreateEntityLayoutView) {
                    var view = new CreateEntityLayoutView({
                        guid: guid,
                        entityDefCollection: that.entityDefCollection,
                        typeHeaders: that.typeHeaders,
                        callback: function() {
                            that.fetchCollection();
                        }
                    });
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
                    Utils.setUrl({
                        url: '#!/search/searchResult',
                        urlParams: this.value,
                        mergeBrowserUrl: false,
                        trigger: false,
                        updateTabState: function() {
                            return { searchUrl: this.url, stateChanged: true };
                        }
                    });
                }
                this.fetchCollection();
            }
        });
    return SearchResultLayoutView;
});