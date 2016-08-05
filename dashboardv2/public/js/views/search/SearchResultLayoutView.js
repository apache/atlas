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
    'utils/Messages'
], function(require, Backbone, SearchResultLayoutViewTmpl, Modal, VEntity, Utils, Globals, VSearchList, VCommon, CommonViewFunction, Messages) {
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
                addAssignTag: "[data-id='addAssignTag']"
            },

            /** ui events hash */
            events: function() {
                var events = {};
                events["click " + this.ui.tagClick] = function(e) {
                    if (e.target.nodeName.toLocaleLowerCase() == "i") {
                        this.onClickTagCross(e);
                    } else {
                        var scope = $(e.currentTarget);
                        if (scope.hasClass('term')) {
                            var url = scope.data('href').split(".").join("/terms/");
                            Globals.saveApplicationState.tabState.stateChanged = false;
                            Utils.setUrl({
                                url: '#!/taxonomy/detailCatalog/api/atlas/v1/taxonomies/' + url,
                                mergeBrowserUrl: false,
                                trigger: true
                            });
                        } else {
                            Utils.setUrl({
                                url: '#!/tag/tagAttribute/' + e.currentTarget.text,
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
                return events;
            },
            /**
             * intialize a new SearchResultLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'globalVent', 'vent', 'value'));
                var pagination = "";
                this.entityModel = new VEntity();
                this.searchCollection = new VSearchList();
                this.limit = 25;
                this.firstFetch = true;
                this.fetchList = 0;
                this.offset = 0;
                this.commonTableOptions = {
                    collection: this.searchCollection,
                    includeFilter: false,
                    includePagination: false,
                    includePageSize: false,
                    includeFooterRecords: false,
                    includeSizeAbleColumns: false,
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
                            that.arr.push({
                                id: item.get("$id$"),
                                model: item
                            });
                        }
                    });
                    if (this.arr.length > 0) {
                        this.$('.searchResult').find(".inputAssignTag.multiSelect").show();
                        this.$('.searchResult').find(".inputAssignTag.multiSelectTag").show();
                    } else {
                        this.$('.searchResult').find(".inputAssignTag.multiSelect").hide();
                        this.$('.searchResult').find(".inputAssignTag.multiSelectTag").hide();
                    }
                });
                this.listenTo(this.searchCollection, "error", function(value, responseData) {
                    this.$('.fontLoader').hide();
                    var message = "Invalid expression";
                    if (this.value && this.value.query) {
                        message += " : " + this.value.query;
                    }
                    if (responseData.responseText) {
                        message = JSON.parse(responseData.responseText).error;
                    }
                    Utils.notifyError({
                        content: message
                    });
                }, this);
            },
            onRender: function() {
                var value = {},
                    that = this;
                if (this.value) {
                    value = this.value;
                } else {
                    value = {
                        'query': '',
                        'searchType': 'fulltext'
                    };
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
            },
            fetchCollection: function(value) {
                var that = this;
                if (value && (value.query === undefined || value.query === "")) {
                    return;
                }
                this.$('.fontLoader').show();
                this.$('.searchTable').hide();
                this.$('.searchResult').hide();
                if (Globals.searchApiCallRef) {
                    Globals.searchApiCallRef.abort();
                }
                $.extend(this.searchCollection.queryParams, { limit: this.limit });
                if (value) {
                    if (value.searchType) {
                        this.searchCollection.url = "/api/atlas/discovery/search/" + value.searchType;
                        $.extend(this.searchCollection.queryParams, { limit: this.limit });
                        this.offset = 0;
                    }
                    if (Utils.getUrlState.isTagTab()) {
                        this.searchCollection.url = "/api/atlas/discovery/search/dsl";
                    }
                    _.extend(this.searchCollection.queryParams, { 'query': value.query.trim() });
                }
                Globals.searchApiCallRef = this.searchCollection.fetch({
                    success: function() {
                        if (that.searchCollection.models.length < that.limit) {
                            that.ui.nextData.attr('disabled', true);
                        }
                        Globals.searchApiCallRef = undefined;
                        if (that.offset === 0) {
                            that.pageFrom = 1;
                            that.pageTo = that.limit;
                            if (that.searchCollection.length > 0) {
                                that.ui.pageRecordText.html("Showing " + that.pageFrom + " - " + that.searchCollection.length);
                            }
                        } else if (that.searchCollection.models.length && !that.previousClick) {
                            //on next click, adding "1" for showing the another records..
                            that.pageFrom = that.pageTo + 1;
                            that.pageTo = that.pageTo + that.searchCollection.models.length;
                            if (that.pageFrom && that.pageTo) {
                                that.ui.pageRecordText.html("Showing " + that.pageFrom + " - " + that.pageTo);
                            }
                        } else if (that.previousClick) {
                            that.pageTo = (that.pageTo - (that.pageTo - that.pageFrom)) - 1;
                            //if limit is 0 then result is change to 1 because page count is showing from 1
                            that.pageFrom = (that.pageFrom - that.limit === 0 ? 1 : that.pageFrom - that.limit);
                            that.ui.pageRecordText.html("Showing " + that.pageFrom + " - " + that.pageTo);
                        }
                        if (that.searchCollection.models.length === 0) {
                            that.checkTableFetch();
                            that.offset = that.offset - that.limit;
                            if (that.firstFetch) {
                                that.startRenderTableProcess();
                            }
                        }
                        if (that.firstFetch) {
                            that.firstFetch = false;
                        }
                        if (that.offset < that.limit) {
                            that.ui.previousData.attr('disabled', true);
                        }
                        // checking length for not rendering the table
                        if (that.searchCollection.models.length) {
                            that.startRenderTableProcess();
                        }
                        var resultData = 'Results for <b>' + that.searchCollection.queryParams.query + '</b>'
                        var multiAssignDataTag = '<a href="javascript:void(0)" class="inputAssignTag multiSelectTag assignTag" style="display:none" data-id="addAssignTag"><i class="fa fa-plus"></i>' + " " + 'Assign Tag</a>'
                        if (Globals.taxonomy) {
                            var multiAssignDataTerm = '<a href="javascript:void(0)" class="inputAssignTag multiSelect" style="display:none" data-id="addTerm"><i class="fa fa-folder-o"></i>' + " " + 'Assign Term</a>'
                            that.$('.searchResult').html(resultData + multiAssignDataTerm + multiAssignDataTag);
                        } else {
                            that.$('.searchResult').html(resultData + multiAssignDataTag);
                        }

                    },
                    silent: true
                });
            },
            startRenderTableProcess: function() {
                this.getEntityTableColumns();
            },
            renderTableLayoutView: function(col) {
                var that = this,
                    count = 5;
                require(['utils/TableLayout'], function(TableLayout) {
                    var columns = new Backgrid.Columns(col);
                    that.REntityTableLayoutView.show(new TableLayout(_.extend({}, that.commonTableOptions, {
                        globalVent: that.globalVent,
                        columns: columns
                    })));
                    that.ui.paginationDiv.show();
                    that.$('.searchResult').find(".inputAssignTag.multiSelect").hide();
                    that.renderBreadcrumb();
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
                if (this.fetchList <= 0) {
                    this.$('div[data-id="r_tableSpinner"]').removeClass('show')
                    this.$('.fontLoader').hide();
                    this.$('.searchTable').show();
                    this.$('.searchResult').show();
                }
            },
            getEntityTableColumns: function() {
                var that = this,
                    col = {};
                var responseData = this.searchCollection.responseData;
                if (this.searchCollection.responseData) {
                    if (responseData.dataType && responseData.dataType.typeName.indexOf('_temp') == -1) {
                        that.renderTableLayoutView(that.getFixedDslColumn())
                    } else {
                        var idFound = false,
                            fetchResultCount = 0;
                        _.each(this.searchCollection.models, function(model) {
                            var modelJSON = model.toJSON();
                            var guid = "";
                            _.each(modelJSON, function(val, key) {
                                if (_.isObject(val)) {
                                    if (val.id) {
                                        model.set('id', val.id);
                                        guid = val.id;
                                    } else if (val.guid) {
                                        model.set('id', val.guid);
                                        guid = val.guid;
                                    }
                                } else if (key === "id") {
                                    model.set('id', val);
                                    guid = val;
                                } else if (key === "guid") {
                                    model.set('id', val);
                                    guid = val;
                                }
                            });
                            if (guid.length) {
                                idFound = true;
                                ++fetchResultCount;
                                model.getEntity(guid, {
                                    success: function(data) {
                                        if (data.definition) {
                                            if (data.definition.id && data.definition.values) {
                                                var id = "";
                                                if (_.isObject(data.definition.id) && data.definition.id.id) {
                                                    id = data.definition.id.id;
                                                } else {
                                                    id = data.definition.id;
                                                }
                                                that.searchCollection.get(id).set(data.definition.values);
                                                that.searchCollection.get(id).set('$id$', data.definition.id);
                                                that.searchCollection.get(id).set('$traits$', data.definition.traits);
                                            }
                                        }

                                    },
                                    error: function(error, data, status) {},
                                    complete: function() {
                                        --fetchResultCount;
                                        if (fetchResultCount === 0) {
                                            that.renderTableLayoutView(that.getFixedDslColumn())
                                        }
                                    }
                                });
                            }
                        });
                        if (idFound === false) {
                            that.renderTableLayoutView(this.getDaynamicColumn())
                        }
                    }
                }
            },
            getDaynamicColumn: function() {
                var that = this,
                    modelJSON = "",
                    col = {};
                modelJSON = this.searchCollection.toJSON()[0];
                _.keys(modelJSON).map(function(key) {
                    if (key.indexOf("$") == -1) {
                        col[key] = {
                            cell: 'Html',
                            editable: false,
                            sortable: false,
                            orderable: true,
                            formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                                fromRaw: function(rawValue, model) {
                                    return CommonViewFunction.propertyTable({ 'notUsedKey': rawValue }, that, true);
                                }
                            })
                        };
                    }
                });
                that.checkTableFetch();
                return this.searchCollection.constructor.getTableCols(col, this.searchCollection);
            },
            getFixedDslColumn: function() {
                var that = this,
                    nameCheck = 0,
                    col = {};
                for (var i = 0; i < this.searchCollection.models.length; i++) {
                    var model = this.searchCollection.models[i];
                    if (model && (model.get('name') || model.get('qualifiedName'))) {
                        ++nameCheck
                    }
                    if (model && model.get('$id$') === undefined) {
                        i = i - 1;
                        that.searchCollection.remove(model);
                    }
                }
                col['Check'] = {
                    name: "selected",
                    label: "",
                    cell: "select-row",
                    headerCell: "select-all"
                };
                if (nameCheck > 0) {
                    col['name'] = {
                        label: "Name",
                        cell: "html",
                        editable: false,
                        sortable: false,
                        className: "searchTableName",
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                            fromRaw: function(rawValue, model) {
                                var nameHtml = "";
                                if (rawValue === undefined) {
                                    if (model.get('qualifiedName')) {
                                        rawValue = model.get('qualifiedName');
                                    } else if (model.get('$id$') && model.get('$id$').qualifiedName) {
                                        rawValue = model.get('$id$').qualifiedName
                                    } else {
                                        return "";
                                    }
                                }
                                if (model.get('$id$') && model.get('$id$').id) {
                                    nameHtml = '<a href="#!/detailPage/' + model.get('$id$').id + '">' + rawValue + '</a>';
                                } else {
                                    nameHtml = '<a>' + rawValue + '</a>';
                                }
                                if (model.get('$id$') && model.get('$id$').state && Globals.entityStateReadOnly[model.get('$id$').state]) {
                                    nameHtml += '<button title="Deleted" class="btn btn-atlasAction btn-atlas deleteBtn"><i class="fa fa-trash"></i></button>';
                                    return '<div class="readOnly readOnlyLink">' + nameHtml + '</div>';
                                } else {
                                    return nameHtml;
                                }
                            }
                        })
                    }
                }
                if (nameCheck === 0) {
                    col['typeName'] = {
                        label: "Type Name",
                        cell: "html",
                        editable: false,
                        sortable: false,
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                            fromRaw: function(rawValue, model) {
                                var nameHtml = "";
                                if (rawValue === undefined) {
                                    if (model.get('$id$') && model.get('$id$')['$typeName$']) {
                                        rawValue = model.get('$id$')['$typeName$']
                                    } else if (model.get('$typeName$')) {
                                        rawValue = model.get('$typeName$');
                                    } else if (model.get('typeName')) {
                                        rawValue = model.get('typeName')
                                    } else {
                                        return "";
                                    }
                                }
                                if (model.get('$id$') && model.get('$id$').id) {
                                    nameHtml = '<a href="#!/detailPage/' + model.get('$id$').id + '">' + rawValue + '</a>';
                                } else {
                                    nameHtml = '<a>' + rawValue + '</a>';
                                }
                                if (model.get('$id$') && model.get('$id$').state && Globals.entityStateReadOnly[model.get('$id$').state]) {
                                    nameHtml += '<button title="Deleted" class="btn btn-atlasAction btn-atlas deleteBtn"><i class="fa fa-trash"></i></button>';
                                    return '<div class="readOnly readOnlyLink">' + nameHtml + '</div>';
                                } else {
                                    return nameHtml;
                                }
                            }
                        })
                    }
                }

                col['description'] = {
                    label: "Description",
                    cell: "String",
                    editable: false,
                    sortable: false
                }
                col['owner'] = {
                    label: "Owner",
                    cell: "String",
                    editable: false,
                    sortable: false
                }
                col['tag'] = {
                    label: "Tags",
                    cell: "Html",
                    editable: false,
                    sortable: false,
                    orderable: true,
                    className: 'searchTag',
                    formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                        fromRaw: function(rawValue, model) {
                            if (model.get('$id$') && model.get('$id$').state && Globals.entityStateReadOnly[model.get('$id$').state]) {
                                return '<div class="readOnly">' + CommonViewFunction.tagForTable(model); + '</div>';
                            } else {
                                return CommonViewFunction.tagForTable(model);
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
                        orderable: true,
                        className: 'searchTerm',
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                            fromRaw: function(rawValue, model) {
                                var returnObject = CommonViewFunction.termTableBreadcrumbMaker(model);
                                if (returnObject.object) {
                                    that.bradCrumbList.push(returnObject.object);
                                }
                                if (model.get('$id$') && model.get('$id$').state && Globals.entityStateReadOnly[model.get('$id$').state]) {
                                    return '<div class="readOnly">' + returnObject.html + '</div>';
                                } else {
                                    return returnObject.html;
                                }
                            }
                        })
                    };
                }
                that.checkTableFetch();
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
                        showLoader: function() {
                            that.$('.fontLoader').show();
                            that.$('.searchTable').hide();
                        }
                    });
                    // view.saveTagData = function() {
                    //override saveTagData function 
                    // }
                });
            },
            checkedValue: function(e) {
                var guid = "",
                    that = this;
                var multiSelectTag = $(e.currentTarget).hasClass('assignTag');
                if (multiSelectTag) {
                    if (this.arr && this.arr.length && multiSelectTag) {
                        that.addTagModalView(guid, this.arr);
                    } else {
                        guid = that.$(e.currentTarget).data("guid");
                        that.addTagModalView(guid);
                    }
                } else {
                    if (this.arr && this.arr.length) {
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
                        showLoader: function() {
                            that.$('.fontLoader').show();
                            that.$('.searchTable').hide();
                        }
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
                        msg: "<div class='ellipsis'>Remove: " + "<b>" + tagName + "</b> assignment from" + " " + "<b>" + assetName + " ?</b></div>",
                        titleMessage: Messages.removeTerm,
                        buttonText: "Remove"
                    });
                } else if (tagOrTerm === "tag") {
                    var modal = CommonViewFunction.deleteTagModel({
                        msg: "<div class='ellipsis'>Remove: " + "<b>" + tagName + "</b> assignment from" + " " + "<b>" + assetName + " ?</b></div>",
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
                this.previousClick = false;
                this.fetchCollection();
            },
            onClickpreviousData: function() {
                var that = this;
                this.ui.nextData.removeAttr("disabled");
                that.offset = that.offset - that.limit;
                $.extend(this.searchCollection.queryParams, {
                    offset: that.offset
                });
                this.previousClick = true;
                this.fetchCollection();
            },
        });
    return SearchResultLayoutView;
});
