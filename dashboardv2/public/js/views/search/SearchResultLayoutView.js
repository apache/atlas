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
                nextData: "[data-id='nextData']"
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
                events["click " + this.ui.addTag] = 'addTagModalView';
                events["click " + this.ui.addTerm] = 'checkedValue';
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
                this.fetchList = 0;
                if (options.value.searchType == "dsl") {
                    pagination = false;
                } else {
                    pagination = true;
                }
                this.commonTableOptions = {
                    collection: this.searchCollection,
                    includeFilter: false,
                    includePagination: pagination,
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
                    if (checked === true) {
                        model.set("isEnable", true);
                        this.$('.searchResult').find(".inputAssignTag.multiSelect").show();
                    } else {
                        model.set("isEnable", false);
                        this.$('.searchResult').find(".inputAssignTag.multiSelect").hide();
                    }
                    this.arr = [];
                    this.searchCollection.find(function(item) {
                        if (item.get('isEnable')) {
                            var term = [];
                            that.arr.push({
                                id: item.get("$id$"),
                                model: item
                            });
                        }
                    });
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
                this.$('.fontLoader').show();
                this.$('.searchTable').hide();
                that.$('.searchResult').hide();
                if (Globals.searchApiCallRef) {
                    Globals.searchApiCallRef.abort();
                }
                $.extend(this.searchCollection.queryParams, { limit: this.limit });
                if (value) {
                    if (value.searchType) {
                        this.searchCollection.url = "/api/atlas/discovery/search/" + value.searchType;
                        if (value.searchType === "dsl") {
                            $.extend(this.searchCollection.queryParams, { limit: this.limit });
                            this.offset = 0;
                        }
                    }
                    _.extend(this.searchCollection.queryParams, { 'query': value.query });
                }
                Globals.searchApiCallRef = this.searchCollection.fetch({
                    success: function() {
                        if (that.searchCollection.length === 0) {
                            that.ui.nextData.attr('disabled', true);
                        }
                        Globals.searchApiCallRef = undefined;
                        if (that.searchCollection.toJSON().length == 0) {
                            that.checkTableFetch();
                        }
                        that.renderTableLayoutView();
                        var resultData = that.searchCollection.fullCollection.length + ' results for <b>' + that.searchCollection.queryParams.query + '</b>'
                        var multiAssignData = '<a href="javascript:void(0)" class="inputAssignTag multiSelect" style="display:none" data-id="addTerm"><i class="fa fa-folder-o"></i>' + " " + 'Assign Term</a>'
                        that.$('.searchResult').html(resultData + multiAssignData);
                    },
                    silent: true
                });
            },
            renderTableLayoutView: function() {
                var that = this,
                    count = 5;
                require(['utils/TableLayout'], function(TableLayout) {
                    var columnCollection = Backgrid.Columns.extend({
                        sortKey: "position",
                        comparator: function(item) {
                            return item.get(this.sortKey) || 999;
                        },
                        setPositions: function() {
                            _.each(this.models, function(model, index) {
                                if (model.get('name') == "name") {
                                    model.set("position", 2, { silent: true });
                                    model.set("label", "Name");
                                } else if (model.get('name') == "description") {
                                    model.set("position", 3, { silent: true });
                                    model.set("label", "Description");
                                } else if (model.get('name') == "owner") {
                                    model.set("position", 4, { silent: true });
                                    model.set("label", "Owner");
                                }
                            });
                            return this;
                        }
                    });
                    var columns = new columnCollection(that.getEntityTableColumns());
                    columns.setPositions().sort();
                    that.REntityTableLayoutView.show(new TableLayout(_.extend({}, that.commonTableOptions, {
                        globalVent: that.globalVent,
                        columns: columns,
                        includeOrderAbleColumns: true
                    })));
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
                    if (this.value.searchType == "dsl") {
                        this.ui.paginationDiv.show();
                    } else {
                        this.ui.paginationDiv.hide();
                    }
                }
            },
            getEntityTableColumns: function() {
                var that = this,
                    col = {};
                var responseData = this.searchCollection.responseData;
                if (this.searchCollection.responseData) {
                    if (responseData.dataType) {
                        if (responseData.dataType.attributeDefinitions.length == 2 && responseData.dataType.attributeDefinitions[1].name == "instanceInfo") {
                            return this.getFixedFullTextColumn();
                        } else {
                            if (responseData.dataType.typeName.indexOf('_temp') == -1) {
                                return this.getFixedDslColumn();
                            } else {
                                var idFound = false;
                                _.each(this.searchCollection.models, function(model) {
                                    var modelJSON = model.toJSON();
                                    var guid = "";
                                    _.each(modelJSON, function(val, key) {
                                        if (_.isObject(val) && val.id) {
                                            model.set('id', val.id);
                                            guid = val.id;
                                        } else if (key === "id") {
                                            model.set('id', val);
                                            guid = val;
                                        }
                                    });
                                    if (guid.length) {
                                        idFound = true;
                                        model.getEntity(guid, {
                                            async: false,
                                            success: function(data) {
                                                if (data.definition) {
                                                    if (data.definition.id && data.definition.values) {
                                                        that.searchCollection.get(data.definition.id).set(data.definition.values);
                                                        that.searchCollection.get(data.definition.id).set('$id$', data.definition.id);
                                                        that.searchCollection.get(data.definition.id).set('$traits$', data.definition.traits);
                                                    }
                                                }
                                            },
                                            error: function(error, data, status) {},
                                            complete: function() {}
                                        });
                                    }
                                });
                                if (idFound) {
                                    return this.getFixedDslColumn();
                                } else {
                                    return this.getDaynamicColumn();
                                }
                            }
                        }
                    } else {
                        return this.getFixedFullTextColumn();
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
                    col = {};
                col['name'] = {
                    label: "Name",
                    cell: "html",
                    editable: false,
                    sortable: false,
                    formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                        fromRaw: function(rawValue, model) {
                            var nameHtml = "";
                            if (model.get('$id$')) {
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
                    col['Check'] = {
                        name: "selected",
                        label: "",
                        cell: "select-row",
                        headerCell: "select-all",
                        position: 1
                    };
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
            getFixedFullTextColumn: function() {
                var that = this;
                return this.searchCollection.constructor.getTableCols({
                    instanceInfo: {
                        label: "Type Name",
                        cell: "html",
                        editable: false,
                        sortable: false,
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                            fromRaw: function(rawValue, model) {
                                var modelObject = model.toJSON();
                                if (modelObject.$typeName$ && modelObject.instanceInfo) {
                                    return '<a href="#!/detailPage/' + modelObject.instanceInfo.guid + '">' + modelObject.instanceInfo.typeName + '</a>';
                                } else if (!modelObject.$typeName$) {
                                    return '<a href="#!/detailPage/' + modelObject.guid + '">' + modelObject.typeName + '</a>';
                                }
                            }
                        })
                    },
                    name: {
                        label: "Name",
                        cell: "html",
                        editable: false,
                        sortable: false,
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                            fromRaw: function(rawValue, model) {
                                var modelObject = model.toJSON();
                                if (modelObject.$typeName$ && modelObject.instanceInfo) {
                                    var guid = model.toJSON().instanceInfo.guid;
                                    var json = model.toJSON();
                                    json['id'] = guid;
                                    return CommonViewFunction.propertyTable({ 'notUsedKey': json }, that, true);
                                } else if (!modelObject.$typeName$) {
                                    var guid = model.toJSON().guid;
                                    var json = model.toJSON();
                                    json['id'] = guid;
                                    return CommonViewFunction.propertyTable({ 'notUsedKey': json }, that, true);
                                }
                            }
                        })
                    }
                }, this.searchCollection);
            },
            addTagModalView: function(e) {
                var that = this;
                require(['views/tag/addTagModalView'], function(AddTagModalView) {
                    var view = new AddTagModalView({
                        guid: that.$(e.currentTarget).data("guid"),
                        callback: function() {
                            that.fetchCollection();
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
                if (this.arr && this.arr.length) {
                    that.addTermModalView(guid, this.arr);
                } else {
                    guid = that.$(e.currentTarget).data("guid");
                    that.addTermModalView(guid);
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
                        callback: function(termName) {
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
                $.extend(this.searchCollection.queryParams, {
                    offset: function() {
                        that.offset = that.offset + that.limit;
                        return that.offset;
                    }
                });
                if (this.offset > this.limit) {
                    this.ui.nextData.attr('disabled', true);
                }
                this.fetchCollection();
            },
            onClickpreviousData: function() {
                var that = this;
                this.ui.nextData.removeAttr("disabled");
                $.extend(this.searchCollection.queryParams, {
                    offset: function() {
                        that.offset = that.offset - that.limit;
                        return that.offset;
                    }
                });
                if (this.offset <= that.limit) {
                    this.ui.previousData.attr('disabled', true);
                }
                this.fetchCollection();
            },
        });
    return SearchResultLayoutView;
});
