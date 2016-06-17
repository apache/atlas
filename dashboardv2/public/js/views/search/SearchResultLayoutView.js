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
    'utils/CommonViewFunction'
], function(require, Backbone, SearchResultLayoutViewTmpl, Modal, VEntity, Utils, Globals, VSearchList, CommonViewFunction) {
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
                showMoreLessTerm: '[data-id="showMoreLessTerm"]'
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
                    $(e.currentTarget).find('i').toggleClass('fa fa-angle-right fa fa-angle-up');
                    $(e.currentTarget).parents('.searchTag').find('a').toggleClass('hide show');
                    if ($(e.currentTarget).find('i').hasClass('fa-angle-right')) {
                        $(e.currentTarget).find('span').text('Show More');
                    } else {
                        $(e.currentTarget).find('span').text('Show less');
                    }
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
                return events;
            },
            /**
             * intialize a new SearchResultLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'globalVent', 'vent', 'value'));
                this.entityModel = new VEntity();
                this.searchCollection = new VSearchList();
                this.fetchList = 0;
                this.commonTableOptions = {
                    collection: this.searchCollection,
                    includeFilter: false,
                    includePagination: true,
                    includePageSize: false,
                    includeFooterRecords: true,
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
                this.listenTo(this.searchCollection, 'backgrid:selected', function(model, checked) {
                    if (checked === true) {
                        model.set("isEnable", true);
                        this.$('.searchResult').find(".inputAssignTag.multiSelect").show();
                    } else {
                        model.set("isEnable", false);
                        this.$('.searchResult').find(".inputAssignTag.multiSelect").hide();
                    }
                    this.arr = [];
                    var that = this;
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
                this.listenTo(this.vent, "show:searchResult", function(value) {
                    this.fetchCollection(value);
                    this.REntityTableLayoutView.reset();
                }, this);
                this.listenTo(this.searchCollection, "reset", function(value) {
                    if (this.searchCollection.toJSON().length == 0) {
                        this.checkTableFetch();
                    }
                    this.renderTableLayoutView();
                    var resultData = this.searchCollection.fullCollection.length + ' result for <b>' + this.searchCollection.queryParams.query + '</b>'
                    var multiAssignData = '<a href="javascript:void(0)" class="inputAssignTag multiSelect" style="display:none" data-id="addTerm"><i class="fa fa-folder-o">' + " " + 'Assign Term</i></a>'
                    this.$('.searchResult').html(resultData + multiAssignData);
                }, this);
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
                //this.renderTableLayoutView();
                var value = {};
                if (this.value) {
                    value = this.value;
                } else {
                    value = {
                        'query': '',
                        'searchType': 'fulltext'

                    };
                }
                this.fetchCollection(value);
            },
            fetchCollection: function(value) {
                this.$('.fontLoader').show();
                this.$('.searchTable').hide();
                this.$('.searchResult').html('');
                if (value) {
                    if (value.searchType) {
                        this.searchCollection.url = "/api/atlas/discovery/search/" + value.searchType;
                    }
                    _.extend(this.searchCollection.queryParams, { 'query': value.query });
                }
                this.searchCollection.fetch({ reset: true });
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
                    this.$('.fontLoader').hide();
                    this.$('.searchTable').show();
                }
            },
            getEntityTableColumns: function() {
                var that = this,
                    col = {};
                var responseData = this.searchCollection.responseData;
                if (this.searchCollection.responseData) {
                    if (responseData.dataType) {
                        if (responseData.dataType.attributeDefinitions.length == 2 && responseData.dataType.attributeDefinitions[1].name == "instanceInfo") {
                            return this.getFixedColumn();
                        } else {
                            col['Check'] = {
                                name: "selected",
                                label: "",
                                cell: "select-row",
                                headerCell: "select-all",
                                position: 1
                            };
                            var modelJSON = this.searchCollection.toJSON()[0];
                            _.keys(modelJSON).map(function(key) {
                                if (key.indexOf("$") == -1 && typeof modelJSON[key] != "object") {
                                    if (typeof modelJSON[key] == "string" || typeof modelJSON[key] == "number") {
                                        if (typeof modelJSON[key] == "number" && key != "createTime") {
                                            return;
                                        }
                                        if (key == "name" || key == "owner" || key == "description") {
                                            col[key] = {
                                                cell: (key == "name") ? ("Html") : ("String"),
                                                editable: false,
                                                sortable: false,
                                                orderable: true,
                                                formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                                                    fromRaw: function(rawValue, model) {
                                                        if (rawValue == null) {
                                                            return null;
                                                        }
                                                        if (model.get('createTime') == rawValue) {
                                                            return new Date(rawValue);
                                                        }
                                                        if (model.get('name') == rawValue) {
                                                            var nameHtml = "";
                                                            if (model.get('$id$')) {
                                                                nameHtml = '<a href="#!/detailPage/' + model.get('$id$').id + '">' + rawValue + '</a>';
                                                            } else {
                                                                nameHtml = '<a>' + rawValue + '</a>';
                                                            }
                                                            if (Globals.entityStateReadOnly[model.get('$id$').state]) {
                                                                nameHtml += '<button title="Deleted" class="btn btn-atlasAction btn-atlas deleteBtn"><i class="fa fa-trash"></i></button>';
                                                                return '<div class="readOnly readOnlyLink">' + nameHtml + '</div>';
                                                            } else {
                                                                return nameHtml;
                                                            }
                                                        } else {
                                                            return rawValue;
                                                        }
                                                    }
                                                })
                                            };
                                        }
                                    }
                                }
                            });
                            col['tag'] = {
                                label: "Tags",
                                cell: "Html",
                                editable: false,
                                sortable: false,
                                orderable: true,
                                className: 'searchTag',
                                formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                                    fromRaw: function(rawValue, model) {
                                        if (Globals.entityStateReadOnly[model.get('$id$').state]) {
                                            return '<div class="readOnly">' + CommonViewFunction.tagForTable(model); + '</div>';
                                        } else {
                                            return CommonViewFunction.tagForTable(model);
                                        }

                                    }
                                })
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
                                        if (Globals.entityStateReadOnly[model.get('$id$').state]) {
                                            return '<div class="readOnly">' + returnObject.html + '</div>';
                                        } else {
                                            return returnObject.html;
                                        }
                                    }
                                })
                            };
                            that.checkTableFetch();
                            return this.searchCollection.constructor.getTableCols(col, this.searchCollection);
                        }
                    } else {
                        return this.getFixedColumn();
                    }
                }
            },
            getFixedColumn: function() {
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
                                    ++that.fetchList;
                                    model.getEntity(guid, {
                                        beforeSend: function() {},
                                        success: function(data) {
                                            if (data.definition) {
                                                if (data.definition.id && data.definition.id.state) {
                                                    if (Globals.entityStateReadOnly[data.definition.id.state]) {
                                                        that.$('td a[data-id="' + guid + '"]').parent().addClass('readOnly readOnlyLink');
                                                        that.$('td a[data-id="' + guid + '"]').parent().append('<button title="Deleted" class="btn btn-atlasAction btn-atlas deleteBtn"><i class="fa fa-trash"></i></button>');
                                                    }
                                                }
                                                if (data.definition.values && data.definition.values.name) {
                                                    return that.$('td a[data-id="' + guid + '"]').html(data.definition.values.name);
                                                } else {
                                                    return that.$('td a[data-id="' + guid + '"]').html(data.definition.id.id);
                                                }
                                            }
                                        },
                                        error: function(error, data, status) {},
                                        complete: function() {
                                            --that.fetchList;
                                            that.checkTableFetch();
                                        }
                                    });
                                    return '<div><a href="#!/detailPage/' + guid + '" data-id="' + guid + '"></a></div>';
                                } else if (!modelObject.$typeName$) {
                                    var guid = model.toJSON().guid;
                                    ++that.fetchList;
                                    model.getEntity(guid, {
                                        beforeSend: function() {},
                                        success: function(data) {
                                            if (data.definition) {
                                                if (data.definition.id && data.definition.id.state) {
                                                    if (Globals.entityStateReadOnly[data.definition.id.state]) {
                                                        that.$('td a[data-id="' + guid + '"]').parent().addClass('readOnly readOnlyLink');
                                                        that.$('td a[data-id="' + guid + '"]').parent().append('<button title="Deleted" class="btn btn-atlasAction btn-atlas deleteBtn"><i class="fa fa-trash"></i></button>');
                                                    }
                                                }
                                                if (data.definition.values && data.definition.values.name) {
                                                    return that.$('td a[data-id="' + guid + '"]').html(data.definition.values.name);
                                                } else {
                                                    return that.$('td a[data-id="' + guid + '"]').html(data.definition.id.id);
                                                }
                                            }
                                        },
                                        error: function(error, data, status) {},
                                        complete: function() {
                                            --that.fetchList;
                                            that.checkTableFetch();
                                        }
                                    });
                                    return '<div><a href="#!/detailPage/' + guid + '" data-id="' + guid + '"></a></div>';
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
                    that = this,
                    modal = CommonViewFunction.deleteTagModel(tagName, "assignTerm");
                modal.on('ok', function() {
                    that.deleteTagData(e);
                });
                modal.on('closeModal', function() {
                    modal.trigger('cancel');
                });
            },
            deleteTagData: function(e) {
                var that = this,
                    tagName = $(e.target).data("name"),
                    guid = $(e.target).data("guid");
                CommonViewFunction.deleteTag({
                    'tagName': tagName,
                    'guid': guid,
                    callback: function() {
                        that.fetchCollection();
                    }
                });
            }
        });
    return SearchResultLayoutView;
});
