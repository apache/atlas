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
                addTerm: '[data-id="addTerm"]'
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
                events["click " + this.ui.addTerm] = 'addTermModalView';
                events["click " + this.ui.tagCrossIcon] = function(e) {};
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

            },
            bindEvents: function() {
                this.listenTo(this.vent, "show:searchResult", function(value) {
                    this.fetchCollection(value);
                    this.REntityTableLayoutView.reset();
                }, this);
                this.listenTo(this.searchCollection, "reset", function(value) {
                    if (this.searchCollection.toJSON().length == 0) {
                        this.checkTableFetch();
                    }
                    this.renderTableLayoutView();
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
                    count = 4;
                require(['utils/TableLayout'], function(TableLayout) {
                    var columnCollection = Backgrid.Columns.extend({
                        sortKey: "position",
                        comparator: function(item) {
                            return item.get(this.sortKey) || 999;
                        },
                        setPositions: function() {
                            _.each(this.models, function(model, index) {
                                if (model.get('name') == "name") {
                                    model.set("position", 1, { silent: true });
                                    model.set("label", "Name");
                                } else if (model.get('name') == "description") {
                                    model.set("position", 2, { silent: true });
                                    model.set("label", "Description");
                                } else if (model.get('name') == "owner") {
                                    model.set("position", 3, { silent: true });
                                    model.set("label", "Owner");
                                } else {
                                    model.set("position", ++count, { silent: true });
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
                                                            if (model.get('$id$')) {
                                                                return '<a href="#!/detailPage/' + model.get('$id$').id + '">' + rawValue + '</a>';
                                                            } else {
                                                                return '<a>' + rawValue + '</a>';
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
                                formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                                    fromRaw: function(rawValue, model) {
                                        var traits = model.get('$traits$');
                                        var atags = "",
                                            addTag = "";
                                        _.keys(model.get('$traits$')).map(function(key) {
                                            var tagName = Utils.checkTagOrTerm(traits[key].$typeName$);
                                            if (!tagName.term) {
                                                atags += '<a class="inputTag" data-id="tagClick">' + traits[key].$typeName$ + '<i class="fa fa-times" data-id="delete" data-name="' + tagName.name + '" data-guid="' + model.get('$id$').id + '" ></i></a>';
                                            }
                                        });
                                        if (model.get('$id$')) {
                                            addTag += '<a href="javascript:void(0)" data-id="addTag" class="inputTag" data-guid="' + model.get('$id$').id + '" ><i style="right:0" class="fa fa-plus"></i></a>';
                                        } else {
                                            addTag += '<a href="javascript:void(0)" data-id="addTag" class="inputTag"><i style="right:0" class="fa fa-plus"></i></a>';

                                        }
                                        return '<div class="tagList">' + atags + addTag + '</div>';
                                    }
                                })
                            };
                            col['terms'] = {
                                label: "Terms",
                                cell: "Html",
                                editable: false,
                                sortable: false,
                                orderable: true,
                                formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                                    fromRaw: function(rawValue, model) {
                                        var traits = model.get('$traits$');
                                        var aterm = "",
                                            addTerm = "";
                                        _.keys(model.get('$traits$')).map(function(key) {
                                            var tagName = Utils.checkTagOrTerm(traits[key].$typeName$);
                                            if (tagName.term) {
                                                aterm += '<a class="inputTag term" data-id="tagClick" data-href="' + traits[key].$typeName$ + '">' + traits[key].$typeName$ + '<i class="fa fa-times" data-id="delete" data-name="' + traits[key].$typeName$ + '" data-guid="' + model.get('$id$').id + '" ></i></a>';
                                            }
                                        });
                                        if (model.get('$id$')) {
                                            addTerm += '<a href="javascript:void(0)" data-id="addTerm" class="inputTag" data-guid="' + model.get('$id$').id + '" ><i style="right:0" class="fa fa-plus"></i></a>';
                                        } else {
                                            addTerm += '<a href="javascript:void(0)" data-id="addTerm" class="inputTag"><i style="right:0" class="fa fa-plus"></i></a>';
                                        }
                                        return '<div class="tagList">' + aterm + addTerm + '</div>';
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
                                            if (data.definition && data.definition.values && data.definition.values.name) {
                                                return that.$('td a[data-id="' + guid + '"]').html(data.definition.values.name);
                                            } else {
                                                return that.$('td a[data-id="' + guid + '"]').html(data.definition.id.id);
                                            }
                                        },
                                        error: function(error, data, status) {},
                                        complete: function() {
                                            --that.fetchList;
                                            that.checkTableFetch();
                                        }
                                    });
                                    return '<a href="#!/detailPage/' + guid + '" data-id="' + guid + '"></a>';
                                } else if (!modelObject.$typeName$) {
                                    var guid = model.toJSON().guid;
                                    ++that.fetchList;
                                    model.getEntity(guid, {
                                        beforeSend: function() {},
                                        success: function(data) {
                                            if (data.definition && data.definition.values && data.definition.values.name) {
                                                return that.$('td a[data-id="' + guid + '"]').html(data.definition.values.name);
                                            } else {
                                                return that.$('td a[data-id="' + guid + '"]').html(data.definition.id.id);
                                            }
                                        },
                                        error: function(error, data, status) {},
                                        complete: function() {
                                            --that.fetchList;
                                            that.checkTableFetch();
                                        }
                                    });
                                    return '<a href="#!/detailPage/' + guid + '" data-id="' + guid + '"></a>';
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
            addTermModalView: function(e) {
                var that = this;
                require([
                    'views/business_catalog/AddTermToEntityLayoutView',
                ], function(AddTermToEntityLayoutView) {
                    var view = new AddTermToEntityLayoutView({
                        guid: that.$(e.currentTarget).data("guid"),
                        callback: function() {
                            that.fetchCollection();
                        }
                    });
                });

            },
            onClickTagCross: function(e) {
                var tagName = $(e.target).data("name"),
                    guid = $(e.target).data("guid"),
                    that = this,
                    modal = CommonViewFunction.deleteTagModel(tagName);
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
