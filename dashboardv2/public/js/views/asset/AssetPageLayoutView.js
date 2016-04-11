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
    'hbs!tmpl/asset/AssetPageLayoutView_tmpl',
    'modules/Modal',
    'models/VEntity',
    'utils/Utils',
    'utils/Globals',
], function(require, Backbone, AssetPageLayoutViewTmpl, Modal, VEntity, Utils, Globals) {
    'use strict';

    var AssetPageLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends AssetPageLayoutView */
        {
            _viewName: 'AssetPageLayoutView',

            template: AssetPageLayoutViewTmpl,

            /** Layout sub regions */
            regions: {
                RTagLayoutView: "#r_tagLayoutView",
                RSearchLayoutView: "#r_searchLayoutView",
                REntityTableLayoutView: "#r_entityTableLayoutView",
            },

            /** ui selector cache */
            ui: {
                tagClick: '[data-id="tagClick"]',
                addTag: '[data-id="addTag"]',
            },

            /** ui events hash */
            events: function() {
                var events = {};
                events["click " + this.ui.tagClick] = function(e) {
                    if (e.target.nodeName.toLocaleLowerCase() == "i") {
                        this.onClickTagCross(e);
                    } else {
                        Utils.setUrl({
                            url: '#!/dashboard/assetPage',
                            urlParams: {
                                query: e.currentTarget.text
                            },
                            mergeBrowserUrl: true,
                            trigger: false
                        });
                        this.vent.trigger("tag:click", { 'query': e.currentTarget.text });
                    }
                };
                events["click " + this.ui.addTag] = function(e) {
                    this.addModalView(e);
                };
                events["click " + this.ui.tagCrossIcon] = function(e) {};
                return events;
            },
            /**
             * intialize a new AssetPageLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'globalVent', 'collection', 'vent'));
                this.entityModel = new VEntity();
                this.searchCollection = this.collection;
                this.fetchList = 0
                this.commonTableOptions = {
                    collection: this.searchCollection,
                    includeFilter: false,
                    includePagination: true,
                    includePageSize: false,
                    includeFooterRecords: true,
                    includeSizeAbleColumns: false,
                    gridOpts: {
                        emptyText: 'No Record found!',
                        className: 'table table-bordered table-hover table-condensed backgrid table-quickMenu'
                    },
                    filterOpts: {},
                    paginatorOpts: {}
                };

            },
            bindEvents: function() {
                this.listenTo(this.vent, "search:click", function(value) {
                    this.fetchCollection(value);
                    this.REntityTableLayoutView.reset();
                }, this);
                this.listenTo(this.searchCollection, "reset", function(value) {
                    this.renderTableLayoutView();
                }, this);
                this.listenTo(this.searchCollection, "error", function(value) {
                    this.$('.fontLoader').hide();
                    this.$('.entityTable').show();
                    Utils.notifyError({
                        content: "Invalid expression"
                    });
                }, this);
            },
            onRender: function() {
                this.renderTagLayoutView();
                this.renderSearchLayoutView();
                this.renderTableLayoutView();
                this.bindEvents();
            },
            fetchCollection: function(value) {
                this.$('.fontLoader').show();
                this.$('.entityTable').hide();
                if (value) {
                    if (value.type) {
                        this.searchCollection.url = "/api/atlas/discovery/search/" + value.type;
                    }
                    $.extend(this.searchCollection.queryParams, { 'query': value.query });
                }
                this.searchCollection.fetch({ reset: true });
            },
            renderTagLayoutView: function() {
                var that = this;
                require(['views/tag/TagLayoutView'], function(TagLayoutView) {
                    that.RTagLayoutView.show(new TagLayoutView({
                        globalVent: that.globalVent,
                        vent: that.vent
                    }));
                });
            },
            renderSearchLayoutView: function() {
                var that = this;
                require(['views/search/SearchLayoutView'], function(SearchLayoutView) {
                    that.RSearchLayoutView.show(new SearchLayoutView({
                        globalVent: that.globalVent,
                        vent: that.vent
                    }));
                    var hashUrl = window.location.hash.split("?");
                    if (hashUrl.length > 1) {
                        var param = Utils.getQueryParams(hashUrl[1]);
                        if (param) {
                            var type = param.searchType;
                            var query = param.query;
                            that.vent.trigger("tag:click", { 'query': query, 'searchType': type });
                        }
                    }
                });
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
                                } else if (model.get('name') == "description") {
                                    model.set("position", 2, { silent: true });
                                } else if (model.get('name') == "owner") {
                                    model.set("position", 3, { silent: true });
                                } else if (model.get('name') == "createTime") {
                                    model.set("position", 4, { silent: true });
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
                    if (that.fetchList <= 0) {
                        that.$('.fontLoader').hide();
                        that.$('.entityTable').show();
                    }
                });

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
                                        col[key] = {
                                            cell: (key == "name") ? ("Html") : ("String"),
                                            editable: false,
                                            sortable: false,
                                            orderable: true,
                                            formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                                                fromRaw: function(rawValue, model) {
                                                    if (model.get('createTime') == rawValue) {
                                                        return new Date(rawValue);
                                                    }
                                                    if (model.get('name') == rawValue) {
                                                        if (model.get('$id$')) {
                                                            return '<a href="#!/dashboard/detailPage/' + model.get('$id$').id + '">' + rawValue + '</a>';
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
                                        var atags = "";
                                        _.keys(model.get('$traits$')).map(function(key) {
                                            atags += '<a data-id="tagClick">' + traits[key].$typeName$ + '<i class="fa fa-times" data-id="delete" data-name="' + traits[key].$typeName$ + '" data-guid="' + model.get('$id$').id + '" ></i></a>';
                                        });
                                        return '<div class="tagList">' + atags + '</div>';
                                    }
                                })
                            };
                            col['addTag'] = {
                                label: "Tools",
                                cell: "Html",
                                editable: false,
                                sortable: false,
                                orderable: true,
                                formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                                    fromRaw: function(rawValue, model) {
                                        if (model.get('$id$')) {
                                            return '<a href="javascript:void(0)" data-id="addTag" class="addTagGuid" data-guid="' + model.get('$id$').id + '" ><i class="fa fa-tag"></i></a>';
                                        } else {
                                            return '<a href="javascript:void(0)" data-id="addTag"><i class="fa fa-tag"></i></a>';

                                        }
                                    }
                                })
                            };
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
                                    return '<a href="#!/dashboard/detailPage/' + modelObject.instanceInfo.guid + '">' + modelObject.instanceInfo.typeName + '</a>';
                                } else if (!modelObject.$typeName$) {
                                    return '<a href="#!/dashboard/detailPage/' + modelObject.guid + '">' + modelObject.typeName + '</a>';
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
                                    ++that.fetchList
                                    model.getEntity(guid, {
                                        beforeSend: function() {},
                                        success: function(data) {
                                            --that.fetchList
                                            if (that.fetchList <= 0) {
                                                that.$('.fontLoader').hide();
                                                that.$('.entityTable').show();

                                            }
                                            if (data.definition && data.definition.values && data.definition.values.name) {
                                                return that.$('td a[data-id="' + guid + '"]').html(data.definition.values.name);
                                            } else {
                                                return that.$('td a[data-id="' + guid + '"]').html(data.definition.id.id);
                                            }
                                        },
                                        error: function(error, data, status) {
                                            that.$('.fontLoader').hide();
                                            that.$('.entityTable').show();
                                        },
                                        complete: function() {}
                                    });
                                    return '<a href="#!/dashboard/detailPage/' + guid + '" data-id="' + guid + '"></a>';
                                } else if (!modelObject.$typeName$) {
                                    var guid = model.toJSON().guid;
                                    ++that.fetchList
                                    model.getEntity(guid, {
                                        beforeSend: function() {},
                                        success: function(data) {
                                            --that.fetchList
                                            if (that.fetchList <= 0) {
                                                that.$('.fontLoader').hide();
                                                that.$('.entityTable').show();
                                            }
                                            if (data.definition && data.definition.values && data.definition.values.name) {
                                                return that.$('td a[data-id="' + guid + '"]').html(data.definition.values.name);
                                            } else {
                                                return that.$('td a[data-id="' + guid + '"]').html(data.definition.id.id);
                                            }
                                        },
                                        error: function(error, data, status) {
                                            that.$('.fontLoader').hide();
                                            that.$('.entityTable').show();
                                        },
                                        complete: function() {}
                                    });
                                    return '<a href="#!/dashboard/detailPage/' + guid + '" data-id="' + guid + '"></a>';
                                }
                            }
                        })
                    }
                }, this.searchCollection);
            },
            addModalView: function(e) {
                var that = this;
                require(['views/tag/addTagModalView'], function(addTagModalView) {
                    var view = new addTagModalView({
                        vent: that.vent,
                        guid: that.$(e.currentTarget).data("guid"),
                        modalCollection: that.searchCollection
                    });
                    // view.saveTagData = function() {
                    //override saveTagData function 
                    // }
                });
            },
            onClickTagCross: function(e) {
                var tagName = $(e.target).data("name");
                var that = this;
                require([
                    'modules/Modal'
                ], function(Modal) {
                    var modal = new Modal({
                        title: 'Are you sure you want to delete ?',
                        okText: 'Delete',
                        htmlContent: "<b>Tag: " + tagName + "</b>",
                        cancelText: "Cancel",
                        allowCancel: true,
                        okCloses: true,
                        showFooter: true,
                    }).open();
                    modal.on('ok', function() {
                        that.deleteTagData(e);
                    });
                    modal.on('closeModal', function() {
                        modal.trigger('cancel');
                    });
                });
            },
            deleteTagData: function(e) {
                var that = this,
                    tagName = $(e.target).data("name");
                var guid = $(e.target).data("guid");
                require(['models/VTag'], function(VTag) {
                    var tagModel = new VTag();
                    tagModel.deleteTag(guid, tagName, {
                        beforeSend: function() {},
                        success: function(data) {
                            that.searchCollection.fetch({ reset: true });
                        },
                        error: function(error, data, status) {},
                        complete: function() {}
                    });
                });
            }
        });
    return AssetPageLayoutView;
});
