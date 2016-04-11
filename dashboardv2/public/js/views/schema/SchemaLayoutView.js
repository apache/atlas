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
    'hbs!tmpl/schema/SchemaTableLayoutView_tmpl',
    'collection/VSchemaList',
    'utils/Utils',
], function(require, Backbone, SchemaTableLayoutViewTmpl, VSchemaList, Utils) {
    'use strict';

    var SchemaTableLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends SchemaTableLayoutView */
        {
            _viewName: 'SchemaTableLayoutView',

            template: SchemaTableLayoutViewTmpl,

            /** Layout sub regions */
            regions: {
                RTagLayoutView: "#r_tagLayoutView",
            },
            /** ui selector cache */
            ui: {
                tagClick: '[data-id="tagClick"]',
                DetailValue: "[data-id='detailValue']",
                addTag: "[data-id='addTag']",
            },
            /** ui events hash */
            events: function() {
                var events = {};

                events["click " + this.ui.addTag] = function(e) {
                        this.onClickSchemaTag(e);
                    },
                    events["click " + this.ui.tagClick] = function(e) {
                        if (e.target.nodeName.toLocaleLowerCase() == "i") {
                            this.onClickTagCross(e);
                        } else {
                            var value = e.currentTarget.text;
                            Utils.setUrl({
                                url: '#!/dashboard/assetPage',
                                urlParams: {
                                    query: value
                                },
                                mergeBrowserUrl: false,
                                trigger: true
                            });
                        }
                    };
                return events;
            },
            /**
             * intialize a new SchemaTableLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'globalVent', 'name', 'vent'));
                this.schemaCollection = new VSchemaList([], {});
                this.schemaCollection.url = "/api/atlas/lineage/hive/table/" + this.name + "/schema";
                this.commonTableOptions = {
                    collection: this.schemaCollection,
                    includeFilter: false,
                    includePagination: false,
                    includePageSize: false,
                    includeFooterRecords: true,
                    gridOpts: {
                        className: "table table-striped table-condensed backgrid table-quickMenu",
                        emptyText: 'No records found!'
                    },
                    filterOpts: {},
                    paginatorOpts: {}
                };
                this.bindEvents();
            },
            bindEvents: function() {
                this.listenTo(this.schemaCollection, "reset", function(value) {
                    this.renderTableLayoutView();
                    $('.schemaTable').show();
                }, this);
                this.listenTo(this.schemaCollection, "error", function(value) {
                    $('.schemaTable').hide();
                }, this);
            },
            onRender: function() {
                this.schemaCollection.fetch({ reset: true });
                this.renderTableLayoutView();
            },
            renderTableLayoutView: function() {
                var that = this;
                require(['utils/TableLayout'], function(TableLayout) {
                    var cols = new Backgrid.Columns(that.getSchemaTableColumns());
                    that.RTagLayoutView.show(new TableLayout(_.extend({}, that.commonTableOptions, {
                        globalVent: that.globalVent,
                        columns: cols,
                        gridOpts: {
                            className: "table table-bordered table-hover table-condensed backgrid table-quickMenu",
                        },
                    })));
                });
            },
            getSchemaTableColumns: function() {
                var that = this;
                return this.schemaCollection.constructor.getTableCols({
                    name: {
                        label: "Name",
                        cell: "Html",
                        editable: false,
                        sortable: false,
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                            fromRaw: function(rawValue, model) {
                                return '<div><a href="#!/dashboard/detailPage/' + model.get('$id$').id + '">' + rawValue + '</a></div>';
                            }
                        })
                    },
                    comment: {
                        label: "Comment",
                        cell: "html",
                        editable: false,
                        sortable: false
                    },
                    dataType: {
                        label: "DataType",
                        cell: "html",
                        editable: false,
                        sortable: false
                    },
                    tag: {
                        label: "Tags",
                        cell: "Html",
                        editable: false,
                        sortable: false,
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
                    },
                    addTag: {
                        label: "Tools",
                        cell: "Html",
                        editable: false,
                        sortable: false,
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                            fromRaw: function(rawValue, model) {
                                return '<a href="javascript:void(0);" data-id="addTag" data-guid="' + model.get('$id$').id + '"><i class="fa fa-tag"></i></a>';
                            }
                        })
                    }
                }, this.schemaCollection);
            },
            onClickSchemaTag: function(e) {
                var that = this;
                require(['views/tag/addTagModalView'], function(addTagModalView) {
                    var view = new addTagModalView({
                        vent: that.vent,
                        guid: that.$(e.currentTarget).data("guid"),
                        modalCollection: that.schemaCollection
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
                            that.schemaCollection.fetch({ reset: true });
                        },
                        error: function(error, data, status) {},
                        complete: function() {}
                    });
                });
            }
        });
    return SchemaTableLayoutView;
});
