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
    'utils/CommonViewFunction'
], function(require, Backbone, SchemaTableLayoutViewTmpl, VSchemaList, Utils, CommonViewFunction) {
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
                                url: '#!/tag/tagAttribute/' + value,
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
                _.extend(this, _.pick(options, 'globalVent', 'guid', 'vent'));
                this.schemaCollection = new VSchemaList([], {});
                this.schemaCollection.url = "/api/atlas/lineage/" + this.guid + "/schema";
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
                                return '<div><a href="#!/detailPage/' + model.get('$id$').id + '">' + rawValue + '</a></div>';
                            }
                        })
                    },
                    comment: {
                        label: "Comment",
                        cell: "html",
                        editable: false,
                        sortable: false
                    },
                    type: {
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
                                    atags += '<a class="inputTag" data-id="tagClick">' + traits[key].$typeName$ + '<i class="fa fa-times" data-id="delete" data-name="' + traits[key].$typeName$ + '" data-guid="' + model.get('$id$').id + '" ></i></a>';
                                });
                                return '<div class="tagList">' + atags + '<a href="javascript:void(0);" class="inputTag" data-id="addTag" data-guid="' + model.get('$id$').id + '"><i style="right:0" class="fa fa-plus"></i></a></div>';
                            }
                        })
                    }
                }, this.schemaCollection);
            },
            onClickSchemaTag: function(e) {
                var that = this;
                require(['views/tag/addTagModalView'], function(AddTagModalView) {
                    var view = new AddTagModalView({
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
                var tagName = $(e.target).data("name"),
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
                    'collection': that.tagCollection
                });
            }
        });
    return SchemaTableLayoutView;
});
