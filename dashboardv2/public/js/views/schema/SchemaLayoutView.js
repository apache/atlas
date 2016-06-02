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
                addTerm: '[data-id="addTerm"]'

            },
            /** ui events hash */
            events: function() {
                var events = {};

                events["click " + this.ui.addTag] = 'addTagModalView';
                events["click " + this.ui.addTerm] = 'addTermModalView';
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
                    includePagination: true,
                    includePageSize: false,
                    includeFooterRecords: true,
                    gridOpts: {
                        className: "table table-hover backgrid table-quickMenu",
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
                    this.$('.fontLoader').hide();
                }, this);
                this.listenTo(this.schemaCollection, "error", function(value) {
                    $('.schemaTable').hide();
                    this.$('.fontLoader').hide();
                }, this);
            },
            onRender: function() {
                this.schemaCollection.fetch({ reset: true });
                this.renderTableLayoutView();
            },
            fetchCollection: function() {
                this.$('.fontLoader').show();
                this.schemaCollection.fetch({ reset: true });
            },
            renderTableLayoutView: function() {
                var that = this;
                require(['utils/TableLayout'], function(TableLayout) {
                    var cols = new Backgrid.Columns(that.getSchemaTableColumns());
                    that.RTagLayoutView.show(new TableLayout(_.extend({}, that.commonTableOptions, {
                        globalVent: that.globalVent,
                        columns: cols
                    })));
                });
            },
            getSchemaTableColumns: function() {
                var that = this;
                var col = {};
                if (this.schemaCollection.keyList) {
                    _.each(this.schemaCollection.keyList, function(obj, key) {
                        col[obj.name] = {
                            cell: "Html",
                            editable: false,
                            sortable: false,
                            orderable: true,
                            formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                                fromRaw: function(rawValue, model) {
                                    if (model) {
                                        if (!_.isArray(rawValue) && _.isObject(rawValue)) {
                                            if (rawValue.id) {
                                                return '<div><a href="#!/detailPage/' + rawValue.id + '">' + rawValue.$typeName$ + '</a></div>';
                                            } else {
                                                return rawValue.$typeName$;
                                            }
                                        } else if (_.isArray(rawValue)) {
                                            var links = "";
                                            _.each(rawValue, function(val, key) {
                                                if (val.id) {
                                                    links += '<div><a href="#!/detailPage/' + val.id + '">' + val.$typeName$ + '</a></div>';
                                                } else {
                                                    links += '<div>' + val.$typeName$ + '</div>';
                                                }
                                            });
                                            return links;

                                        } else if (model.get('$id$') && model.get('$id$').id && model.get('name') == rawValue) {
                                            return '<div><a href="#!/detailPage/' + model.get('$id$').id + '">' + rawValue + '</a></div>';
                                        } else {
                                            return rawValue;
                                        }
                                    } else {
                                        return rawValue;
                                    }

                                }
                            })
                        };
                    });
                    col['tag'] = {
                        label: "Tags",
                        cell: "Html",
                        editable: false,
                        sortable: false,
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                            fromRaw: function(rawValue, model) {
                                var traits = model.get('$traits$');
                                var atags = "";
                                _.keys(model.get('$traits$')).map(function(key) {
                                    var tagName = Utils.checkTagOrTerm(traits[key].$typeName$);
                                    if (!tagName.term) {
                                        atags += '<a class="inputTag" data-id="tagClick">' + traits[key].$typeName$ + '<i class="fa fa-times" data-id="delete" data-name="' + traits[key].$typeName$ + '" data-guid="' + model.get('$id$').id + '" ></i></a>';
                                    }
                                });
                                return '<div class="tagList">' + atags + '<a href="javascript:void(0);" class="inputTag" data-id="addTag" data-guid="' + model.get('$id$').id + '"><i style="right:0" class="fa fa-plus"></i></a></div>';
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
                                var aterms = "";
                                _.keys(model.get('$traits$')).map(function(key) {
                                    var tagName = Utils.checkTagOrTerm(traits[key].$typeName$);
                                    if (tagName.term) {
                                        aterms += '<a class="inputTag" data-id="tagClick">' + traits[key].$typeName$ + '<i class="fa fa-times" data-id="delete" data-name="' + traits[key].$typeName$ + '" data-guid="' + model.get('$id$').id + '" ></i></a>';
                                    }
                                });
                                return '<div class="tagList">' + aterms + '<a href="javascript:void(0);" class="inputTag" data-id="addTerm" data-guid="' + model.get('$id$').id + '"><i style="right:0" class="fa fa-plus"></i></a></div>';
                            }
                        })
                    };
                }


                return this.schemaCollection.constructor.getTableCols(col, this.schemaCollection);
            },
            addTagModalView: function(e) {
                if (e) {
                    e.stopPropagation();
                }
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
                if (e) {
                    e.stopPropagation();
                }
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
    return SchemaTableLayoutView;
});
