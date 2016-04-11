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
    'hbs!tmpl/detail_page/DetailPageLayoutView_tmpl'
], function(require, Backbone, DetailPageLayoutViewTmpl) {
    'use strict';

    var DetailPageLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends DetailPageLayoutView */
        {
            _viewName: 'DetailPageLayoutView',

            template: DetailPageLayoutViewTmpl,

            /** Layout sub regions */
            regions: {
                REntityDetailTableLayoutView: "#r_entityDetailTableLayoutView",
                RSchemaTableLayoutView: "#r_schemaTableLayoutView",
                RTagTableLayoutView: "#r_tagTableLayoutView",
                RLineageLayoutView: "#r_lineageLayoutView",
            },
            /** ui selector cache */
            ui: {},
            /** ui events hash */
            events: function() {
                var events = {};
                return events;
            },
            /**
             * intialize a new DetailPageLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'globalVent', 'collection', 'vent'));
                this.bindEvents();
                this.commonTableOptions = {
                    collection: this.collection,
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
            },
            bindEvents: function() {
                this.listenTo(this.collection, 'reset', function() {
                    var collectionJSON = this.collection.toJSON();
                    if (collectionJSON[0].id && collectionJSON[0].id.id) {
                        var tagGuid = collectionJSON[0].id.id;
                    }
                    if (collectionJSON && collectionJSON.length) {
                        if (collectionJSON[0].values) {
                            this.name = collectionJSON[0].values.name;
                            this.description = collectionJSON[0].values.description;
                            if (this.name) {
                                this.$('.breadcrumbName').text(this.name);
                                this.$('.name').show();
                                this.$('.name').html('<strong>Name: </strong><span>' + this.name + '</span>');
                            } else {
                                this.$('.name').hide();
                            }
                            if (this.description) {
                                this.$('.description').show();
                                this.$('.description').html('<strong>Description: </strong><span>' + this.description + '</span>');
                            } else {
                                this.$('.description').hide();
                            }
                        }
                    }
                    this.renderEntityDetailTableLayoutView();
                    this.renderTagTableLayoutView(tagGuid);
                    this.renderLineageLayoutView(tagGuid);
                    this.renderSchemaLayoutView();
                }, this);
            },
            onRender: function() {},
            renderEntityDetailTableLayoutView: function() {
                var that = this;
                require(['views/entity/EntityDetailTableLayoutView'], function(EntityDetailTableLayoutView) {
                    that.REntityDetailTableLayoutView.show(new EntityDetailTableLayoutView({
                        globalVent: that.globalVent,
                        collection: that.collection
                    }));
                });
            },
            renderTagTableLayoutView: function(tagGuid) {
                var that = this;
                require(['views/tag/TagDetailTableLayoutView'], function(TagDetailTableLayoutView) {
                    that.RTagTableLayoutView.show(new TagDetailTableLayoutView({
                        globalVent: that.globalVent,
                        collection: that.collection,
                        guid: tagGuid
                    }));
                });
            },
            renderLineageLayoutView: function(tagGuid) {
                var that = this;
                require(['views/graph/LineageLayoutView'], function(LineageLayoutView) {
                    that.RLineageLayoutView.show(new LineageLayoutView({
                        globalVent: that.globalVent,
                        assetName: that.name,
                        guid: tagGuid
                    }));
                });
            },
            renderSchemaLayoutView: function() {
                var that = this;
                require(['views/schema/SchemaLayoutView'], function(SchemaLayoutView) {
                    that.RSchemaTableLayoutView.show(new SchemaLayoutView({
                        globalVent: that.globalVent,
                        name: that.name,
                        vent: that.vent
                    }));
                });
            },
            getTagTableColumns: function() {
                var that = this;
                return this.collection.constructor.getTableCols({
                    tag: {
                        label: "Tag",
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
                    attributes: {
                        label: "Attributes",
                        cell: "html",
                        editable: false,
                        sortable: false,
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                            fromRaw: function(rawValue, model) {
                                var modelObject = model.toJSON();
                                if (modelObject.$typeName$ && modelObject.instanceInfo) {
                                    var guid = model.toJSON().instanceInfo.guid;
                                    model.getEntity(guid, {
                                        beforeSend: function() {},
                                        success: function(data) {
                                            return that.$('td a[data-id="' + guid + '"]').html(data.definition.values.name);
                                        },
                                        error: function(error, data, status) {},
                                        complete: function() {}
                                    });
                                    return '<a href="#!/dashboard/detailPage/' + guid + '" data-id="' + guid + '"></a>';
                                } else if (!modelObject.$typeName$) {
                                    var guid = model.toJSON().guid;
                                    model.getEntity(guid, {
                                        beforeSend: function() {},
                                        success: function(data) {
                                            return that.$('td a[data-id="' + guid + '"]').html(data.definition.values.name);
                                        },
                                        error: function(error, data, status) {},
                                        complete: function() {}
                                    });
                                    return '<a href="#!/dashboard/detailPage/' + guid + '" data-id="' + guid + '"></a>';
                                }
                            }
                        })
                    }
                }, this.collection);
            }
        });
    return DetailPageLayoutView;
});
