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
    'hbs!tmpl/audit/AuditTableLayoutView_tmpl',
    'collection/VEntityList',
    'utils/Enums',
    'utils/UrlLinks'
], function(require, Backbone, AuditTableLayoutView_tmpl, VEntityList, Enums, UrlLinks) {
    'use strict';

    var AuditTableLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends AuditTableLayoutView */
        {
            _viewName: 'AuditTableLayoutView',

            template: AuditTableLayoutView_tmpl,

            /** Layout sub regions */
            regions: {
                RAuditTableLayoutView: "#r_auditTableLayoutView",
            },

            /** ui selector cache */
            ui: {
                previousAuditData: "[data-id='previousAuditData']",
                nextAuditData: "[data-id='nextAuditData']",
                pageRecordText: "[data-id='pageRecordText']",
                activePage: "[data-id='activePage']"
            },
            /** ui events hash */
            events: function() {
                var events = {};
                events["click " + this.ui.nextAuditData] = "onClickNextAuditData";
                events["click " + this.ui.previousAuditData] = "onClickPreviousAuditData";
                return events;
            },
            /**
             * intialize a new AuditTableLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'guid', 'entity', 'entityName', 'attributeDefs'));
                this.entityCollection = new VEntityList();
                this.limit = 26;
                this.entityCollection.url = UrlLinks.entityCollectionaudit(this.guid);
                this.entityCollection.modelAttrName = "events";
                this.entityModel = new this.entityCollection.model();
                this.pervOld = [];
                this.commonTableOptions = {
                    collection: this.entityCollection,
                    includeFilter: false,
                    includePagination: false,
                    includePageSize: false,
                    includeAtlasTableSorting: true,
                    includeFooterRecords: false,
                    gridOpts: {
                        className: "table table-hover backgrid table-quickMenu",
                        emptyText: 'No records found!'
                    },
                    filterOpts: {},
                    paginatorOpts: {}
                };
                this.currPage = 1;
            },
            onRender: function() {
                $.extend(this.entityCollection.queryParams, { count: this.limit });
                this.fetchCollection({
                    next: this.ui.nextAuditData,
                    nextClick: false,
                    previous: this.ui.previousAuditData
                });
                this.entityCollection.comparator = function(model) {
                    return -model.get('timestamp');
                }
            },
            bindEvents: function() {},
            getToOffset: function() {
                return ((this.limit - 1) * this.currPage);
            },
            getFromOffset: function(toOffset) {
                // +2 because of toOffset is alrady in minus and limit is +1;
                return ((toOffset - this.limit) + 2);
            },
            renderOffset: function(options) {
                var entityLength;
                if (options.nextClick) {
                    options.previous.removeAttr("disabled");
                    if (this.entityCollection.length != 0) {
                        this.currPage++;

                    }
                } else if (options.previousClick) {
                    options.next.removeAttr("disabled");
                    if (this.currPage > 1 && this.entityCollection.models.length) {
                        this.currPage--;
                    }
                }
                if (this.entityCollection.models.length === this.limit) {
                    // Because we have 1 extra record.
                    entityLength = this.entityCollection.models.length - 1;
                } else {
                    entityLength = this.entityCollection.models.length
                }
                this.ui.activePage.attr('title', "Page " + this.currPage);
                this.ui.activePage.text(this.currPage);
                var toOffset = this.getToOffset();
                this.ui.pageRecordText.html("Showing  <u>" + entityLength + " records</u> From " + this.getFromOffset(toOffset) + " - " + toOffset);
            },
            fetchCollection: function(options) {
                var that = this;
                this.$('.fontLoader').show();
                this.$('.tableOverlay').show();
                if (that.entityCollection.models.length > 1) {
                    if (options.nextClick) {
                        this.pervOld.push(that.entityCollection.first().get('eventKey'));
                    }
                }
                this.entityCollection.fetch({
                    success: function() {
                        if (!(that.ui.pageRecordText instanceof jQuery)) {
                            return;
                        }
                        if (that.entityCollection.models.length < that.limit) {
                            options.previous.attr('disabled', true);
                            options.next.attr('disabled', true);
                        }
                        that.renderOffset(options);
                        that.entityCollection.sort();
                        if (that.entityCollection.models.length) {
                            if (that.entityCollection && (that.entityCollection.models.length < that.limit && that.currPage == 1) && that.next == that.entityCollection.last().get('eventKey')) {
                                options.next.attr('disabled', true);
                                options.previous.removeAttr("disabled");
                            } else {
                                that.next = that.entityCollection.last().get('eventKey');
                                if (that.pervOld.length === 0) {
                                    options.previous.attr('disabled', true);
                                }
                            }
                        }
                        that.renderTableLayoutView();
                        that.$('.fontLoader').hide();
                        that.$('.tableOverlay').hide();
                        that.$('.auditTable').show(); // Only for first time table show because we never hide after first render.
                    },
                    silent: true
                });
            },
            renderTableLayoutView: function() {
                var that = this;
                require(['utils/TableLayout'], function(TableLayout) {
                    var cols = new Backgrid.Columns(that.getAuditTableColumns());
                    that.RAuditTableLayoutView.show(new TableLayout(_.extend({}, that.commonTableOptions, {
                        columns: cols
                    })));
                    if (!(that.entityCollection.models.length < that.limit)) {
                        that.RAuditTableLayoutView.$el.find('table tr').last().hide();
                    }
                });
            },
            getAuditTableColumns: function() {
                var that = this;
                return this.entityCollection.constructor.getTableCols({

                    tool: {
                        label: "",
                        cell: "html",
                        editable: false,
                        sortable: false,
                        fixWidth: "20",
                        cell: Backgrid.ExpandableCell,
                        accordion: false,
                        expand: function(el, model) {
                            el.attr('colspan', '4');
                            require([
                                'views/audit/CreateAuditTableLayoutView',
                            ], function(CreateAuditTableLayoutView) {

                                that.action = model.get('action');
                                // $(el.target).attr('disabled', true);
                                var eventModel = that.entityCollection.fullCollection.findWhere({ 'eventKey': model.get('eventKey') }).toJSON(),
                                    collectionModel = new that.entityCollection.model(eventModel),
                                    view = new CreateAuditTableLayoutView({ guid: that.guid, entityModel: collectionModel, action: that.action, entity: that.entity, entityName: that.entityName, attributeDefs: that.attributeDefs });
                                view.render();
                                $(el).append($('<div>').html(view.$el));
                            });

                        }
                    },
                    user: {
                        label: "Users",
                        cell: "html",
                        editable: false,
                    },
                    timestamp: {
                        label: "Timestamp",
                        cell: "html",
                        editable: false,
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                            fromRaw: function(rawValue, model) {
                                return new Date(rawValue);
                            }
                        })
                    },
                    action: {
                        label: "Actions",
                        cell: "html",
                        editable: false,
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                            fromRaw: function(rawValue, model) {
                                if (Enums.auditAction[rawValue]) {
                                    return Enums.auditAction[rawValue];
                                } else {
                                    return rawValue;
                                }
                            }
                        })
                    }
                }, this.entityCollection);

            },
            onClickNextAuditData: function() {
                var that = this;
                this.ui.previousAuditData.removeAttr("disabled");
                $.extend(this.entityCollection.queryParams, {
                    startKey: function() {
                        return that.next;
                    }
                });
                this.fetchCollection({
                    next: this.ui.nextAuditData,
                    nextClick: true,
                    previous: this.ui.previousAuditData
                });
            },
            onClickPreviousAuditData: function() {
                var that = this;
                this.ui.nextAuditData.removeAttr("disabled");
                $.extend(this.entityCollection.queryParams, {
                    startKey: function() {
                        return that.pervOld.pop();
                    }
                });
                this.fetchCollection({
                    next: this.ui.nextAuditData,
                    previousClick: true,
                    previous: this.ui.previousAuditData
                });
            },
        });
    return AuditTableLayoutView;
});