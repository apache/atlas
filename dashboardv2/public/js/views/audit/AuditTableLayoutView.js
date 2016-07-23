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
    'utils/Globals'
], function(require, Backbone, AuditTableLayoutView_tmpl, VEntityList, Globals) {
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
                auditValue: "[data-id='auditValue']",
                auditCreate: "[data-id='auditCreate']",
                previousAuditData: "[data-id='previousAuditData']",
                nextAuditData: "[data-id='nextAuditData']"
            },
            /** ui events hash */
            events: function() {
                var events = {};
                events["click " + this.ui.auditCreate] = "onClickAuditCreate";
                events["click " + this.ui.nextAuditData] = "onClickNextAuditData";
                events["click " + this.ui.previousAuditData] = "onClickPreviousAuditData";
                return events;
            },
            /**
             * intialize a new AuditTableLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'globalVent', 'guid'));
                this.entityCollection = new VEntityList();
                this.count = 25;
                this.entityCollection.url = "/api/atlas/entities/" + this.guid + "/audit";
                this.entityCollection.modelAttrName = "events"
                this.entityModel = new this.entityCollection.model();
                this.pervOld = [];
                this.commonTableOptions = {
                    collection: this.entityCollection,
                    includeFilter: false,
                    includePagination: false,
                    includePageSize: false,
                    includeFooterRecords: false,
                    gridOpts: {
                        className: "table table-hover backgrid table-quickMenu",
                        emptyText: 'No records found!'
                    },
                    filterOpts: {},
                    paginatorOpts: {}
                };
            },
            onRender: function() {
                $.extend(this.entityCollection.queryParams, { count: this.count });
                this.fetchCollection({
                    next: this.ui.nextAuditData,
                    nextClick: false,
                    previous: this.ui.previousAuditData
                });
                this.renderTableLayoutView();
            },
            fetchCollection: function(options) {
                var that = this;
                this.$('.fontLoader').show();
                this.$('.auditTable').hide();
                if (that.entityCollection.models.length > 1) {
                    if (options.nextClick) {
                        this.pervOld.push(that.entityCollection.first().get('eventKey'));
                    }
                }
                this.entityCollection.fetch({
                    success: function() {
                        that.$('.fontLoader').hide();
                        that.$('.auditTable').show();
                        options.previous.attr('disabled', true);
                        if (that.entityCollection.models.length <= 1) {
                            options.next.attr('disabled', true);
                        }
                        if (that.entityCollection.models.length == 1 && that.next == that.entityCollection.last().get('eventKey')) {
                            options.next.attr('disabled', true);
                            options.previous.removeAttr("disabled");
                            that.entityCollection.reset();
                        } else {
                            if (that.entityCollection.models.length > 0) {
                                that.next = that.entityCollection.last().get('eventKey');
                                if (options.nextClick) {
                                    options.previous.removeAttr("disabled");
                                }
                                if (options.previousClick) {
                                    options.previous.removeAttr("disabled");
                                }
                                if (that.pervOld.length == 0) {
                                    options.previous.attr('disabled', true);
                                }
                            }
                            that.renderTableLayoutView();
                        }
                    },
                    silent: true
                });
            },
            renderTableLayoutView: function() {
                var that = this;
                require(['utils/TableLayout'], function(TableLayout) {
                    var cols = new Backgrid.Columns(that.getAuditTableColumns());
                    that.RAuditTableLayoutView.show(new TableLayout(_.extend({}, that.commonTableOptions, {
                        globalVent: that.globalVent,
                        columns: cols
                    })));
                });
            },
            getAuditTableColumns: function() {
                var that = this;
                return this.entityCollection.constructor.getTableCols({
                    user: {
                        label: "Users",
                        cell: "html",
                        editable: false,
                        sortable: false,
                    },
                    timestamp: {
                        label: "Timestamp",
                        cell: "html",
                        editable: false,
                        sortable: false,
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
                        sortable: false,
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                            fromRaw: function(rawValue, model) {
                                that.detailBtnDisable = false;
                                if (Globals.auditAction[rawValue]) {
                                    return Globals.auditAction[rawValue];
                                } else {
                                    return rawValue;
                                }
                            }
                        })
                    },
                    tool: {
                        label: "Tools",
                        cell: "html",
                        editable: false,
                        sortable: false,
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                            fromRaw: function(rawValue, model) {
                                return '<div class="label label-success auditDetailBtn" data-id="auditCreate" data-action="' + Globals.auditAction[model.attributes.action] + '" disabled="' + that.detailBtnDisable + '" data-modalId="' + model.get('eventKey') + '">Detail</div>';
                            }
                        })
                    },

                }, this.entityCollection);

            },
            onClickAuditCreate: function(e) {
                var that = this;
                require([
                    'modules/Modal',
                    'views/audit/CreateAuditTableLayoutView',
                ], function(Modal, CreateAuditTableLayoutView) {
                    var collectionModel = that.entityCollection.findWhere({ 'eventKey': $(e.currentTarget).data('modalid') });
                    that.action = $(e.target).data("action");
                    var view = new CreateAuditTableLayoutView({ guid: that.guid, model: collectionModel, action: that.action });
                    var modal = new Modal({
                        title: that.action,
                        content: view,
                        okCloses: true,
                        showFooter: true,
                    }).open();
                    view.on('closeModal', function() {
                        modal.trigger('cancel');
                    });
                    view.$el.on('click', 'td a', function() {
                        modal.trigger('cancel');
                    })
                });
            },
            onClickNextAuditData: function() {
                var that = this;
                this.ui.previousAuditData.removeAttr("disabled");
                $.extend(this.entityCollection.queryParams, {
                    startKey: function() {
                        return that.next
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
                        return that.pervOld.pop()
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
