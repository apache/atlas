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
                auditCreate: "[data-id='auditCreate']",
                previousAuditData: "[data-id='previousAuditData']",
                nextAuditData: "[data-id='nextAuditData']",
                pageRecordText: "[data-id='pageRecordText']"
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
                _.extend(this, _.pick(options, 'guid', 'entity', 'entityName'));
                this.entityCollection = new VEntityList();
                this.count = 26;
                this.entityCollection.url = UrlLinks.entityCollectionaudit(this.guid);
                this.entityCollection.modelAttrName = "events";
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
                this.currPage = 1;
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
            bindEvents: function() {},
            getToOffset: function() {
                var toOffset = 0;
                if (this.entityCollection.models.length < this.count) {
                    toOffset = ((this.currPage - 1) * (this.count - 1) + (this.entityCollection.models.length));
                } else {
                    toOffset = ((this.currPage - 1) * (this.count - 1) + (this.entityCollection.models.length - 1));
                }
                return toOffset;
            },
            renderOffset: function(options) {
                var that = this;
                if (options.nextClick) {
                    options.previous.removeAttr("disabled");
                    if (that.entityCollection.length != 0) {
                        that.currPage++;
                        that.ui.pageRecordText.html("Showing " + ((that.currPage - 1) * (that.count - 1) + 1) + " - " + that.getToOffset());
                    }
                }
                if (options.previousClick) {
                    options.next.removeAttr("disabled");
                    if (that.currPage > 1 && that.entityCollection.models.length) {
                        that.currPage--;
                        that.ui.pageRecordText.html("Showing " + ((that.currPage - 1) * (that.count - 1) + 1) + " - " + that.getToOffset());
                    }
                }
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
                        if (that.entityCollection.models.length < that.count) {
                            options.previous.attr('disabled', true);
                            options.next.attr('disabled', true);
                        }
                        if (!options.nextClick && !options.previousClick) {
                            that.ui.pageRecordText.html("Showing " + ((that.currPage - 1) * (that.count - 1) + 1) + " - " + that.getToOffset());
                        }
                        that.$('.fontLoader').hide();
                        that.$('.auditTable').show();
                        that.renderOffset(options);
                        if (that.entityCollection.models.length) {
                            if (that.entityCollection && (that.entityCollection.models.length < that.count && that.currPage == 1) && that.next == that.entityCollection.last().get('eventKey')) {
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
                    if (!(that.entityCollection.models.length < that.count)) {
                        that.RAuditTableLayoutView.$el.find('table tr').last().hide();
                    }
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
                                if (Enums.auditAction[rawValue]) {
                                    return Enums.auditAction[rawValue];
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
                                return '<div class="label label-success auditDetailBtn" data-id="auditCreate" data-action="' + Enums.auditAction[model.get('action')] + '" data-modalId="' + model.get('eventKey') + '">Detail</div>';
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
                    that.action = $(e.target).data("action");
                    var eventModel = that.entityCollection.findWhere({ 'eventKey': $(e.currentTarget).data('modalid') }).toJSON(),
                        collectionModel = new that.entityCollection.model(eventModel),
                        view = new CreateAuditTableLayoutView({ guid: that.guid, entityModel: collectionModel, action: that.action, entity: that.entity, entityName: that.entityName });
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
                    });
                });
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
