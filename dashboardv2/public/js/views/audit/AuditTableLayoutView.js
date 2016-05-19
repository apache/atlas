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
    'moment'
], function(require, Backbone, AuditTableLayoutView_tmpl, VEntityList, moment) {
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
            },
            /** ui events hash */
            events: function() {
                var events = {};
                events["click " + this.ui.auditCreate] = "onClickAuditCreate";
                return events;
            },
            /**
             * intialize a new AuditTableLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'globalVent', 'guid'));
                this.entityCollection = new VEntityList();
                this.entityCollection.url = "/api/atlas/entities/" + this.guid + "/audit";
                this.entityCollection.modelAttrName = "events";
                //this.collectionObject = entityCollection;
                this.entityModel = new this.entityCollection.model();
                this.commonTableOptions = {
                    collection: this.entityCollection,
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
                this.listenTo(this.entityCollection, "reset", function(value) {
                    this.renderTableLayoutView();
                }, this);
            },
            onRender: function() {
                this.entityCollection.fetch({ reset: true });
                this.bindEvents();
                this.renderTableLayoutView();
            },
            renderTableLayoutView: function() {
                var that = this;
                require(['utils/TableLayout'], function(TableLayout) {
                    var cols = new Backgrid.Columns(that.getAuditTableColumns());
                    that.RAuditTableLayoutView.show(new TableLayout(_.extend({}, that.commonTableOptions, {
                        globalVent: that.globalVent,
                        columns: cols,
                        gridOpts: {
                            className: "table table-bordered table-hover table-condensed backgrid table-quickMenu",
                        },
                    })));
                });
            },
            getAuditTableColumns: function() {
                var that = this;
                return this.entityCollection.constructor.getTableCols({
                    user: {
                        label: "User",
                        cell: "html",
                        editable: false,
                        sortable: false,
                    },
                    timestamp: {
                        label: "Timestamp",
                        cell: "time",
                        editable: false,
                        sortable: false,
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                            fromRaw: function(rawValue, model) {
                                return moment(rawValue).format("YYYY-MM-DD HH:mm:ss,SSS");
                            }
                        })
                    },
                    action: {
                        label: "Action",
                        cell: "html",
                        editable: false,
                        sortable: false
                    },
                    tool: {
                        label: "Tool",
                        cell: "html",
                        editable: false,
                        sortable: false,
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                            fromRaw: function(rawValue, model) {
                                return '<div class="label label-success auditCreateBtn" data-id="auditCreate">Create</div>';
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

                    var view = new CreateAuditTableLayoutView({ guid: that.guid });
                    var modal = new Modal({
                        title: 'Create',
                        content: view,
                        okCloses: true,
                        showFooter: true,
                    }).open();
                });
            }
        });
    return AuditTableLayoutView;
});
