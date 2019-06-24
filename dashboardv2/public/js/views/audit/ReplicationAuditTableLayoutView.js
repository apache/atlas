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
    'hbs!tmpl/audit/ReplicationAuditTableLayoutView_tmpl',
    'utils/CommonViewFunction',
    'utils/Utils',
    'collection/VSearchList',
    'collection/VEntityList',
    'utils/Messages',
    'utils/UrlLinks'
], function(require, Backbone, ReplicationAuditTableLayoutView_tmpl, CommonViewFunction, Utils, VSearchList, VEntityList, Messages, UrlLinks) {
    'use strict';

    var ReplicationAuditTableLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends TagDetailTableLayoutView */
        {
            _viewName: 'ReplicationAuditTableLayoutView',

            template: ReplicationAuditTableLayoutView_tmpl,

            /** Layout sub regions */
            regions: {
                RReplicationAuditTableLayoutView: "#r_replicationAuditTableLayoutView"
            },

            /** ui selector cache */
            ui: {
                auditDetail: "[data-action='audit_detail']",
            },
            /** ui events hash */
            events: function() {
                var events = {}
                events["click " + this.ui.auditDetail] = "onClickAuditDetails";
                return events;
            },
            /**
             * intialize a new TagDetailTableLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'entity', 'entityName', 'attributeDefs'));
                this.searchCollection = new VSearchList();
                this.entityModel = new(new VEntityList()).model();
                this.limit = 25;
                this.offset = 0;
                this.name = Utils.getName(this.entity);
                this.commonTableOptions = {
                    collection: this.searchCollection,
                    includePagination: false,
                    includeAtlasPagination: true,
                    includeFooterRecords: false,
                    includeColumnManager: false,
                    includeOrderAbleColumns: false,
                    includeSizeAbleColumns: false,
                    includeTableLoader: true,
                    atlasPaginationOpts: {
                        limit: this.limit,
                        offset: this.offset,
                        fetchCollection: this.fetchCollection.bind(this),
                    },
                    gridOpts: {
                        emptyText: 'No Record found!',
                        className: 'table table-hover backgrid table-quickMenu colSort'
                    },
                    filterOpts: {},
                    paginatorOpts: {}
                };
            },
            bindEvents: function() {},
            onRender: function() {
                this.renderTableLayoutView();
            },
            fetchCollection: function(options) {
                var that = this;

                this.searchCollection.getExpimpAudit(this.searchCollection.queryParams, {
                    success: function(response) {
                        that.searchCollection.reset(response, options);
                    }
                });
            },
            renderTableLayoutView: function() {
                var that = this;
                require(['utils/TableLayout'], function(TableLayout) {
                    var columnCollection = Backgrid.Columns.extend({
                        sortKey: "displayOrder",
                        comparator: function(item) {
                            return item.get(this.sortKey) || 999;
                        },
                        setPositions: function() {
                            _.each(this.models, function(model, index) {
                                model.set("displayOrder", index + 1, { silent: true });
                            });
                            return this;
                        }
                    });
                    var columns = new columnCollection(that.getColumn());
                    columns.setPositions().sort();
                    that.RReplicationAuditTableLayoutView.show(new TableLayout(_.extend({}, that.commonTableOptions, {
                        columns: columns
                    })));
                    _.extend(that.searchCollection.queryParams, { limit: that.limit, offset: that.offset, "serverName": that.name });
                    that.fetchCollection(_.extend({ 'fromUrl': true }));
                });
            },
            getColumn: function(argument) {
                var that = this,
                    col = {};

                col['operation'] = {
                    label: "Operation",
                    cell: "string",
                    editable: false,
                    sortable: false,
                    className: "searchTableName"
                };
                col['sourceServerName'] = {
                    label: "Source Server",
                    cell: "string",
                    editable: false,
                    sortable: false,
                    className: "searchTableName"
                };
                col['targetServerName'] = {
                    label: "Target Server",
                    cell: "string",
                    editable: false,
                    sortable: false,
                    className: "searchTableName"
                };
                col['startTime'] = {
                    label: "Operation StartTime",
                    cell: "html",
                    editable: false,
                    sortable: false,
                    formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                        fromRaw: function(rawValue, model) {
                            if (rawValue) {
                                return new Date(rawValue);
                            } else {
                                return '-';
                            }
                        }
                    })
                };
                col['endTime'] = {
                    label: "Operation EndTime",
                    cell: "html",
                    editable: false,
                    sortable: false,
                    formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                        fromRaw: function(rawValue, model) {
                            if (rawValue) {
                                return new Date(rawValue);
                            } else {
                                return '-';
                            }
                        }
                    })
                };
                col['tools'] = {
                    label: "Tools",
                    cell: "html",
                    editable: false,
                    sortable: false,
                    formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                        fromRaw: function(rawValue, model) {
                            return '<div class="btn btn-action btn-sm" data-action="audit_detail" data-guid="' + model.get('guid') + '">Detail</div>';
                        }
                    })
                };
                return this.searchCollection.constructor.getTableCols(col, this.searchCollection);
            },
            onClickAuditDetails: function(e) {
                var that = this;
                require([
                    'modules/Modal',
                    'views/audit/CreateAuditTableLayoutView',
                ], function(Modal, CreateAuditTableLayoutView) {
                    $(e.target).attr('disabled', true);
                    var guid = $(e.target).data("guid"),
                        model = that.searchCollection.fullCollection.findWhere({ 'guid': guid }),
                        result = JSON.parse(model.get("resultSummary")),
                        view = "<table class='table table-bordered table-striped'>" + CommonViewFunction.propertyTable({ scope: that, valueObject: result, attributeDefs: that.attributeDefs }) + "</table>";
                    var modal = new Modal({
                        title: model.get("operation") + " Details",
                        content: view,
                        contentHtml: true,
                        okCloses: true,
                        showFooter: true,
                    });
                    modal.open();
                    modal.on('closeModal', function() {
                        modal.trigger('cancel');
                    });
                    modal.on('hidden.bs.modal', function() {
                        that.$('.btn-action[data-action="audit_detail"]').attr('disabled', false);
                    });
                });
            },
        });
    return ReplicationAuditTableLayoutView;
});