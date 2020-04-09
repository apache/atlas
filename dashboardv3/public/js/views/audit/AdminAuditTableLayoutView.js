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
    'hbs!tmpl/audit/AdminAuditTableLayoutView_tmpl',
    'collection/VEntityList',
    'utils/Utils',
    'utils/UrlLinks',
    'utils/CommonViewFunction'
], function(require, Backbone, AdminAuditTableLayoutView_tmpl, VEntityList, Utils, UrlLinks, CommonViewFunction) {
    'use strict';

    var AdminAuditTableLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends AuditTableLayoutView */
        {
            _viewName: 'AdminAuditTableLayoutView',

            template: AdminAuditTableLayoutView_tmpl,

            /** Layout sub regions */
            regions: {
                RAuditTableLayoutView: "#r_adminAuditTableLayoutView",
                RQueryBuilderAdmin: "#r_attributeQueryBuilderAdmin"
            },

            /** ui selector cache */
            ui: {
                adminEntityClick: "[data-id='adminEntity']",
                adminType: "[data-id='adminType']",
                attrFilter: "[data-id='adminAttrFilter']",
                adminRegion: "[data-id='adminRegion']",
                attrApply: "[data-id='attrApply']",
                showDefault: "[data-id='showDefault']",
                attrClose: "[data-id='attrClose']"
            },
            /** ui events hash */
            events: function() {
                var events = {},
                    that = this;
                events["click " + this.ui.adminEntityClick] = "onClickAdminEntity";
                events["change " + this.ui.adminType] = "onClickAdminType";
                events["click " + this.ui.attrFilter] = function(e) {
                    this.$('.fa-angle-right').toggleClass('fa-angle-down');
                    this.$('.attributeResultContainer').addClass("overlay");
                    this.$('.attribute-filter-container, .attr-filter-overlay').toggleClass('hide');
                    this.onClickAttrFilter();
                };
                events["click " + this.ui.attrClose] = function(e) {
                    that.closeAttributeModel();
                };
                events["click " + this.ui.attrApply] = function(e) {
                    that.okAttrFilterButton(e);
                };
                return events;
            },
            /**
             * intialize a new AdminTableLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'searchTableFilters'));
                this.entityCollection = new VEntityList();
                this.limit = 25;
                this.entityCollection.url = UrlLinks.adminApiUrl();
                this.entityCollection.modelAttrName = "events";
                this.commonTableOptions = {
                    collection: this.entityCollection,
                    includeFilter: false,
                    includePagination: true,
                    includeFooterRecords: true,
                    includePageSize: true,
                    includeAtlasTableSorting: true,
                    includeTableLoader: true,
                    includeColumnManager: false,
                    gridOpts: {
                        className: "table table-hover backgrid table-quickMenu",
                        emptyText: 'No records found!'
                    },
                    columnOpts: {
                        opts: {
                            initialColumnsVisible: null,
                            saveState: false
                        },
                        visibilityControlOpts: {
                            buttonTemplate: _.template("<button class='btn btn-action btn-sm pull-right'>Columns&nbsp<i class='fa fa-caret-down'></i></button>")
                        },
                        el: this.ui.colManager
                    },
                    filterOpts: {},
                    paginatorOpts: {}
                };
                this.isFilters = null;
                this.adminAttrFilters = [{
                    "id": "startTime",
                    "label": "startTime (date)",
                    "operators": [
                        "=",
                        "!=",
                        ">",
                        "<",
                        ">=",
                        "<="
                    ],
                    "plugin": "daterangepicker",
                    "plugin_config": {
                        "locale": {
                            "format": "MM/DD/YYYY h:mm A"
                        },
                        "showDropdowns": true,
                        "singleDatePicker": true,
                        "timePicker": true
                    },
                    "type": "date"
                }, {
                    "id": "endTime",
                    "label": "endTime (date)",
                    "operators": [
                        "=",
                        "!=",
                        ">",
                        "<",
                        ">=",
                        "<="
                    ],
                    "plugin": "daterangepicker",
                    "plugin_config": {
                        "locale": {
                            "format": "MM/DD/YYYY h:mm A"
                        },
                        "showDropdowns": true,
                        "singleDatePicker": true,
                        "timePicker": true
                    },
                    "type": "date"
                }]
            },
            onRender: function() {
                var str = '<option>All</option><option>Purged</option>';
                this.ui.adminType.html(str);
                this.ui.adminType.select2({});
                this.ui.adminRegion.hide();
                this.getAdminCollection();
                this.entityCollection.comparator = function(model) {
                    return -model.get('timestamp');
                }
            },
            bindEvents: function() {},
            closeAttributeModel: function() {
                var that = this;
                that.$('.attributeResultContainer').removeClass("overlay");
                that.$('.fa-angle-right').toggleClass('fa-angle-down');
                that.$('.attribute-filter-container, .attr-filter-overlay').toggleClass('hide');
            },
            getAttributes: function() {
                var adminAttributes = [{
                    "attributeName": "userName",
                    "operator": "like",
                    "attributeValue": "admin"
                }];
                if (this.onlyPurged === true) {
                    adminAttributes.push({
                        "attributeName": "operation",
                        "operator": "like",
                        "attributeValue": "PURGE"
                    })
                }
                if (this.isFilters) {
                    _.each(this.isFilters, function(adminFilter) {
                        adminAttributes.push({
                            "attributeName": adminFilter.id,
                            "operator": adminFilter.operator,
                            "attributeValue": Date.parse(adminFilter.value).toString(),
                        })
                    })
                    this.isFilters = null;
                }
                return adminAttributes;
            },
            onClickAttrFilter: function() {
                var that = this;
                this.ui.adminRegion.show();
                require(['views/search/QueryBuilderView'], function(QueryBuilderView) {
                    that.RQueryBuilderAdmin.show(new QueryBuilderView({ adminAttrFilters: that.adminAttrFilters, searchTableFilters: that.searchTableFilters }));
                });
            },
            okAttrFilterButton: function(options) {
                var that = this,
                    isFilterValidate = true,
                    queryBuilderRef = that.RQueryBuilderAdmin.currentView.ui.builder;
                if (queryBuilderRef.data("queryBuilder")) {
                    var queryBuilder = queryBuilderRef.queryBuilder("getRules");
                    if (queryBuilder) {
                        that.isFilters = queryBuilder.rules;
                        that.searchTableFilters["adminAttrFilters"] = CommonViewFunction.attributeFilter.generateUrl({ value: queryBuilder, formatedDateToLong: true });
                    } else {
                        isFilterValidate = false
                    }
                }
                if (isFilterValidate) {
                    that.closeAttributeModel();
                    that.getAdminCollection();
                }
            },
            getAdminCollection: function() {
                var that = this,
                    adminParam = {
                        condition: "AND",
                        criterion: that.getAttributes()
                    };
                this.$('.fontLoader').show();
                this.$('.tableOverlay').show();
                $.extend(that.entityCollection.queryParams, { limit: this.limit, offset: 0, auditFilters: adminParam });
                var apiObj = {
                    sort: false,
                    data: that.entityCollection.queryParams,
                    success: function(dataOrCollection, response) {
                        that.entityCollection.fullCollection.reset(dataOrCollection);
                        that.renderTableLayoutView();
                        that.$('.fontLoader').hide();
                        that.$('.tableOverlay').hide();
                        that.$('.auditTable').show();
                    },
                    silent: true,
                    reset: true
                }
                this.entityCollection.getAdminData(apiObj);
            },
            renderTableLayoutView: function() {
                var that = this;
                this.ui.showDefault.hide();
                require(['utils/TableLayout'], function(TableLayout) {
                    var cols = new Backgrid.Columns(that.getAuditTableColumns());
                    that.RAuditTableLayoutView.show(new TableLayout(_.extend({}, that.commonTableOptions, {
                        columns: cols
                    })));
                });
            },
            getAuditTableColumns: function() {
                var that = this;
                return this.entityCollection.constructor.getTableCols({
                    result: {
                        label: "",
                        cell: "html",
                        editable: false,
                        sortable: false,
                        cell: Backgrid.ExpandableCell,
                        fixWidth: "20",
                        accordion: false,
                        alwaysVisible: true,
                        expand: function(el, model) {
                            var adminValues = '<div class="col-sm-6">',
                                newColumn = '';
                            el.attr('colspan', '6');
                            if (model.attributes.params) {
                                var guids = model.attributes.result.replace('[', '').replace(']', '').split(',');
                                _.each(guids, function(adminGuid, index) {
                                    if (index % 5 == 0 && index != 0) {
                                        adminValues += '</div><div class="col-sm-6">';
                                    }
                                    adminValues += '<a class="blue-link" data-id="adminEntity" >' + adminGuid.trim() + '</a></br>';
                                })
                                adminValues += '</div>';

                            } else {
                                adminValues = '';
                            }
                            var adminText = '<div class="row"><div class="col-sm-12 attr-details admin-attr-details"><div class="col-sm-2">Purged Entities: </div><div class="col-sm-10">' + adminValues + '</div></div></div>';
                            $(el).append($('<div>').html(adminText));

                        }
                    },
                    userName: {
                        label: "Users",
                        cell: "html",
                        editable: false
                    },
                    operation: {
                        label: "Operation",
                        cell: "String",
                        editable: false
                    },
                    clientId: {
                        label: "Client ID",
                        cell: "String",
                        editable: false
                    },
                    startTime: {
                        label: "Start Time",
                        cell: "html",
                        editable: false,
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                            fromRaw: function(rawValue, model) {
                                return new Date(rawValue);
                            }
                        })
                    },
                    endTime: {
                        label: "End Time",
                        cell: "html",
                        editable: false,
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                            fromRaw: function(rawValue, model) {
                                return new Date(rawValue);
                            }
                        })
                    }
                }, this.entityCollection);

            },
            onClickAdminType: function(e, value) {
                this.onlyPurged = e.currentTarget.value === "Purged";
                this.getAdminCollection();
            },
            onClickAdminEntity: function(e) {
                var that = this;
                require([
                    'modules/Modal', 'views/audit/AuditTableLayoutView', 'views/audit/CreateAuditTableLayoutView',
                ], function(Modal, AuditTableLayoutView, CreateAuditTableLayoutView) {
                    var obj = {
                            guid: $(e.target).text(),
                        },
                        modal = new Modal({
                            title: "Purged Entity Details: " + obj.guid,
                            content: new AuditTableLayoutView(obj),
                            mainClass: "modal-full-screen",
                            okCloses: true,
                            showFooter: false,
                        }).open();

                    modal.on('closeModal', function() {
                        $('.modal').css({ 'padding-right': '0px !important' });
                        modal.trigger('cancel');
                    });
                    modal.$el.on('click', 'td a', function() {
                        modal.trigger('cancel');
                    });
                });
            }
        });
    return AdminAuditTableLayoutView;
});