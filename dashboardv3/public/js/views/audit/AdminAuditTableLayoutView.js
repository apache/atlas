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
    'utils/CommonViewFunction',
    'utils/Enums'
], function(require, Backbone, AdminAuditTableLayoutView_tmpl, VEntityList, Utils, UrlLinks, CommonViewFunction, Enums) {
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
                adminPurgedEntityClick: "[data-id='adminPurgedEntity']",
                adminAuditEntityDetails: "[data-id='adminAuditEntityDetails']",
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
                events["click " + this.ui.adminPurgedEntityClick] = "onClickAdminPurgedEntity";
                events["click " + this.ui.adminAuditEntityDetails] = "showAdminAuditEntity";
                events["click " + this.ui.attrFilter] = function(e) {
                    this.ui.attrFilter.find('.fa-angle-right').toggleClass('fa-angle-down');
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
                _.extend(this, _.pick(options, 'searchTableFilters', 'entityDefCollection', 'enumDefCollection'));
                this.entityCollection = new VEntityList();
                this.limit = 25;
                this.offset = 0;
                this.entityCollection.url = UrlLinks.adminApiUrl();
                this.entityCollection.modelAttrName = "events";
                this.commonTableOptions = {
                    collection: this.entityCollection,
                    includePagination: false,
                    includeAtlasPagination: true,
                    includeFooterRecords: false,
                    includeColumnManager: false,
                    includeOrderAbleColumns: false,
                    includeSizeAbleColumns: false,
                    includeTableLoader: true,
                    includeAtlasPageSize: true,
                    includeAtlasTableSorting: true,
                    atlasPaginationOpts: {
                        limit: this.limit,
                        offset: this.offset,
                        fetchCollection: this.getAdminCollection.bind(this),
                    },
                    gridOpts: {
                        emptyText: 'No Record found!',
                        className: 'table table-hover backgrid table-quickMenu colSort'
                    },
                    filterOpts: {},
                    paginatorOpts: {}
                };
                this.isFilters = null;
                this.adminAuditEntityData = {};
            },
            onRender: function() {
                this.ui.adminRegion.hide();
                this.getAdminCollection();
                this.entityCollection.comparator = function(model) {
                    return -model.get('timestamp');
                }
                this.renderTableLayoutView();
            },
            onShow: function() {
                this.$('.fontLoader').show();
                this.$('.tableOverlay').show();
            },
            bindEvents: function() {},
            closeAttributeModel: function() {
                var that = this;
                that.$('.attributeResultContainer').removeClass("overlay");
                that.ui.attrFilter.find('.fa-angle-right').toggleClass('fa-angle-down');
                that.$('.attribute-filter-container, .attr-filter-overlay').toggleClass('hide');
            },
            onClickAttrFilter: function() {
                var that = this;
                this.ui.adminRegion.show();
                require(['views/search/QueryBuilderView'], function(QueryBuilderView) {
                    that.RQueryBuilderAdmin.show(new QueryBuilderView({ adminAttrFilters: true, searchTableFilters: that.searchTableFilters, entityDefCollection: that.entityDefCollection, enumDefCollection: that.enumDefCollection }));
                });
            },
            okAttrFilterButton: function(options) {
                var that = this,
                    isFilterValidate = true,
                    queryBuilderRef = that.RQueryBuilderAdmin.currentView.ui.builder;
                if (queryBuilderRef.data("queryBuilder")) {
                    var queryBuilder = queryBuilderRef.queryBuilder("getRules");
                    if (queryBuilder) {
                        that.ruleUrl = that.searchTableFilters["adminAttrFilters"] = CommonViewFunction.attributeFilter.generateUrl({ value: queryBuilder, formatedDateToLong: true });
                        that.isFilters = queryBuilder.rules.length ? queryBuilder.rules : null;
                    } else {
                        isFilterValidate = false
                    }
                }
                if (isFilterValidate) {
                    that.closeAttributeModel();
                    that.defaultPagination();
                    that.getAdminCollection();
                }
            },
            getAdminCollection: function(option) {
                var that = this,
                    auditFilters = CommonViewFunction.attributeFilter.generateAPIObj(that.ruleUrl);
                $.extend(that.entityCollection.queryParams, { auditFilters: that.isFilters ? auditFilters : null });
                var apiObj = {
                    sort: false,
                    data: that.entityCollection.queryParams,
                    success: function(dataOrCollection, response) {
                        that.entityCollection.state.pageSize = that.entityCollection.queryParams.limit || 25;
                        that.entityCollection.fullCollection.reset(dataOrCollection, option);
                    },
                    complete: function() {
                        that.$('.fontLoader').hide();
                        that.$('.tableOverlay').hide();
                        that.$('.auditTable').show();
                    },
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
            createTableWithValues: function(tableDetails, isAdminAudit) {
                var attrTable = CommonViewFunction.propertyTable({
                    scope: this,
                    valueObject: tableDetails,
                    fromAdminAudit: isAdminAudit
                });
                return attrTable;
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
                            var operation = model.get('operation'),
                                results = model.get('result') || null,
                                adminText = 'No records found',
                                adminTypDetails = null;
                            el.attr('colspan', '7');
                            if (results) {
                                var adminValues = null;
                                if (operation == "PURGE") {
                                    adminValues = '<ul class="col-sm-6">';
                                    var guids = results.replace('[', '').replace(']', '').split(',');
                                    adminTypDetails = Enums.category[operation];
                                    _.each(guids, function(adminGuid, index) {
                                        if (index % 5 == 0 && index != 0) {
                                            adminValues += '</ul><ul class="col-sm-6">';
                                        }
                                        adminValues += '<li class="blue-link" data-id="adminPurgedEntity" >' + adminGuid.trim() + '</li>';
                                    })
                                    adminValues += '</ul>';
                                    adminText = '<div class="row"><div class="attr-details"><h4 style="word-break: break-word;">' + adminTypDetails + '</h4>' + adminValues + '</div></div>';
                                } else {
                                    var resultData = JSON.parse(results),
                                        typeName = model.get('params').split(','),
                                        typeContainer = '';
                                    _.each(typeName, function(name) {
                                        var typeData = resultData[name],
                                            adminValues = (typeName.length == 1) ? '<ul class="col-sm-4">' : '<ul>';
                                        adminTypDetails = Enums.category[name] + " " + Enums.auditAction[operation];
                                        typeContainer += '<div class="attr-type-container"><h4 style="word-break: break-word;">' + adminTypDetails + '</h4>';
                                        _.each(typeData, function(typeDefObj, index) {
                                            if (index % 5 == 0 && index != 0 && typeName.length == 1) {
                                                adminValues += '</ul><ul class="col-sm-4">';
                                            }
                                            var panelId = typeDefObj.name.split(" ").join("") + model.get('startTime');
                                            that.adminAuditEntityData[panelId] = typeDefObj;
                                            adminValues += '<li class="blue-link" data-id="adminAuditEntityDetails" data-auditEntityId=' + panelId + '>' + typeDefObj.name + '</li>';
                                        });
                                        adminValues += '</ul>';
                                        typeContainer += adminValues + '</div>';
                                    })
                                    var typeClass = (typeName.length == 1) ? null : "admin-audit-details";
                                    adminText = '<div class="row"><div class="attr-details ' + typeClass + '">' + typeContainer + '</div></div>';
                                }
                            }
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
                    resultCount: {
                        label: "Result Count",
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
            defaultPagination: function() {
                $.extend(this.entityCollection.queryParams, { limit: this.limit, offset: this.offset });
                this.renderTableLayoutView();
            },
            showAdminAuditEntity: function(e) {
                var typeDefObj = this.adminAuditEntityData[e.target.dataset.auditentityid],
                    typeDetails = this.createTableWithValues(typeDefObj, true),
                    view = '<table class="table admin-audit-details bold-key" ><tbody >' + typeDetails + '</tbody></table>',
                    modalData = {
                        title: Enums.category[typeDefObj.category] + " Type Details: " + typeDefObj.name,
                        htmlContent: view,
                        mainClass: "modal-full-screen",
                        okCloses: true,
                        showFooter: false,
                        width: "40%"
                    };
                this.showModal(modalData);
            },
            onClickAdminPurgedEntity: function(e) {
                var that = this;
                require(['views/audit/AuditTableLayoutView'], function(AuditTableLayoutView) {
                    var obj = {
                            guid: $(e.target).text(),
                        },
                        modalData = {
                            title: "Purged Entity Details: " + obj.guid,
                            content: new AuditTableLayoutView(obj),
                            mainClass: "modal-full-screen",
                            okCloses: true,
                            showFooter: false,
                        };
                    that.showModal(modalData);
                });
            },
            showModal: function(modalObj, title) {
                var that = this;
                require([
                    'modules/Modal'
                ], function(Modal) {
                    var modal = new Modal(modalObj).open();
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