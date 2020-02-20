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
    'hbs!tmpl/audit/PurgeAuditTableLayoutView_tmpl',
    'collection/VEntityList',
    'models/VSearch',
    'utils/Utils',
    'utils/Enums',
    'utils/UrlLinks',
    'utils/CommonViewFunction'
], function(require, Backbone, PurgeAuditTableLayoutView_tmpl, VEntityList, VSearch, Utils, Enums, UrlLinks, CommonViewFunction) {
    'use strict';

    var PurgeAuditTableLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends AuditTableLayoutView */
        {
            _viewName: 'PurgeAuditTableLayoutView',

            template: PurgeAuditTableLayoutView_tmpl,

            /** Layout sub regions */
            regions: {
                RAuditTableLayoutView: "#r_purgeAuditTableLayoutView",
                RQueryBuilderPurge: "#r_attributeQueryBuilderPurge",
                RNamespaceTableLayoutView: "#r_namespaceTableLayoutView"
            },

            /** ui selector cache */
            ui: {
                auditCreate: "[data-id='auditCreate']",
                previousAuditData: "[data-id='previousAuditData']",
                nextAuditData: "[data-id='nextAuditData']",
                pageRecordText: "[data-id='pageRecordText']",
                activePage: "[data-id='activePage']",
                purgeEntityClick: "[data-id='purgeEntity']",
                purgeType: "[data-id='purgeType']",
                attrFilter: "[data-id='purgeAttrFilter']",
                purgeRegion: "[data-id='purgeRegion']",
                attrApply: "[data-id='attrApply']",
                showDefault: "[data-id='showDefault']",
                attrClose: "[data-id='attrClose']",
                tablist: '[data-id="tab-list"] li'
            },
            /** ui events hash */
            events: function() {
                var events = {},
                    that = this;
                events["click " + this.ui.purgeEntityClick] = "onClickPurgeEntity";
                events["change " + this.ui.purgeType] = "onClickPurgeType";
                events["click " + this.ui.nextAuditData] = "onClickNextAuditData";
                events["click " + this.ui.previousAuditData] = "onClickPreviousAuditData";
                events["click " + this.ui.attrFilter] = function(e) {
                    // this.$('.fa-chevron-right').toggleClass('fa-chevron-down');
                    this.$('.fa-angle-right').toggleClass('fa-angle-down');
                    this.$('.attributeResultContainer').addClass("overlay");
                    this.$('.attribute-filter-container, .attr-filter-overlay').toggleClass('hide');
                    // this.$('.attribute-filter-container').toggleClass('attribute-filter-container')
                    this.onClickAttrFilter();
                };
                events["click " + this.ui.attrClose] = function(e) {
                    that.closeAttributeModel();
                };
                events["click " + this.ui.attrApply] = function(e) {
                    that.okAttrFilterButton(e);
                };
                events["click " + this.ui.tablist] = function(e) {
                    var tabValue = $(e.currentTarget).attr('role');
                    Utils.setUrl({
                        url: Utils.getUrlState.getQueryUrl().queyParams[0],
                        urlParams: { tabActive: tabValue || 'properties' },
                        mergeBrowserUrl: false,
                        trigger: false,
                        updateTabState: true
                    });

                };
                return events;
            },
            /**
             * intialize a new AuditTableLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'value', 'guid', 'entity', 'entityName', 'attributeDefs', 'nameSpaceCollection'));
                this.entityCollection = new VEntityList();
                this.limit = 26;
                this.entityCollection.url = UrlLinks.purgeApiUrl();
                this.entityCollection.modelAttrName = "events";
                this.entityModel = new this.entityCollection.model();
                this.pervOld = [];
                this.onlyPurge = true;
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
                this.isFilters = null;
                this.purgeAttrFilters = [{
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
                    "optgroup": "Select Purge Attribute",
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
                    "optgroup": "Select Purge Attribute",
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
                var str = '<option>All</option><option>Purge</option>';
                this.ui.purgeType.html(str);
                this.ui.purgeType.select2({});
                this.bindEvents();
                this.ui.purgeRegion.hide();
                this.getPurgeParam();
                this.entityCollection.comparator = function(model) {
                    return -model.get('timestamp');
                }
            },
            onShow: function() {
                if (this.value && this.value.tabActive) {
                    this.$('.nav.nav-tabs').find('[role="' + this.value.tabActive + '"]').addClass('active').siblings().removeClass('active');
                    this.$('.tab-content').find('[role="' + this.value.tabActive + '"]').addClass('active').siblings().removeClass('active');
                    $("html, body").animate({ scrollTop: (this.$('.tab-content').offset().top + 1200) }, 1000);
                }
            },
            bindEvents: function() {
                this.renderNameSpaceLayoutView();
            },

            renderNameSpaceLayoutView: function(obj) {
                var that = this;
                require(['views/name_space/AllNameSpaceTableLayoutView'], function(AllNameSpaceTableLayoutView) {
                    that.RNamespaceTableLayoutView.show(new AllNameSpaceTableLayoutView({ nameSpaceCollection: that.nameSpaceCollection }));
                });
            },
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
            getPurgeParam: function() {
                var that = this;
                that.fetchCollection({
                    next: that.ui.nextAuditData,
                    nextClick: false,
                    previous: that.ui.previousAuditData,
                    isPurge: that.onlyPurge
                });
            },
            closeAttributeModel: function() {
                var that = this;
                that.$('.attributeResultContainer').removeClass("overlay");
                that.$('.fa-angle-right').toggleClass('fa-angle-down');
                that.$('.attribute-filter-container, .attr-filter-overlay').toggleClass('hide');
            },
            getAttributes: function() {
                var purgeAttributes = [{
                    "attributeName": "operation",
                    "operator": "like",
                    "attributeValue": "PURGE"
                }];
                if (!this.onlyPurge) {
                    purgeAttributes.push({
                        "attributeName": "userName",
                        "operator": "like",
                        "attributeValue": "admin"
                    })
                }
                if (this.isFilters) {
                    _.each(this.isFilters, function(purgeFilter) {
                        purgeAttributes.push({
                            "attributeName": purgeFilter.id,
                            "operator": purgeFilter.operator,
                            "attributeValue": Date.parse(purgeFilter.value).toString(),
                        })
                    })
                    this.isFilters = null;
                }
                return purgeAttributes;
            },

            renderQueryBuilder: function(obj, rQueryBuilder) {
                var that = this;
                require(['views/search/QueryBuilderView'], function(QueryBuilderView) {
                    rQueryBuilder.show(new QueryBuilderView(obj));
                });
            },
            onClickAttrFilter: function() {
                var that = this;
                this.ui.purgeRegion.show();
                require(['views/search/QueryBuilderView'], function(QueryBuilderView) {
                    that.RQueryBuilderPurge.show(new QueryBuilderView({ purgeAttrFilters: that.purgeAttrFilters }));
                });

            },
            okAttrFilterButton: function(options) {
                var that = this,
                    isFilterValidate = true,
                    queryBuilderRef = that.RQueryBuilderPurge.currentView.ui.builder;
                if (queryBuilderRef.data("queryBuilder")) {
                    var queryBuilder = queryBuilderRef.queryBuilder("getRules");
                    queryBuilder ? that.isFilters = queryBuilder.rules : isFilterValidate = false;
                }
                if (isFilterValidate) {
                    that.closeAttributeModel();
                    that.getPurgeParam();
                }
            },
            fetchCollection: function(options) {
                var that = this,
                    purgeParam = {
                        condition: "AND",
                        criterion: that.getAttributes()
                    };
                this.$('.fontLoader').show();
                this.$('.tableOverlay').show();
                if (that.entityCollection.models.length > 1) {
                    if (options.nextClick) {
                        this.pervOld.push(that.entityCollection.first().get('eventKey'));
                    }
                }
                var apiObj = {
                    sort: false,
                    success: function(dataOrCollection, response) {
                        if (!(that.ui.pageRecordText instanceof jQuery)) {
                            return;
                        }
                        that.entityCollection.fullCollection.reset(dataOrCollection);
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
                    silent: true,
                    reset: true
                }
                $.extend(that.entityCollection.queryParams, { limit: 25, offset: 0, auditFilters: purgeParam });
                $.extend(apiObj, { contentType: 'application/json', dataType: 'json', data: JSON.stringify(that.entityCollection.queryParams) })
                this.entityCollection.constructor.nonCrudOperation.call(this, UrlLinks.purgeApiUrl(), "POST", apiObj);
            },
            renderTableLayoutView: function() {
                var that = this;

                this.ui.showDefault.hide();
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
                    result: {
                        label: "",
                        cell: "html",
                        editable: false,
                        sortable: false,
                        cell: Backgrid.ExpandableCell,
                        fixWidth: "20",
                        accordion: false,
                        expand: function(el, model) {
                            var purgeValues = '';
                            if (model.attributes.params) {
                                var guids = model.attributes.result.replace('[', '').replace(']', '').split(',');
                                _.each(guids, function(purgeGuid) {
                                    // purgeGuid.trim();
                                    purgeValues += '<a class="blue-link" data-id="purgeEntity" >' + purgeGuid.trim() + '</a></br>';
                                })
                            } else {
                                purgeValues = '';
                            }
                            var purgeText = '<div class="row"><div class="col-sm-2">Purge Entities: </div><div class="col-sm-10">' + purgeValues + '</div></div>';
                            /* set expanded row's content */
                            $(el).append($('<div>').html(purgeText));

                        }
                    },
                    userName: {
                        label: "Users",
                        cell: "html",
                        editable: false,
                        sortable: function(e, attr, order) {
                            return function(left, right) {

                                // no-op
                                if (order == null) return 0;

                                var l = left.get(attr),
                                    r = right.get(attr),
                                    t;

                                // if descending order, swap left and right
                                if (order === 1) t = l, l = r, r = t;

                                // compare as usual
                                if (l === r) return 0;
                                else if (l < r) return -1;
                                return 1;
                            }
                        }
                    },
                    operation: {
                        label: "Operation",
                        cell: "String",
                        editable: false,
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                            fromRaw: function(rawValue, model) {
                                if (rawValue === "PURGE" && model.attributes.params) {
                                    var purgeLength = model.attributes.result.replace('[', '').replace(']', '').split(',').length;
                                    return purgeLength === 1 ? purgeLength + " entity purged." : purgeLength + " entities purged.";
                                } else {
                                    return "No entity purged.";
                                }

                            }
                        })
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
            onClickPurgeType: function(e, value) {
                this.onlyPurge = e.currentTarget.value === "Purge" ? true : false;
                this.getPurgeParam();
            },
            onClickPurgeEntity: function(e) {
                var that = this;
                require([
                    'modules/Modal', 'views/audit/AuditTableLayoutView', 'views/audit/CreateAuditTableLayoutView',
                ], function(Modal, AuditTableLayoutView, CreateAuditTableLayoutView) {
                    var obj = {
                            guid: $(e.target).text(),
                        },
                        modal = new Modal({
                            title: "Purged Entity Details : " + obj.guid,
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
    return PurgeAuditTableLayoutView;
});