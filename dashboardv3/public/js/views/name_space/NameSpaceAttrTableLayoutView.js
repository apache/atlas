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
    'hbs!tmpl/name_space/NameSpaceAttrTableLayoutView_tmpl',
    'collection/VEntityList',
    'collection/VTagList',
    'models/VSearch',
    'utils/Utils',
    'utils/Messages',
    'utils/Enums',
    'utils/UrlLinks',
    'utils/CommonViewFunction'
], function(require, Backbone, NameSpaceAttrTableLayoutView_tmpl, VEntityList, VTagList, VSearch, Utils, Messages, Enums, UrlLinks, CommonViewFunction) {
    'use strict';

    var NameSpaceAttrTableLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends NameSpaceAttrTableLayoutView */
        {
            _viewName: 'NameSpaceAttrTableLayoutView',

            template: NameSpaceAttrTableLayoutView_tmpl,

            /** Layout sub regions */
            regions: {
                RNameSpaceAttrTableLayoutView: "#r_nameSpaceAttrTableLayoutView",
                RModal: "#r_modal"
            },

            /** ui selector cache */
            ui: {
                attributeEdit: "[data-id='attributeEdit']",
                addAttribute: '[data-id="addAttribute"]',
                namespaceAttrPage: "[data-id='namespaceAttrPage']",
                namespaceAttrPageTitle: "[data-id='namespaceAttrPageTitle']",
                namespaceDetailPage: "[data-id='namespaceDetailPage']",
            },
            /** ui events hash */
            events: function() {
                var events = {};
                events["click " + this.ui.attributeEdit] = "onEditAttr";
                events["click " + this.ui.addAttribute] = "onEditAttr";
                return events;
            },
            /**
             * intialize a new NameSpaceAttrTableLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'guid', 'entity', 'entityName', 'attributeDefs', 'typeHeaders', 'nameSpaceCollection', 'nameSpaceAttr', 'entityDefCollection'));
                this.commonTableOptions = {
                    collection: this.nameSpaceAttr,
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
                this.isFilters = null;
                this.showDetails = true;
            },
            onRender: function() {
                this.toggleNamespaceDetailsAttrView();
                if (this.nameSpaceCollection.models.length && !this.model) {
                    this.model = this.nameSpaceCollection.fullCollection.findWhere({ guid: this.guid });
                    Utils.showTitleLoader(this.$('.page-title .fontLoader'), this.$('.tagDetail'));
                    this.renderTableLayoutView();
                }
            },
            bindEvents: function() {},
            toggleNamespaceDetailsAttrView: function() {
                var that = this;
                if (that.showDetails) {
                    that.ui.namespaceAttrPage.hide();
                    that.ui.namespaceDetailPage.show();
                } else {
                    that.ui.namespaceAttrPage.show();
                    that.ui.namespaceDetailPage.hide();
                }
            },
            onEditAttr: function(e) {
                var that = this,
                    isAttrEdit = false,
                    selectedNamespace = that.nameSpaceCollection.fullCollection.findWhere({ guid: that.guid }),
                    attrributes = selectedNamespace ? selectedNamespace.get('attributeDefs') : null,
                    attrName = e.target.dataset.name ? e.target.dataset.name : null,
                    attrDetails = { name: attrName };
                if (e.target.dataset.action == 'attributeEdit') {
                    isAttrEdit = true
                }
                if (selectedNamespace) {
                    that.newAttr = isAttrEdit ? false : true;
                    _.each(attrributes, function(attrObj) {
                        if (attrObj.name === attrName) {
                            attrDetails.attrTypeName = attrObj.typeName;
                            if (attrObj.typeName.includes('array')) {
                                attrDetails.attrTypeName = attrObj.typeName.replace("array<", "").replace(">", "");
                                attrDetails.multiValued = true;
                            }
                            attrDetails.attrEntityType = attrObj.options && attrObj.options.applicableEntityTypes ? JSON.parse(attrObj.options.applicableEntityTypes) : null;
                            attrDetails.maxStrLength = attrObj.options && attrObj.options.maxStrLength ? attrObj.options.maxStrLength : null;
                            attrDetails.enumValues = attrObj.enumValues ? attrObj.enumValues : null;
                        }
                    });
                    this.showDetails = false;
                    that.toggleNamespaceDetailsAttrView();
                    require(["views/name_space/CreateNameSpaceLayoutView"], function(CreateNameSpaceLayoutView) {
                        that.view = new CreateNameSpaceLayoutView({
                            onEditCallback: function() {
                                enumDefCollection.fetch({ reset: true });
                                that.nameSpaceAttr.fullCollection.reset();
                                that.options.selectedNameSpace.fetch({
                                    complete: function(model, status) {
                                        that.nameSpaceAttr.fullCollection.add(model.responseJSON.attributeDefs);
                                    }
                                });
                            },
                            onUpdateNamespace: function() {
                                that.renderTableLayoutView();
                                that.showDetails = true;
                                that.toggleNamespaceDetailsAttrView();
                                that.entityDefCollection.fetch({ silent: true });
                            },
                            parent: that.$el,
                            tagCollection: that.nameSpaceCollection,
                            enumDefCollection: enumDefCollection,
                            isAttrEdit: isAttrEdit,
                            attrDetails: attrDetails,
                            typeHeaders: typeHeaders,
                            selectedNamespace: selectedNamespace,
                            nameSpaceCollection: nameSpaceCollection,
                            guid: that.guid,
                            isNewAttr: that.newAttr
                        });
                        if (isAttrEdit) {
                            that.ui.namespaceAttrPageTitle.text("Update Attribute of: " + selectedNamespace.get('name'));
                        } else {
                            that.ui.namespaceAttrPageTitle.text("Add Namespace Attribute for: " + selectedNamespace.get('name'));
                        }
                        that.RModal.show(that.view);
                    });
                }

            },
            renderTableLayoutView: function() {
                var that = this;
                require(['utils/TableLayout'], function(TableLayout) {
                    var cols = new Backgrid.Columns(that.getNamespaceTableColumns());
                    that.RNameSpaceAttrTableLayoutView.show(new TableLayout(_.extend({}, that.commonTableOptions, {
                        columns: cols
                    })));
                    if (!(that.nameSpaceAttr.models.length < that.limit)) {
                        // that.RNameSpaceAttrTableLayoutView.$el.find('table tr').last().hide();
                    }
                });
            },
            getNamespaceTableColumns: function() {
                var that = this;
                return this.nameSpaceAttr.constructor.getTableCols({
                    name: {
                        label: "Attribute Name",
                        cell: "html",
                        editable: false,
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                            fromRaw: function(rawValue, model) {
                                return model.get('name');
                            }
                        })
                    },
                    typeName: {
                        label: "Type Name",
                        cell: "html",
                        editable: false,
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                            fromRaw: function(rawValue, model) {
                                return _.escape(model.get('typeName'));
                            }
                        })
                    },
                    options: {
                        label: "Entity Type(s)",
                        cell: "html",
                        editable: false,
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                            fromRaw: function(rawValue, model) {
                                var applicableEntityTypes = '',
                                    attrEntityTypes = JSON.parse(model.get('options').applicableEntityTypes);
                                _.each(attrEntityTypes, function(values) {
                                    applicableEntityTypes += '<label class="btn btn-action btn-xs btn-blue no-pointer">' + values + '</label>';
                                });
                                return applicableEntityTypes;
                            }
                        })
                    },
                    tool: {
                        label: "Action",
                        cell: "html",
                        editable: false,
                        sortable: false,
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                            fromRaw: function(rawValue, model) {
                                return '<div class="btn btn-action btn-sm" data-id="attributeEdit" data-action="attributeEdit" data-name="' + model.get('name') + '">Edit</div>';
                            }
                        })
                    }
                }, this.nameSpaceAttr);
            }
        });
    return NameSpaceAttrTableLayoutView;
});