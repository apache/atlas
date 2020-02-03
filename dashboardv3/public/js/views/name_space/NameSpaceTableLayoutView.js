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
    'hbs!tmpl/name_space/NameSpaceTableLayoutView_tmpl',
    'collection/VEntityList',
    'collection/VTagList',
    'models/VSearch',
    'utils/Utils',
    'utils/Messages',
    'utils/Enums',
    'utils/UrlLinks',
    'utils/CommonViewFunction'
], function(require, Backbone, NameSpaceTableLayoutView_tmpl, VEntityList, VTagList, VSearch, Utils, Messages, Enums, UrlLinks, CommonViewFunction) {
    'use strict';

    var NameSpaceTableLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends NameSpaceTableLayoutView */
        {
            _viewName: 'NameSpaceTableLayoutView',

            template: NameSpaceTableLayoutView_tmpl,

            /** Layout sub regions */
            regions: {
                RNameSpaceTableLayoutView: "#r_nameSpaceTableLayoutView",
                RModal: "#r_modal"
            },

            /** ui selector cache */
            ui: {
                namespaceAttrPage: "[data-id='namespaceAttrPage']",
                namespaceAttrPageTitle: "[data-id='namespaceAttrPageTitle']",
                namespaceDetailPage: "[data-id='namespaceDetailPage']",
                auditCreate: "[data-id='auditCreate']",
                pageRecordText: "[data-id='pageRecordText']",
                activePage: "[data-id='activePage']",
                createNameSpace: "[data-id='createNameSpace']",
                attributeEdit: "[data-id='attributeEdit']",
                addAttribute: '[data-id="addAttribute"]',
                namespaceAttrPageOk: '[data-id="namespaceAttrPageOk"]',
                colManager: "[data-id='colManager']",
                deleteNamespace: '[data-id="deleteNamespace"]',
                namespaceAttrFontLoader: '.namespace-attr-fontLoader',
                namespaceAttrTableOverlay: '.namespace-attr-tableOverlay'

            },
            /** ui events hash */
            events: function() {
                var events = {},
                    that = this;
                events["click " + this.ui.createNameSpace] = "onClickCreateNamespace";
                events["click " + this.ui.addAttribute] = "onEditAttr";
                events["click " + this.ui.attributeEdit] = "onEditAttr";
                events["click " + this.ui.deleteNamespace] = function(e) {
                    that.guid = e.target.dataset.guid;
                    that.deleteNamespaceElement();
                };
                return events;
            },
            /**
             * intialize a new NameSpaceTableLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'guid', 'entity', 'entityName', 'attributeDefs', 'typeHeaders', 'nameSpaceCollection', 'nameSpaceAttr', 'selectedNameSpace'));
                this.limit = 10;
                this.offset = 0;
                this.pervOld = [];
                this.onlyPurge = true;
                this.newAttr = false;
                this.commonTableOptions = {
                    collection: this.nameSpaceCollection,
                    includeFilter: false,
                    includePagination: true,
                    includeFooterRecords: true,
                    includePageSize: true,
                    includeGotoPage: true,
                    includeAtlasTableSorting: true,
                    includeTableLoader: true,
                    includeColumnManager: true,
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
                this.currPage = 1;
                this.isFilters = null;
                this.guid = null;
                this.showDetails = true; // toggle between sttribute page and detail page
            },
            onRender: function() {
                this.toggleNamespaceDetailsAttrView();
                $.extend(this.nameSpaceCollection.queryParams, { count: this.limit });
                this.nameSpaceCollection.fullCollection.sort({ silent: true });
                this.renderTableLayoutView();
                this.$('.tableOverlay').hide();
                this.$('.auditTable').show(); // Only for first time table show because we never hide after first render.
                this.nameSpaceCollection.comparator = function(model) {
                    return -model.get('timestamp');
                }
            },
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
            bindEvents: function() {},
            loaderStatus: function(isActive) {
                var that = this;
                if (isActive) {
                    that.$('.namespace-attr-tableOverlay').show();
                    that.$('.namespace-attr-fontLoader').show();
                } else {
                    that.$('.namespace-attr-tableOverlay').hide();
                    that.$('.namespace-attr-fontLoader').hide();
                }
            },
            onEditAttr: function(e) {
                var that = this,
                    isAttrEdit = e.target.dataset && e.target.dataset.id === 'attributeEdit' ? true : false,
                    guid = e.target.dataset && e.target.dataset.guid ? e.target.dataset.guid : null,
                    selectedNamespace = that.nameSpaceCollection.fullCollection.findWhere({ guid: guid }),
                    attrributes = selectedNamespace ? selectedNamespace.get('attributeDefs') : null,
                    attrName = e.target.dataset.name ? e.target.dataset.name : null,
                    attrDetails = { name: attrName };
                if (selectedNamespace) {
                    that.ui.namespaceAttrPageOk.text("Save");
                    that.newAttr = e.target && e.target.dataset.action === "createAttr" ? true : false;
                    that.guid = guid;
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

                    that.showDetails = false;
                    that.toggleNamespaceDetailsAttrView();
                    that.ui.namespaceAttrPageOk.attr('data-action', e.target.dataset.id);
                    require(["views/name_space/CreateNameSpaceLayoutView"], function(CreateNameSpaceLayoutView) {
                        that.view = new CreateNameSpaceLayoutView({
                            onEditCallback: function() {
                                that.nameSpaceCollection.fullCollection.sort({ silent: true });
                                that.renderTableLayoutView();

                            },
                            onUpdateNamespace: function() {
                                enumDefCollection.fetch({ reset: true });
                                that.showDetails = true;
                                that.toggleNamespaceDetailsAttrView();
                            },
                            parent: that.$el,
                            tagCollection: that.nameSpaceCollection,
                            enumDefCollection: enumDefCollection,
                            isAttrEdit: isAttrEdit,
                            typeHeaders: typeHeaders,
                            attrDetails: attrDetails,
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
            onClickCreateNamespace: function(e) {
                var that = this,
                    isNewNameSpace = true;
                that.showDetails = false;
                that.ui.namespaceAttrPageOk.text("Create");
                that.ui.namespaceAttrPageOk.attr('data-action', 'createNamespace');
                that.ui.namespaceAttrPageTitle.text("Create Namespace");
                that.toggleNamespaceDetailsAttrView();
                require(["views/name_space/CreateNameSpaceLayoutView"], function(CreateNameSpaceLayoutView) {
                    that.view = new CreateNameSpaceLayoutView({
                        onUpdateNamespace: function() {
                            enumDefCollection.fetch({ reset: true });
                            that.showDetails = true;
                            that.toggleNamespaceDetailsAttrView();
                        },
                        tagCollection: that.nameSpaceCollection,
                        enumDefCollection: enumDefCollection,
                        typeHeaders: typeHeaders,
                        isNewNameSpace: isNewNameSpace,
                        nameSpaceCollection: nameSpaceCollection
                    });
                    that.RModal.show(that.view);
                });
            },
            renderTableLayoutView: function() {
                var that = this;
                require(['utils/TableLayout'], function(TableLayout) {
                    var cols = new Backgrid.Columns(that.getNamespaceTableColumns());
                    that.RNameSpaceTableLayoutView.show(new TableLayout(_.extend({}, that.commonTableOptions, {
                        columns: cols
                    })));
                    if (!(that.nameSpaceCollection.models.length < that.limit)) {
                        that.RNameSpaceTableLayoutView.$el.find('table tr').last().hide();
                    }

                });
            },
            getNamespaceTableColumns: function() {
                var that = this;
                return this.nameSpaceCollection.constructor.getTableCols({
                    attributeDefs: {
                        label: "",
                        cell: "html",
                        editable: false,
                        sortable: false,
                        cell: Backgrid.ExpandableCell,
                        fixWidth: "50",
                        accordion: false,
                        alwaysVisible: true,
                        expand: function(el, model) {
                            el.attr('colspan', '8');
                            var attrValues = '',
                                attrTable = $('table'),
                                attrTableBody = $('tbody'),
                                attrTableHeading = "<thead><td style='display:table-cell'><b>Attribute</b></td><td style='display:table-cell'><b>Type</b></td><td style='display:table-cell'><b>Applicable Type(s)</b></td><td style='display:table-cell'><b>Action</b></td></thead>",
                                attrRow = '',
                                attrTableDetails = '';
                            if (model.attributes && model.attributes.attributeDefs.length) {
                                _.each(model.attributes.attributeDefs, function(attrObj) {
                                    var applicableEntityTypes = '',
                                        typeName = attrObj.typeName;
                                    if (attrObj.options) {
                                        // attrEntityTypes = JSON.parse(attrObj.options.applicableEntityTypes).join(', ');
                                        var entityTypes = JSON.parse(attrObj.options.applicableEntityTypes);
                                        _.each(entityTypes, function(values) {
                                            applicableEntityTypes += '<label class="btn btn-action btn-xs btn-blue no-pointer">' + values + '</label>';
                                        })
                                    }
                                    if (typeName.includes('array')) {
                                        typeName = _.escape(typeName);
                                    }
                                    attrRow += "<tr> <td style='display:table-cell'>" + _.escape(attrObj.name) + "</td><td style='display:table-cell'>" + typeName + "</td><td style='display:table-cell'>" + applicableEntityTypes + "</td><td style='display:table-cell'> <div class='btn btn-action btn-sm' style='margin-left:0px;' data-id='attributeEdit' data-guid='" + model.get('guid') + "' data-name ='" + _.escape(attrObj.name) + "' data-action='attributeEdit' >Edit</div> </td></tr> ";
                                });
                                var purgeText = '<div class="row"><div class="col-sm-12 attr-details"><table style="padding: 50px;">' + attrTableHeading + attrRow + '</table></div></div>';
                                $(el).append($('<div>').html(purgeText));
                            } else {
                                var purgeText = '<div class="row"><div class="col-sm-12 attr-details"><h5 class="text-center"> No attributes to show.</h5></div></div>';
                                $(el).append($('<div>').html(purgeText));
                            }

                        }
                    },
                    name: {
                        label: "Name",
                        cell: "html",
                        editable: false,
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                            fromRaw: function(rawValue, model) {
                                return '<a title= "' + model.get('name') + '" href ="#!/administrator/namespace/' + model.get('guid') + '">' + model.get('name') + '</a>';
                            }
                        })
                    },
                    description: {
                        label: "Description",
                        cell: "html",
                        editable: false,
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                            fromRaw: function(rawValue, model) {
                                return model.get('description');
                            }
                        })
                    },
                    createdBy: {
                        label: "Created by",
                        cell: "html",
                        renderable: false,
                        editable: false,
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                            fromRaw: function(rawValue, model) {
                                return model.get('updatedBy');
                            }
                        })
                    },
                    createTime: {
                        label: "Created on",
                        cell: "html",
                        renderable: false,
                        editable: false,
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                            fromRaw: function(rawValue, model) {
                                return new Date(model.get('createTime'));
                            }
                        })
                    },
                    updatedBy: {
                        label: "Updated by",
                        cell: "html",
                        renderable: false,
                        editable: false,
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                            fromRaw: function(rawValue, model) {
                                return model.get('updatedBy');
                            }
                        })
                    },
                    updateTime: {
                        label: "Updated on",
                        cell: "html",
                        renderable: false,
                        editable: false,
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                            fromRaw: function(rawValue, model) {
                                return new Date(model.get('updateTime'));
                            }
                        })
                    },
                    tools: {
                        label: "Action",
                        cell: "html",
                        sortable: false,
                        editable: false,
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                            fromRaw: function(rawValue, model) {
                                return "<button type='button' data-id='addAttribute' data-guid='" + model.get('guid') + "'' title='' class='btn btn-action btn-xs ' style='margin-bottom: 10px;' data-action='createAttr' data-original-title='Add Namespace attribute'><i class='fa fa-plus'></i> Attributes</button>";
                                // "<button type='button' data-id='deleteNamespace' data-guid='" + model.get('guid') + "'' title='' class='btn btn-action btn-xs ' style='margin-bottom: 10px;' data-action='createAttr' data-original-title='Delete Namespace'><i class='fa fa-trash-o'></i> Delete</button>";
                            }
                        })
                    }
                }, this.nameSpaceCollection);
            },
            deleteNamespaceElement: function(nameSpaceName) {
                var that = this,
                    notifyObj = {
                        modal: true,
                        ok: function(argument) {
                            that.onNotifyDeleteOk();
                        },
                        cancel: function(argument) {}
                    };
                var text = "Are you sure you want to delete the namespace";
                notifyObj["text"] = text;
                Utils.notifyConfirm(notifyObj);
            },
            onNotifyDeleteOk: function(data) {
                var that = this,
                    deleteNamespaceData = that.nameSpaceCollection.fullCollection.findWhere({ guid: that.guid });
                // that.$('.position-relative .fontLoader').addClass('show');
                that.$('.tableOverlay').show();
                if (deleteNamespaceData) {
                    var nameSpaceName = deleteNamespaceData.get("name");
                    deleteNamespaceData.deleteNameSpace({
                        typeName: nameSpaceName,
                        success: function() {
                            Utils.notifySuccess({
                                content: "Namespace " + nameSpaceName + Messages.getAbbreviationMsg(false, 'deleteSuccessMessage')
                            });
                            that.nameSpaceCollection.fullCollection.remove(deleteNamespaceData);
                            that.nameSpaceCollection.fullCollection.sort({ silent: true });
                            that.renderTableLayoutView();
                            that.showDetails = true;
                            that.toggleNamespaceDetailsAttrView();
                            that.loaderStatus(false);
                        },
                        complete: function() {
                            that.$('.tableOverlay').hide();
                            that.$('.position-relative .fontLoader').removeClass('show');
                        }
                    });
                } else {
                    Utils.notifyError({
                        content: Messages.defaultErrorMessage
                    });
                }
            }
        });
    return NameSpaceTableLayoutView;
});