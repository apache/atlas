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
    'hbs!tmpl/graph/PropagationPropertyModalView_tmpl',
    'models/VRelationship',
    'models/VEntity',
    'modules/Modal',
    'utils/Utils',
    'utils/UrlLinks',
    'utils/Messages',
    'collection/VRelationshipList',
], function (require, PropagationPropertyModalViewTmpl, VRelationship, VEntity, Modal, Utils, UrlLinks, Messages, VRelationshipList) {
    'use strict';

    var PropogationPropertyModal = Backbone.Marionette.LayoutView.extend({
        template: PropagationPropertyModalViewTmpl,
        templateHelpers: function () { },
        regions: {
            RPropagatedClassificationTable: "#r_PropagatedClassificationTable"
        },
        ui: {
            propagationOptions: '[data-id="propagationOptions"]',
            edgeDetailName: '[data-id="edgeDetailName"]',
            propagationState: "[data-id='propagationState']",
            entityClick: "[data-id='entityClick']",
            editPropagationType: 'input[name="editPropagationType"]',
            PropagatedClassificationTable: "[data-id='PropagatedClassificationTable']"

        },
        events: function () {
            var events = {},
                that = this;
            events["change " + this.ui.propagationOptions] = function () {
                this.modalEdited = true;
                this.modal.$el.find('button.ok').attr("disabled", false);
            };
            events["click " + this.ui.editPropagationType] = function (e) {
                if (this.modalEdited === true) {
                    e.preventDefault();
                    that.notifyModal();
                }
            };
            events["change " + this.ui.editPropagationType] = function (e) {
                if (e.target.checked) {
                    this.showPropagatedClassificationTable();
                    this.viewType = "table";
                } else {
                    this.showEditPropagation();
                    this.viewType = "flow";
                }
            };
            events["click " + this.ui.entityClick] = function (e) {
                var that = this,
                    url = "",
                    notifyObj = {
                        modal: true,
                        text: "Are you sure you want to navigate away from this page ?",
                        ok: function (argument) {
                            that.modal.trigger('cancel');
                            Utils.setUrl({
                                url: url,
                                mergeBrowserUrl: false,
                                trigger: true
                            });

                        },
                        cancel: function (argument) { }
                    },
                    $entityName = $(e.currentTarget),
                    url = $entityName.hasClass('entityName') ? '#!/detailPage/' + $entityName[0].dataset.entityguid + '?tabActive=lineage' : '#!/tag/tagAttribute/' + $entityName.data('name');
                Utils.notifyConfirm(notifyObj);
            };
            events["change " + this.ui.propagationState] = function (e) {
                this.modalEdited = true;
                this.modal.$el.find('button.ok').attr("disabled", false);
                var $el = $(e.currentTarget),
                    entityguid = $el[0].dataset.entityguid,
                    classificationName = $el[0].dataset.typename,
                    updateClassificationLists = function (fromClassifications, toClassifications) {
                        return _.reject(fromClassifications, function (val, key) {
                            if (val.entityGuid == entityguid && classificationName == val.typeName) {
                                toClassifications.push(val);
                                return true;
                            }
                        });
                    };

                if (e.target.checked) {
                    this.propagatedClassifications = updateClassificationLists(this.propagatedClassifications, this.blockedPropagatedClassifications)
                } else {
                    this.blockedPropagatedClassifications = updateClassificationLists(this.blockedPropagatedClassifications, this.propagatedClassifications)
                }
            };
            return events;
        },
        /**
         * intialize a new PropogationPropertyModal Layout
         * @constructs
         */
        initialize: function (options) {
            _.extend(this, _.pick(options, 'edgeInfo', 'relationshipId', 'lineageData', 'apiGuid', 'detailPageFetchCollection'));
            this.entityModel = new VRelationship();
            this.VEntityModel = new VEntity();
            this.relationShipCollection = new VRelationshipList();
            this.modalEdited = false;
            this.viewType = 'flow';
            var that = this,
                modalObj = {
                    title: 'Classification Propagation Control',
                    content: this,
                    okText: 'Update',
                    okCloses: false,
                    cancelText: "Cancel",
                    mainClass: 'modal-lg',
                    allowCancel: true,
                };
            this.commonTableOptions = {
                collection: this.relationShipCollection,
                includeFilter: false,
                includePagination: false,
                includeFooterRecords: false,
                includePageSize: false,
                includeGotoPage: false,
                includeAtlasTableSorting: false,
                gridOpts: {
                    className: "table table-hover backgrid table-quickMenu",
                    emptyText: 'No records found!'
                },
                filterOpts: {},
                paginatorOpts: {}
            };
            this.modal = new Modal(modalObj)
            this.modal.open();
            this.modal.$el.find('button.ok').attr("disabled", true);
            this.on('ok', function () {
                that.updateRelation();
            });
            this.on('closeModal', function () {
                this.modal.trigger('cancel');
            });
            this.updateEdgeView(this.edgeInfo);
        },

        onRender: function () { },
        renderTableLayoutView: function () {
            var that = this;

            require(['utils/TableLayout'], function (TableLayout) {
                var cols = new Backgrid.Columns(that.getSchemaTableColumns());
                that.RPropagatedClassificationTable.show(new TableLayout(_.extend({}, that.commonTableOptions, {
                    columns: cols
                })));
            });

        },
        getSchemaTableColumns: function (options) {
            var that = this;

            return this.relationShipCollection.constructor.getTableCols({
                typeName: {
                    label: "Classification",
                    cell: "html",
                    editable: false,
                    sortable: false,
                    formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                        fromRaw: function (rawValue, model) {
                            return "<a class='classificationName' data-id='entityClick' title='" + rawValue + "' data-name='" + rawValue + "''>" + rawValue + "</a>"

                        }
                    })
                },
                entityGuid: {
                    label: "Entity Name",
                    cell: "html",
                    editable: false,
                    sortable: false,
                    formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                        fromRaw: function (rawValue, model) {
                            var guid = rawValue,
                                entityObj = that.apiGuid[that.relationshipId].referredEntities[guid],
                                name = guid;
                            if (entityObj) {
                                name = Utils.getName(entityObj) + " (" + entityObj.typeName + ")";
                            }
                            return "<a class='entityName' data-id='entityClick' data-entityguid =" + guid + ">" + name + "</a>";
                        }
                    })
                },
                fromBlockClassification: {
                    label: "Block Propagation",
                    cell: "html",
                    fixWidth: "150",
                    editable: false,
                    sortable: false,
                    formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                        fromRaw: function (rawValue, model) {

                            return "<div class='text-center'><input type='checkbox' " + (rawValue ? "checked" : "") + " data-entityguid ='" + model.attributes.entityGuid + "' + data-typeName ='" + model.attributes.typeName + "' data-id='propagationState' class='input'></div>"
                        }
                    })
                },

            },
                this.relationShipCollection);
        },
        updateEdgeView: function (options) {
            var obj = options,
                fromEntity = this.lineageData.guidEntityMap[obj.fromEntityId],
                toEntity = this.lineageData.guidEntityMap[obj.toEntityId];
            if (fromEntity && toEntity) {
                this.ui.edgeDetailName.html(_.escape(fromEntity.displayText) + " <span class='navigation-font'><i class='fa fa-long-arrow-right fa-color'></i></span> " + _.escape(toEntity.displayText));
            }
            if (obj && obj.relationshipId) {
                this.showLoader();
                this.getEdgeEntity({ id: obj.relationshipId, from: fromEntity, to: toEntity });
            }
        },
        getPropagationFlow: function (options) {
            var relationshipData = options.relationshipData,
                graphData = options.graphData,
                propagateTags = relationshipData.propagateTags;
            if (relationshipData.end1) {
                if (relationshipData.end1.guid == graphData.from.guid || propagateTags == "BOTH" || propagateTags == "NONE") {
                    return propagateTags;
                } else {
                    return propagateTags == "ONE_TO_TWO" ? "TWO_TO_ONE" : "ONE_TO_TWO";
                }
            } else {
                return propagateTags;
            }
        },
        getEdgeEntity: function (options) {
            var that = this,
                id = options.id,
                from = options.from,
                to = options.to,
                enableOtherFlow = function (relationshipObj) {
                    var isTwoToOne = false;
                    if (relationshipObj.propagateTags == "BOTH") {
                        that.ui.propagationOptions.find('.both').show();
                    } else {
                        that.ui.propagationOptions.find('.both').hide();
                        if (that.edgeInfo.fromEntityId != relationshipObj.end1.guid && relationshipObj.propagateTags == "ONE_TO_TWO") {
                            isTwoToOne = true;
                        } else if (that.edgeInfo.fromEntityId == relationshipObj.end1.guid && relationshipObj.propagateTags == "TWO_TO_ONE") {
                            isTwoToOne = true;
                        }
                        if (isTwoToOne) {
                            that.ui.propagationOptions.find('.TWO_TO_ONE').show();
                        } else {
                            that.ui.propagationOptions.find('.TWO_TO_ONE').hide();
                        }
                    }
                },
                updateValue = function (relationshipData) {
                    var relationshipObj = relationshipData.relationship;
                    if (relationshipObj) {
                        that.$("input[name='propagateRelation'][value=" + that.getPropagationFlow({
                            "relationshipData": relationshipObj,
                            "graphData": options
                        }) + "]").prop('checked', true);
                        enableOtherFlow(relationshipObj);
                        that.showBlockedClassificationTable(relationshipData);
                        that.hideLoader({ buttonDisabled: true });
                    }
                }
            this.ui.propagationOptions.find('li label>span.fromName').text(from.typeName);
            this.ui.propagationOptions.find('li label>span.toName').text(to.typeName);

            if (id === this.ui.propagationOptions.attr("entity-id")) {
                return;
            }
            this.ui.propagationOptions.attr("entity-id", id);
            if (this.apiGuid[id]) {
                updateValue(this.apiGuid[id]);
            } else {
                if (this.edgeCall && this.edgeCall.readyState != 4) {
                    this.edgeCall.abort();
                }
                this.edgeCall = this.entityModel.getRelationship(id, {
                    success: function (relationshipData) {
                        that.apiGuid[relationshipData.relationship.guid] = relationshipData;
                        updateValue(relationshipData);
                    },
                    cust_error: function () {
                        that.hideLoader();
                    }
                });
            }
        },
        updateRelation: function () {
            var that = this,
                entityId = that.ui.propagationOptions.attr('entity-id'),
                PropagationValue = this.$("input[name='propagateRelation']:checked").val(),
                relationshipProp = {};
            this.ui.propagationOptions.attr("propagation", PropagationValue);
            if (this.viewType == "flow") {
                relationshipProp = {
                    "propagateTags": that.getPropagationFlow({
                        "relationshipData": _.extend({}, this.apiGuid[entityId].relationship, { 'propagateTags': PropagationValue }),
                        "graphData": { from: { guid: this.edgeInfo.fromEntityId } }
                    })
                }
            } else {
                relationshipProp = {
                    "blockedPropagatedClassifications": this.blockedPropagatedClassifications,
                    "propagatedClassifications": this.propagatedClassifications
                };
            }
            this.showLoader();
            this.entityModel.saveRelationship({
                data: JSON.stringify(_.extend({}, that.apiGuid[entityId].relationship, relationshipProp)),
                success: function (relationshipData) {
                    if (relationshipData) {
                        that.hideLoader({ buttonDisabled: true });
                        that.modal.trigger('cancel');
                        that.apiGuid[relationshipData.guid] = relationshipData;
                        that.detailPageFetchCollection();
                        Utils.notifySuccess({
                            content: "Propagation flow updated succesfully."
                        });
                    }
                },
                cust_error: function () {
                    that.hideLoader();
                }
            });
        },
        showBlockedClassificationTable: function (options) {
            var propagationData = [],
                relationship = options.relationship,
                updateClassification = function (classificationList, isChecked) {
                    _.each(classificationList, function (val, key) {
                        propagationData.push(_.extend(val, { fromBlockClassification: isChecked }));
                    });

                };
            this.blockedPropagatedClassifications = _.clone(relationship.blockedPropagatedClassifications) || [];
            this.propagatedClassifications = _.clone(relationship.propagatedClassifications) || [];
            updateClassification(this.blockedPropagatedClassifications, true);
            updateClassification(this.propagatedClassifications, false);
            this.relationShipCollection.fullCollection.reset(propagationData);
            this.renderTableLayoutView();

        },
        showLoader: function () {
            this.modal.$el.find('button.ok').showButtonLoader();
            this.$('.overlay').removeClass('hide').addClass('show');
        },
        hideLoader: function (options) {
            var buttonDisabled = options && options.buttonDisabled;
            this.modal.$el.find('button.ok').hideButtonLoader();
            this.modal.$el.find('button.ok').attr("disabled", buttonDisabled ? buttonDisabled : false);
            this.$('.overlay').removeClass('show').addClass('hide');
        },
        notifyModal: function (options) {
            var that = this,
                notifyObj = {
                    modal: true,
                    text: "It looks like you have edited something. If you leave before saving, your changes will be lost.",
                    ok: function (argument) {
                        that.viewType = that.ui.editPropagationType.is(":checked") ? "flow" : "table";
                        that.ui.editPropagationType.prop("checked", that.viewType === "flow" ? false : true).trigger("change");
                        that.modal.$el.find('button.ok').attr("disabled", true);
                    },
                    cancel: function (argument) {
                        that.viewType = that.ui.editPropagationType.is(":checked") ? "table" : "flow";
                    }
                };
            Utils.notifyConfirm(notifyObj);
        },
        showEditPropagation: function () {
            this.$('.editPropagation').show();
            this.$('.propagatedClassificationTable').hide();
        },
        showPropagatedClassificationTable: function () {
            this.$('.editPropagation').hide();
            this.$('.propagatedClassificationTable').show();
        }

    });
    return PropogationPropertyModal;
});