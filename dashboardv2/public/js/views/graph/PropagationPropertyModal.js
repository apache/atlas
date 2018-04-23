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
    'utils/Messages'
], function(require, PropagationPropertyModalViewTmpl, VRelationship, VEntity, Modal, Utils, UrlLinks, Messages) {
    'use strict';

    var PropogationPropertyModal = Backbone.Marionette.CompositeView.extend({
        template: PropagationPropertyModalViewTmpl,
        templateHelpers: function() {},
        regions: {},
        ui: {
            propagationOptions: '[data-id="propagationOptions"]',
            edgeDetailName: '[data-id="edgeDetailName"]',
            propagationState: "[data-id='propagationState']",
            classificationClick: "[data-id='classificationClick']",
            editPropagationType: 'input[name="editPropagationType"]',
            PropagatedClassificationTable: "[data-id='PropagatedClassificationTable']"

        },
        events: function() {
            var events = {},
                that = this;
            events["change " + this.ui.propagationOptions] = function() {
                this.modalEdited = true;
                this.modal.$el.find('button.ok').attr("disabled", false);
            };
            events["click " + this.ui.editPropagationType] = function(e) {
                if (this.modalEdited === true) {
                    e.preventDefault();
                    that.notifyModal();
                }
            };
            events["change " + this.ui.editPropagationType] = function(e) {
                if (e.target.checked) {
                    this.showPropagatedClassificationTable();
                    this.viewType = "table";
                } else {
                    this.showEditPropagation();
                    this.viewType = "flow";
                }
            };
            events["click " + this.ui.classificationClick] = function(e) {
                var that = this,
                    url = "",
                    notifyObj = {
                        modal: true,
                        text: "Are you sure you want to navigate away from this page ?",
                        ok: function(argument) {
                            that.modal.trigger('cancel');
                            Utils.setUrl({
                                url: url,
                                mergeBrowserUrl: false,
                                trigger: true
                            });

                        },
                        cancel: function(argument) {}
                    };
                if (e.currentTarget.dataset.entityguid) {
                    url = '#!/tag/tagAttribute/' + e.currentTarget.dataset.entityguid;
                } else {
                    url = '#!/detailPage/' + e.currentTarget.dataset.guid + '?tabActive=lineage';
                }
                Utils.notifyConfirm(notifyObj);
            };
            events["change " + this.ui.propagationState] = function(e) {
                this.modalEdited = true;
                this.modal.$el.find('button.ok').attr("disabled", false);
                var entityguid = e.currentTarget.dataset.entityguid;
                if (e.target.checked) {
                    this.propagatedClassifications = _.reject(this.propagatedClassifications, function(val, key) {
                        if (val.entityGuid == entityguid) {
                            that.blockedPropagatedClassifications.push(val);
                            return true;
                        }
                    });
                } else {
                    this.blockedPropagatedClassifications = _.reject(this.blockedPropagatedClassifications, function(val, key) {
                        if (val.entityGuid == entityguid) {
                            that.propagatedClassifications.push(val);
                            return true;
                        }
                    });
                }
            };
            return events;
        },
        /**
         * intialize a new PropogationPropertyModal Layout
         * @constructs
         */
        initialize: function(options) {
            _.extend(this, _.pick(options, 'edgeInfo', 'relationshipId', 'lineageData', 'apiGuid', 'detailPageFetchCollection'));
            this.entityModel = new VRelationship();
            this.VEntityModel = new VEntity();
            this.modalEdited = false;
            this.viewType = 'flow';
            var that = this,
                modalObj = {
                    title: 'Edit Propagation Flow',
                    content: this,
                    okText: 'Update',
                    okCloses: false,
                    cancelText: "Cancel",
                    mainClass: 'modal-lg',
                    allowCancel: true,
                };

            this.modal = new Modal(modalObj)
            this.modal.open();
            this.modal.$el.find('button.ok').attr("disabled", true);
            this.on('ok', function() {
                that.updateRelation();
            });
            this.on('closeModal', function() {
                this.modal.trigger('cancel');
            });
            this.updateEdgeView(this.edgeInfo);
        },

        onRender: function() {},
        updateEdgeView: function(options) {
            var obj = options.obj,
                fromEntity = this.lineageData.guidEntityMap[obj.fromEntityId],
                toEntity = this.lineageData.guidEntityMap[obj.toEntityId];
            if (fromEntity && toEntity) {
                this.ui.edgeDetailName.html(fromEntity.displayText + " <span class='navigation-font'><i class='fa fa-long-arrow-right fa-color'></i></span> " + toEntity.displayText);
            }
            if (obj && obj.relationshipId) {
                this.showLoader();
                this.getEdgeEntity({ id: obj.relationshipId, from: fromEntity, to: toEntity });
            }
        },
        getPropagationFlow: function(options) {
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
        getEdgeEntity: function(options) {
            var that = this,
                id = options.id,
                from = options.from,
                to = options.to,
                icon = {
                    0: 'fa-long-arrow-right',
                    1: 'fa-long-arrow-left',
                    2: 'fa-arrows-h'
                };
            _.each(icon, function(value, key) {
                that.ui.propagationOptions.find('li label')[key].innerHTML = from.typeName + ' <i class="fa-color fa ' + value + '"></i> ' + to.typeName;
            });
            if (id === this.ui.propagationOptions.attr("entity-id")) {
                return;
            }
            this.ui.propagationOptions.attr("entity-id", id);
            if (this.apiGuid[id]) {
                this.hideLoader();
                this.$("input[name='propagateRelation']").removeAttr('checked');
                this.$("input[name='propagateRelation'][value=" + that.getPropagationFlow({
                    "relationshipData": this.apiGuid[id],
                    "graphData": options
                }) + "]").prop('checked', true);
                this.showBlockedClassificationTable(this.apiGuid[id]);
                this.getEntityNameForClassification(this.apiGuid[id]);
            } else {
                if (this.edgeCall && this.edgeCall.readyState != 4) {
                    this.edgeCall.abort();
                }
                this.edgeCall = this.entityModel.getRelationship(id, {
                    success: function(relationshipData) {
                        if (relationshipData) {
                            that.apiGuid[relationshipData.guid] = relationshipData;
                            that.$("input[name='propagateRelation']").removeAttr('checked');
                            that.$("input[name='propagateRelation'][value=" + that.getPropagationFlow({
                                "relationshipData": relationshipData,
                                "graphData": options
                            }) + "]").prop('checked', true);
                            that.showBlockedClassificationTable(relationshipData);
                            that.getEntityNameForClassification(relationshipData);
                            that.hideLoader({ buttonDisabled: true });
                        }
                    },
                    cust_error: function() {
                        that.hideLoader();
                    }
                });
            }
        },
        updateRelation: function() {
            var that = this,
                entityId = that.ui.propagationOptions.attr('entity-id'),
                PropagationValue = this.$("input[name='propagateRelation']:checked").val(),
                relationshipProp = {};
            if (PropagationValue === this.ui.propagationOptions.attr("propagation")) {
                return;
            }
            this.ui.propagationOptions.attr("propagation", PropagationValue);
            if (this.viewType == "flow") {
                relationshipProp = {
                    "propagateTags": that.getPropagationFlow({
                        "relationshipData": _.extend(that.apiGuid[entityId], { 'propagateTags': PropagationValue }),
                        "graphData": { from: { guid: this.edgeInfo.obj.fromEntityId } }
                    })
                }
            } else {
                relationshipProp = {
                    "blockedPropagatedClassifications": _.uniq(that.blockedPropagatedClassifications, function(val, key) {
                        return val.entityGuid;
                    }),
                    "propagatedClassifications": _.uniq(that.propagatedClassifications, function(val, key) {
                        return val.entityGuid;
                    })
                };
            }
            this.showLoader();
            this.entityModel.saveRelationship({
                data: JSON.stringify(_.extend(that.apiGuid[entityId], relationshipProp)),
                success: function(relationshipData) {
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
                cust_error: function() {
                    that.hideLoader();
                }
            });
        },
        showBlockedClassificationTable: function(options) {
            var that = this,
                propagationStringValue = "",
                classificationTableValue = "";
            this.blockedPropagatedClassifications = _.isUndefined(options.blockedPropagatedClassifications) ? [] : options.blockedPropagatedClassifications;
            this.propagatedClassifications = _.isUndefined(options.propagatedClassifications) ? [] : options.propagatedClassifications;
            _.each(this.blockedPropagatedClassifications, function(val, key) {
                propagationStringValue += "<tr><td class='html-cell string-cell renderable text-center w30'><a data-id='classificationClick' title='' data-entityGuid=" + val.entityGuid + ">" + val.typeName + "</a></td><td class='html-cell string-cell renderable text-center' data-tr-entityGuid=" + val.entityGuid + "></td><td class='html-cell string-cell renderable text-center w30'><input type='checkbox' checked data-id='propagationState' data-entityGuid=" + val.entityGuid + " class='input'></td></tr>";
            });
            _.each(this.propagatedClassifications, function(val, key) {
                propagationStringValue += "<tr><td class='html-cell string-cell renderable text-center w30'><a data-id='classificationClick' title='' data-entityGuid=" + val.entityGuid + ">" + val.typeName + "</a></td><td class='html-cell string-cell renderable text-center' data-tr-entityGuid=" + val.entityGuid + "></td><td class='html-cell string-cell renderable text-center w30'><input type='checkbox' data-id='propagationState' data-entityGuid=" + val.entityGuid + " class='input'></td></tr>";
            });

            classificationTableValue = "<table class='attriTable'><caption>Block Propagatation Table</caption><tr><th class='html-cell string-cell renderable w30'>Classification</th><th class='html-cell string-cell renderable'>Entity Name</th><th class='html-cell string-cell renderable w30'>Block Propagatation</th>" + propagationStringValue + "</table>";

            this.ui.PropagatedClassificationTable.append(_.isEmpty(propagationStringValue) ? "No Records Found." : classificationTableValue);
        },
        getEntityNameForClassification: function(options) {
            var that = this,
                allEntityGuid = _.pluck(_.union(options.blockedPropagatedClassifications, options.propagatedClassifications, function(val, key) {
                    return val.entityGuid;
                }), 'entityGuid'),
                apiCall = allEntityGuid.length;
            _.map(allEntityGuid, function(entityGuid, key) {
                that.VEntityModel.getEntity(entityGuid, {
                    success: function(data) {
                        var entityNameWithType = Utils.getName(data['entity']) + ' ( ' + data['entity'].typeName + ' )',
                            link = "<a data-id='classificationClick' title='' data-guid=" + data['entity'].guid + ">" + entityNameWithType + "</a>";
                        that.ui.PropagatedClassificationTable.find('[data-tr-entityGuid=' + entityGuid + ']').html(link);
                    },
                    complete: function() {
                        apiCall -= 1;
                        if (apiCall == 0) {
                            that.hideLoader({ buttonDisabled: true });
                        }
                    },
                    cust_error: function() {
                        that.hideLoader({ buttonDisabled: true });
                    }
                });
            });
        },
        showLoader: function() {
            this.modal.$el.find('button.ok').attr("disabled", true);
            this.$('.overlay').removeClass('hide').addClass('show');
        },
        hideLoader: function(options) {
            var buttonDisabled = options && options.buttonDisabled;
            this.modal.$el.find('button.ok').attr("disabled", buttonDisabled ? buttonDisabled : false);
            this.$('.overlay').removeClass('show').addClass('hide');
        },
        notifyModal: function(options) {
            var that = this,
                notifyObj = {
                    modal: true,
                    text: "It looks like you have been edited something. If you leave before saving, your changes will be lost.",
                    ok: function(argument) {
                        that.viewType = that.ui.editPropagationType.is(":checked") ? "flow" : "table";
                        that.ui.editPropagationType.prop("checked", that.viewType === "flow" ? false : true).trigger("change");
                        that.modal.$el.find('button.ok').attr("disabled", true);
                    },
                    cancel: function(argument) {
                        that.viewType = that.ui.editPropagationType.is(":checked") ? "table" : "flow";
                    }
                };
            Utils.notifyConfirm(notifyObj);
        },
        showEditPropagation: function() {
            this.$('.editPropagation').show();
            this.$('.propagatedClassificationTable').hide();
            this.modal.$el.find('.modal-title').text("Edit Propagation Flow");
        },
        showPropagatedClassificationTable: function() {
            this.$('.editPropagation').hide();
            this.$('.propagatedClassificationTable').show();
            this.modal.$el.find('.modal-title').text("Block Propagation Table");
        }

    });
    return PropogationPropertyModal;
});