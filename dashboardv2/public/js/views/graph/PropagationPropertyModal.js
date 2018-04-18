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
    'modules/Modal',
    'utils/Utils',
    'utils/UrlLinks',
    'utils/Messages'
], function(require, PropagationPropertyModalViewTmpl, VRelationship, Modal, Utils, UrlLinks, Messages) {
    'use strict';

    var PropogationPropertyModal = Backbone.Marionette.CompositeView.extend({
        template: PropagationPropertyModalViewTmpl,
        templateHelpers: function() {},
        regions: {},
        ui: {
            propagationOptions: '[data-id="propagationOptions"]',
            edgeDetailName: '[data-id="edgeDetailName"]'
        },
        events: function() {
            var events = {};
            events["change " + this.ui.propagationOptions] = function() {
                this.modal.$el.find('button.ok').attr("disabled", false);
            };
            return events;
        },
        /**
         * intialize a new PropogationPropertyModal Layout
         * @constructs
         */
        initialize: function(options) {
            _.extend(this, _.pick(options, 'edgeInfo', 'relationshipId', 'lineageData', 'apiGuid'));
            this.entityModel = new VRelationship();
            var that = this,
                modalObj = {
                    title: 'Edit Propagation Flow',
                    content: this,
                    okText: 'Update',
                    okCloses: false,
                    cancelText: "Cancel",
                    mainClass: 'modal-md',
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

        onRender: function() {

        },
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
                PropagationValue = this.$("input[name='propagateRelation']:checked").val();
            if (PropagationValue === this.ui.propagationOptions.attr("propagation")) {
                return;
            }
            this.ui.propagationOptions.attr("propagation", PropagationValue);
            var relationshipProp = {
                "propagateTags": that.getPropagationFlow({
                    "relationshipData": _.extend(that.apiGuid[entityId], { 'propagateTags': PropagationValue }),
                    "graphData": { from: { guid: this.edgeInfo.obj.fromEntityId } }
                })
            };
            this.showLoader();
            this.entityModel.saveRelationship({
                data: JSON.stringify(_.extend(that.apiGuid[entityId], relationshipProp)),
                success: function(relationshipData) {
                    if (relationshipData) {
                        that.hideLoader({ buttonDisabled: true });
                        that.modal.trigger('cancel');
                        that.apiGuid[relationshipData.guid] = relationshipData;
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
        showLoader: function() {
            this.modal.$el.find('button.ok').attr("disabled", true);
            this.$('.overlay').removeClass('hide').addClass('show');
        },
        hideLoader: function(options) {
            var buttonDisabled = options && options.buttonDisabled;
            this.modal.$el.find('button.ok').attr("disabled", buttonDisabled ? buttonDisabled : false);
            this.$('.overlay').removeClass('show').addClass('hide');
        }
    });
    return PropogationPropertyModal;
});