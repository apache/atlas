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
'hbs!tmpl/entity/EntityLabelDefineView_tmpl',
'models/VEntity',
'utils/Utils',
'utils/Messages',
'utils/Enums'
], function(require, Backbone, EntityLabelDefineView_tmpl, VEntity, Utils, Messages, Enums) {
'use strict';

    return Backbone.Marionette.LayoutView.extend({
        _viewName: 'REntityLabelDefineView',
        template: EntityLabelDefineView_tmpl,
        templateHelpers: function() {
            return {
                swapItem: this.swapItem,
                labels: this.labels,
                saveLabels: this.saveLabels,
                readOnlyEntity: this.readOnlyEntity
            };
        },
        ui: {
            addLabelOptions: "[data-id='addLabelOptions']",
            addLabels: "[data-id='addLabels']",
            saveLabels: "[data-id='saveLabels']"
        },
        events: function() {
            var events = {};
            events["change " + this.ui.addLabelOptions] = 'onChangeLabelChange';
            events["click " + this.ui.addLabels] = 'handleBtnClick';
            events["click " + this.ui.saveLabels] = 'saveUserDefinedLabels';
            return events;
        },
        initialize: function(options) {
            var self = this;
            _.extend(this, _.pick(options, 'entity'));
            this.swapItem = false, this.saveLabels = false;
            this.readOnlyEntity = Enums.entityStateReadOnly[this.entity.status]
            this.entityModel = new VEntity(this.entity);
            this.labels = this.entity.labels || [];
        },
        onRender: function() {
            this.populateLabelOptions();
        },
        bindEvents: function () {
        },
        populateLabelOptions: function() {
            var that = this,
            str = this.labels.map(function (label) {
                return "<option selected > "+ label +" </option>";
            });
            this.ui.addLabelOptions.html(str);
            this.ui.addLabelOptions.select2({
                placeholder: "Select Label",
                allowClear: true,
                tags: true,
                multiple: true
            });
        },
        onChangeLabelChange: function () {
            this.labels = this.ui.addLabelOptions.val();
        },
        handleBtnClick: function () {
            this.swapItem = !this.swapItem;
            this.saveLabels = this.swapItem === true ? true : false;
            this.render();
        },
        saveUserDefinedLabels: function() {
            var that = this;
            var entityJson = that.entityModel.toJSON();
            var payload = this.labels;
            that.entityModel.saveEntityLabels(entityJson.guid ,{
                data: JSON.stringify(payload),
                type: 'POST',
                success: function() {
                    var msg = entityJson.labels === undefined ? 'addSuccessMessage' : 'editSuccessMessage';
                    if (payload.length === 0) {
                        that.entityModel.unset('labels');
                    } else {
                        that.entityModel.set('labels', payload);
                    }
                    Utils.notifySuccess({
                        content: "User-defined labels " + Messages[msg]
                    });
                    that.swapItem = false;
                    that.saveLabels = false;
                    that.render();
                },
                error: function (e) {
                    that.ui.saveLabels && that.ui.saveLabels.length > 0 &&  that.ui.saveLabels[0].setAttribute("disabled", false);
                    Utils.notifySuccess({
                        content: e.message
                    });
                },
                complete: function () {
                    that.ui.saveLabels && that.ui.saveLabels.length > 0 && that.ui.saveLabels[0].setAttribute("disabled", false);
                    that.render();
                }
            });
        }
    });
});
