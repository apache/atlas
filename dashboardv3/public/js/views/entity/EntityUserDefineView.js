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
'hbs!tmpl/entity/EntityUserDefineView_tmpl',
'views/entity/EntityUserDefineItemView',
'utils/CommonViewFunction',
'modules/Modal',
'models/VEntity',
'utils/Utils',
'utils/Enums'
], function(require, Backbone, EntityUserDefineView_tmpl, EntityUserDefineItemView, CommonViewFunction, Modal, VEntity, Utils, Enums) {
'use strict';

    return Backbone.Marionette.LayoutView.extend({
        _viewName: 'EntityUserDefineView',
        template: EntityUserDefineView_tmpl,
        templateHelpers: function() {
            return {
                customAttibutes: this.customAttibutes,
                readOnlyEntity : this.readOnlyEntity
            };
        },
        ui: {
            addAttr: "[data-id='addAttr']",
            editAttr: "[data-id='editAttr']",
            deleteAttr: "[data-id='deleteAttr']"
        },
        events: function() {
            var events = {};
            events["click " + this.ui.editAttr] = 'onEditAttrClick';
            return events;
        },
        initialize: function(options) {
            _.extend(this, _.pick(options, 'entity'));
            this.userDefineAttr = this.entity.customAttributes || [];
            this.editMode = false;
            this.readOnlyEntity = Enums.entityStateReadOnly[this.entity.status];
            this.entityModel = new VEntity(this.entity);
            this.generateTableFields();
        },
        onRender: function() {
        },
        bindEvents: {},
        customAtributesFunc: function() {

        },
        generateTableFields: function() {
            var that = this;
            this.customAttibutes = [];
            _.each(Object.keys(that.userDefineAttr), function(key, i) {
                that.customAttibutes.push({
                    key: key,
                    value: that.userDefineAttr[key]
                });
            });
        },
        onEditAttrClick: function (e) {
            this.editMode = true;
            var options = {items: this.customAttibutes, mode: true};
            var view = new EntityUserDefineItemView(options);
            var modalObj = {
                title: 'User-defined properties',
                content: view,
                okText: 'Save',
                okCloses: false,
                cancelText: "Cancel",
                mainClass: 'modal-md',
                allowCancel: true,
            };
           this.setAttributeModal(modalObj);
        },
        structureAttributes: function (list) {
            var obj={}
            list.map(function (o) {
                obj[o.key] = o.value;
            });
            return obj;
        },
        saveAttributes: function (list) {
            var that = this;
            var entityJson = that.entityModel.toJSON();
            var properties = that.structureAttributes(list);
            entityJson.customAttributes = properties;
            var payload = {entity: entityJson};
            that.entityModel.createOreditEntity({
                data: JSON.stringify(payload),
                type: 'POST',
                success: function() {
                    var msg = "User-defined properties updated successfully";
                    that.customAttibutes = list;
                    Utils.notifySuccess({
                        content: msg
                    });
                    that.modal && that.modal.trigger('cancel');
                    that.render();
                },
                error: function (e) {
                    that.editMode = false;
                    Utils.notifySuccess({
                        content: e.message
                    });
                    that.modal && that.modal.$el.find('button.ok').attr("disabled", false);
                },
                complete: function () {
                    that.modal && that.modal.$el.find('button.ok').attr("disabled", false);
                    that.editMode = false;
                }
            });
        },
        setAttributeModal: function(modalObj) {
            var self = this;
            this.modal = new Modal(modalObj);
            this.modal.open();
            this. modal.on('ok', function() {
                self.modal.$el.find('button.ok').attr("disabled", true);
                var list = self.modal.$el.find("[data-type]"),
                    keyMap = new Map(),
                    validation = true,
                    hasDup = [],
                    dataList = [];
                Array.prototype.push.apply(dataList, self.modal.options.content.items);
                for(var i = 0; i < list.length ; i++) {
                    var input = list[i],
                        type = input.dataset.type,
                        pEl = self.modal.$el.find(input.parentElement).find('p'),
                        classes = 'form-control',
                        val = input.value.trim();
                        pEl[0].innerText = "";

                    if (val === '') {
                        classes = 'form-control errorClass';
                        validation = false;
                        pEl[0].innerText = 'Required!';
                    } else {
                        if (input.tagName === 'INPUT') {
                            var duplicates = dataList.filter(function(c) {
                                return c.key === val;
                            });
                            if (keyMap.has(val) || duplicates.length > 1 ) {
                                classes = 'form-control errorClass';
                                hasDup.push('duplicate');
                                pEl[0].innerText = 'Duplicate key';
                            } else {
                                keyMap.set(val, val);
                            }
                        }
                    }
                    input.setAttribute('class', classes);
                }

                if (validation && hasDup.length === 0) {
                    self.saveAttributes(self.modal.options.content.items);
                } else {
                    self.modal.$el.find('button.ok').attr("disabled", false);
                }
            });
            this.modal.on('closeModal', function() {
                self.editMode = false;
                self.modal.trigger('cancel');
            });
        },
        enableModalButton: function () {
            var self = this;
            self.modal.$el.find('button.ok').attr("disabled", false);
        }
    });
});
