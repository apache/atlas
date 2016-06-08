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
    'hbs!tmpl/tag/TagAttributeDetailLayoutView_tmpl',
    'utils/Utils',
    'views/tag/AddTagAttributeView',
    'collection/VCommonList',
    'models/VTag',
    'utils/Messages'
], function(require, Backbone, TagAttributeDetailLayoutViewTmpl, Utils, AddTagAttributeView, VCommonList, VTag, Messages) {
    'use strict';

    var TagAttributeDetailLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends TagAttributeDetailLayoutView */
        {
            template: TagAttributeDetailLayoutViewTmpl,
            /** Layout sub regions */
            regions: {},
            /** ui selector cache */
            ui: {
                title: '[data-id="title"]',
                editButton: '[data-id="editButton"]',
                editBox: '[data-id="editBox"]',
                addAttrBtn: '[data-id="addAttrBtn"]',
                saveButton: "[data-id='saveButton']",
                showAttribute: "[data-id='showAttribute']",
                cancelButton: "[data-id='cancelButton']",
                addTagListBtn: '[data-id="addTagListBtn"]',
                addTagtext: '[data-id="addTagtext"]',
                addTagPlus: '[data-id="addTagPlus"]',
                description: '[data-id="description"]',
                descriptionTextArea: '[data-id="descriptionTextArea"]',
                publishButton: '[data-id="publishButton"]',
            },
            /** ui events hash */
            events: function() {
                var events = {};
                events["click " + this.ui.editButton] = function() {
                    this.ui.editButton.hide();
                    this.ui.description.hide();
                    this.ui.editBox.show();
                    this.ui.descriptionTextArea.focus();
                    this.ui.publishButton.prop('disabled', true);
                    if (this.ui.description.text().length) {
                        this.ui.descriptionTextArea.val(this.ui.description.text());
                    }
                };
                events["keyup " + this.ui.descriptionTextArea] = 'textAreaChangeEvent';
                events["click " + this.ui.cancelButton] = 'onCancelButtonClick';
                events["click " + this.ui.addAttrBtn] = 'onClickAddAttribute';
                events["click " + this.ui.addTagListBtn] = 'onClickAddTagBtn';
                events["click " + this.ui.publishButton] = 'onPublishClick';
                return events;
            },
            /**
             * intialize a new TagAttributeDetailLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'globalVent', 'tag', 'vent'));
                this.tagCollection = new VCommonList();
                this.tagCollection.url = "/api/atlas/types/" + this.tag;
                this.tagCollection.modelAttrName = "definition";
                this.tagCollection.fetch({ reset: true });
                this.bindEvents();
            },
            bindEvents: function() {
                this.listenTo(this.tagCollection, 'reset', function() {
                    var that = this,
                        attributeData = "";
                    _.each(this.tagCollection.models, function(attr) {
                        var traitTypes = attr.get("traitTypes");
                        if (traitTypes[0].typeDescription != null) {
                            var descriptionValue = traitTypes[0].typeDescription;
                            that.ui.description.html(descriptionValue);
                        }
                        _.each(traitTypes[0].attributeDefinitions, function(value, key) {
                            attributeData += '<span class="inputAttribute">' + value.name + '</span>';
                        });
                    });
                    if (attributeData.length) {
                        that.ui.addTagtext.hide();
                        that.ui.addTagPlus.show();
                    }
                    that.ui.showAttribute.html(attributeData);
                }, this);
                this.listenTo(this.tagCollection, 'error', function(error, response) {
                    if (response.responseJSON && response.responseJSON.error) {
                        Utils.notifyError({
                            content: response.responseJSON.error
                        });
                    }

                }, this);
            },
            onRender: function() {
                this.ui.title.html('<span>' + this.tag + '</span>');
                this.ui.saveButton.attr("disabled", "true");
                this.ui.publishButton.prop('disabled', true);
            },
            onSaveButton: function(saveObject, message) {
                var that = this,
                    tagModel = new VTag();
                tagModel.set(saveObject).save(null, {
                    type: "PUT",
                    success: function(model, response) {
                        that.tagCollection.fetch({ reset: true });
                        Utils.notifySuccess({
                            content: message
                        });
                    },
                    error: function(model, response) {
                        if (response.responseJSON && response.responseJSON.error) {
                            that.tagCollection.fetch({ reset: true });
                            Utils.notifyError({
                                content: response.responseJSON.error
                            });
                        }
                    }
                });
            },
            onClickAddTagBtn: function(e) {
                var that = this;
                require(['views/tag/AddTagAttributeView',
                        'modules/Modal'
                    ],
                    function(AddTagAttributeView, Modal) {
                        var view = new AddTagAttributeView(),
                            modal = new Modal({
                                title: 'Add Attribute',
                                content: view,
                                cancelText: "Cancel",
                                okText: 'Add',
                                allowCancel: true,
                            }).open();
                        modal.on('ok', function() {
                            var attributeName = $(view.el).find("input").val();
                            that.tagCollection.first().get('traitTypes')[0].attributeDefinitions.push({
                                "name": attributeName,
                                "dataTypeName": "string",
                                "multiplicity": "optional",
                                "isComposite": false,
                                "isUniquvar e": false,
                                "isIndexable": true,
                                "reverseAttributeName": null
                            });
                            that.onSaveButton(that.tagCollection.first().toJSON(), Messages.addAttributeSuccessMessage);
                        });
                        modal.on('closeModal', function() {
                            modal.trigger('cancel');
                        });
                    });
            },
            onCancelButtonClick: function() {
                this.ui.description.show();
                this.ui.editButton.show();
                this.ui.editBox.hide();
            },
            textAreaChangeEvent: function() {
                if (this.tagCollection.first().get('traitTypes')[0].typeDescription == this.ui.descriptionTextArea.val()) {
                    this.ui.publishButton.prop('disabled', true);
                } else {
                    this.ui.publishButton.prop('disabled', false);
                }
            },
            onPublishClick: function() {
                this.tagCollection.first().get('traitTypes')[0].typeDescription = this.ui.descriptionTextArea.val();
                this.onSaveButton(this.tagCollection.first().toJSON(), Messages.updateTagDescriptionMessage);
                this.ui.description.show();
                this.ui.editButton.show();
                this.ui.editBox.hide();
            }
        });
    return TagAttributeDetailLayoutView;
});
