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
    'collection/VTagList',
    'models/VTag',
    'utils/Messages',
    'utils/UrlLinks'
], function(require, Backbone, TagAttributeDetailLayoutViewTmpl, Utils, AddTagAttributeView, VTagList, VTag, Messages, UrlLinks) {
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
                events["click " + this.ui.cancelButton] = 'onCancelButtonClick';
                events["click " + this.ui.addAttrBtn] = 'onClickAddAttribute';
                events["click " + this.ui.addTagListBtn] = 'onClickAddTagBtn';
                events["click " + this.ui.editButton] = 'onEditButton';
                return events;
            },
            /**
             * intialize a new TagAttributeDetailLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'globalVent', 'tag', 'collection'));
            },
            bindEvents: function() {
                this.listenTo(this.collection, 'reset', function() {
                    this.model = this.collection.findWhere({ name: this.tag });
                    this.renderTagDetail();
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
                if (this.collection.models.length && !this.model) {
                    this.model = this.collection.findWhere({ name: this.tag });
                    this.renderTagDetail();
                }
                this.bindEvents();
                this.ui.saveButton.attr("disabled", "true");
                this.ui.publishButton.prop('disabled', true);
            },
            renderTagDetail: function() {
                var attributeData = "",
                    attributeDefs = this.model.get("attributeDefs");
                if (this.model.get("name")) {
                    this.ui.title.html('<span>' + this.model.get("name") + '</span>');
                }
                if (this.model.get("description")) {
                    this.ui.description.html(this.model.get("description"));
                }
                if (this.model.get("attributeDefs")) {
                    if (!_.isArray(attributeDefs)) {
                        attributeDefs = [attributeDefs];
                    }
                    _.each(attributeDefs, function(value, key) {
                        attributeData += '<span class="inputAttribute">' + value.name + '</span>';
                    });
                    this.ui.showAttribute.html(attributeData);
                }

            },
            onSaveButton: function(saveObject, message) {
                var that = this;
                this.model.saveTagAttribute(this.model.get('name'), {
                    data: JSON.stringify(saveObject),
                    success: function(model, response) {
                        that.model.set(model);
                        that.renderTagDetail();
                        Utils.notifySuccess({
                            content: message
                        });
                    },
                    error: function(model, response) {
                        if (response.responseJSON && response.responseJSON.error) {
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
                            var attributes = _.clone(that.model.get('attributeDefs'));
                            if (!_.isArray(attributes)) {
                                attributes = [attributes];
                            }
                            attributes.push({
                                "name": attributeName,
                                "typeName": "string",
                                "cardinality": "SINGLE",
                                "isUnique": false,
                                "indexable": true,
                                "isOptional":true
                            });
                            var saveData = _.extend(that.model.toJSON(), { 'attributeDefs': attributes });
                            that.onSaveButton(saveData, Messages.addAttributeSuccessMessage);
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
            textAreaChangeEvent: function(view, modal) {
                if (this.model.get('description') === view.ui.description.val()) {
                    modal.$el.find('button.ok').prop('disabled', true);
                } else {
                    modal.$el.find('button.ok').prop('disabled', false);
                }
            },
            onPublishClick: function(view) {
                var saveObj = _.extend(this.model.toJSON(), { 'description': view.ui.description.val() });
                this.onSaveButton(saveObj, Messages.updateTagDescriptionMessage);
                this.ui.description.show();
            },
            onEditButton: function(e) {
                var that = this;
                $(e.currentTarget).blur();
                require([
                    'views/tag/CreateTagLayoutView',
                    'modules/Modal'
                ], function(CreateTagLayoutView, Modal) {
                    var view = new CreateTagLayoutView({ 'tagCollection': that.collection, 'model': that.model, 'tag': that.tag });
                    var modal = new Modal({
                        title: 'Edit Tag',
                        content: view,
                        cancelText: "Cancel",
                        okText: 'Save',
                        allowCancel: true,
                    }).open();
                    view.ui.description.on('keyup', function(e) {
                        that.textAreaChangeEvent(view, modal);
                    });
                    modal.$el.find('button.ok').prop('disabled', true);
                    modal.on('ok', function() {
                        that.onPublishClick(view);
                    });
                    modal.on('closeModal', function() {
                        modal.trigger('cancel');
                    });
                });
            }
        });
    return TagAttributeDetailLayoutView;
});
