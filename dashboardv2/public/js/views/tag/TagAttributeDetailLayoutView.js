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
                addTagListBtn: '[data-id="addTagListBtn"]',
                addTagtext: '[data-id="addTagtext"]',
                addTagPlus: '[data-id="addTagPlus"]',
                addTagBtn: '[data-id="addTagBtn"]',
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
                _.extend(this, _.pick(options, 'globalVent', 'tag', 'vent'));
                this.tagCollection = new VCommonList();
                this.tagCollection.url = "/api/atlas/types/" + this.tag;
                this.tagCollection.modelAttrName = "definition";
                this.fetchCollection();
                this.bindEvents();
            },
            bindEvents: function() {
                this.listenTo(this.tagCollection, 'reset', function() {
                    var that = this,
                        attributeData = "";
                    this.traitTypes = this.tagCollection.first().get("traitTypes")[0];
                    if (this.traitTypes.typeDescription != null) {
                        that.ui.description.text(this.traitTypes.typeDescription);
                        that.ui.title.html('<span>' + _.escape(this.traitTypes.typeName) + '</span>');
                    }
                    if (this.traitTypes.typeName != null) {
                        that.ui.title.text(this.traitTypes.typeName);
                    }
                    _.each(this.traitTypes.attributeDefinitions, function(value, key) {
                        attributeData += '<span class="inputAttribute">' + _.escape(value.name) + '</span>';
                    });
                    that.ui.showAttribute.html(attributeData);
                    Utils.hideTitleLoader(this.$('.fontLoader'), this.$('.tagDetail'));
                }, this);
                this.listenTo(this.tagCollection, 'error', function(error, response) {
                    this.$('.fontLoader').hide();
                    if (response.responseJSON && response.responseJSON.error) {
                        Utils.notifyError({
                            content: response.responseJSON.error
                        });
                    } else {
                        Utils.notifyError({
                            content: "Something went wrong"
                        });
                    }

                }, this);
            },
            fetchCollection: function() {
                this.tagCollection.fetch({ reset: true });
            },
            onRender: function() {
                Utils.showTitleLoader(this.$('.page-title .fontLoader'), this.$('.tagDetail'));
                this.ui.saveButton.attr("disabled", "true");
                this.ui.publishButton.prop('disabled', true);
            },
            onSaveButton: function(saveObject, message) {
                Utils.showTitleLoader(this.$('.page-title .fontLoader'), this.$('.tagDetail'));
                var that = this,
                    tagModel = new VTag();
                tagModel.set(saveObject).save(null, {
                    type: "PUT",
                    success: function(model, response) {
                        that.fetchCollection();
                        Utils.notifySuccess({
                            content: message
                        });
                    },
                    error: function(model, response) {
                        if (response.responseJSON && response.responseJSON.error) {
                            that.fetchCollection();
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
                            that.traitTypes.attributeDefinitions.push({
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
            textAreaChangeEvent: function(view, modal) {
                if (this.traitTypes.typeDescription == view.ui.description.val()) {
                    modal.$el.find('button.ok').prop('disabled', true);
                } else {
                    modal.$el.find('button.ok').prop('disabled', false);
                }
            },
            onPublishClick: function(view) {
                this.traitTypes.typeDescription = view.ui.description.val();
                this.onSaveButton(this.tagCollection.first().toJSON(), Messages.updateTagDescriptionMessage);
            },
            onEditButton: function(e) {
                var that = this;
                $(e.currentTarget).blur();
                require([
                    'views/tag/CreateTagLayoutView',
                    'modules/Modal'
                ], function(CreateTagLayoutView, Modal) {
                    var view = new CreateTagLayoutView({ 'tagCollection': that.tagCollection, 'tag': that.tag });
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
