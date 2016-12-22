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
                addTagBtn: '[data-id="addTagBtn"]',
                description: '[data-id="description"]',
                descriptionTextArea: '[data-id="descriptionTextArea"]',
                publishButton: '[data-id="publishButton"]'
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
                    this.model = this.collection.fullCollection.findWhere({ name: this.tag });
                    /// this.model = this.collection.fullCollection.findWhere({ typeName: $(".dataTypeSelector").val() });
                    if (this.model) {
                        this.renderTagDetail();
                    } else {
                        this.ui.addTagBtn.hide();
                        this.ui.editButton.hide();
                        Utils.notifyError({
                            content: 'Something went wrong'
                        });
                    }

                }, this);
                this.listenTo(this.tagCollection, 'error', function(error, response) {
                    this.ui.addTagBtn.hide();
                    this.ui.editButton.hide();
                    if (response.responseJSON && response.responseJSON.error) {
                        Utils.notifyError({
                            content: response.responseJSON.error
                        });
                    } else {
                        Utils.notifyError({
                            content: 'Something went wrong'
                        });
                    }

                }, this);
            },
            onRender: function() {
                if (this.collection.models.length && !this.model) {
                    this.model = this.collection.fullCollection.findWhere({ name: this.tag });
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
                    this.ui.title.html('<span>' + _.escape(this.model.get("name")) + '</span>');
                }
                if (this.model.get("description")) {
                    this.ui.description.text(this.model.get("description"));
                }
                if (this.model.get("attributeDefs")) {
                    if (!_.isArray(attributeDefs)) {
                        attributeDefs = [attributeDefs];
                    }
                    _.each(attributeDefs, function(value, key) {
                        attributeData += '<span class="inputAttribute">' + _.escape(value.name) + '</span>';
                    });
                    this.ui.showAttribute.html(attributeData);
                }

            },
            onSaveButton: function(saveObject, message) {
                var that = this;
                var validate = true;
                if (this.modal.$el.find(".attributeInput").length > 1) {
                    this.modal.$el.find(".attributeInput").each(function() {
                        if ($(this).val() === "") {
                            $(this).css('borderColor', "red")
                            validate = false;
                        }
                    });
                }
                this.modal.$el.find(".attributeInput").keyup(function() {
                    $(this).css('borderColor', "#e8e9ee");
                });
                if (!validate) {
                    Utils.notifyInfo({
                        content: "Please fill the attributes or delete the input box"
                    });
                    return;
                }
                this.model.saveTagAttribute(this.model.get('guid'), {
                    data: JSON.stringify(saveObject),
                    success: function(model, response) {
                        that.model.set(model);
                        that.renderTagDetail();
                        Utils.notifySuccess({
                            content: message
                        });
                        that.modal.close();
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
                        var view = new AddTagAttributeView();
                        that.modal = new Modal({
                            title: 'Add Attribute',
                            content: view,
                            cancelText: "Cancel",
                            okText: 'Add',
                            okCloses: false,
                            allowCancel: true,
                        }).open();
                        that.modal.$el.find('button.ok').attr("disabled", "true");
                        $(view.ui.addAttributeDiv).on('keyup', that.modal.$el.find('attributeInput'), function(e) {
                            if ((e.keyCode == 8 || e.keyCode == 46 || e.keyCode == 32) && e.target.value.trim() == "") {
                                that.modal.$el.find('button.ok').attr("disabled", "disabled");
                            } else {
                                that.modal.$el.find('button.ok').removeAttr("disabled");
                            }
                        });
                        that.modal.on('ok', function() {
                            var newAttributeList = view.collection.toJSON();
                            var saveJSON = JSON.parse(JSON.stringify(that.model.toJSON()));
                            var oldAttributeList = saveJSON.attributeDefs;
                            _.each(newAttributeList, function(obj) {
                                oldAttributeList.push(obj);
                            });
                            that.onSaveButton(saveJSON, Messages.addAttributeSuccessMessage);
                        });
                        that.modal.on('closeModal', function() {
                            that.modal.trigger('cancel');
                        });
                    });
            },
            onCancelButtonClick: function() {
                this.ui.description.show();
                this.ui.editButton.show();
                this.ui.editBox.hide();
            },
            textAreaChangeEvent: function(view) {
                if (this.model.get('description') === view.ui.description.val()) {
                    this.modal.$el.find('button.ok').prop('disabled', true);
                } else {
                    this.modal.$el.find('button.ok').prop('disabled', false);
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
                    that.modal = new Modal({
                        title: 'Edit Tag',
                        content: view,
                        cancelText: "Cancel",
                        okText: 'Save',
                        allowCancel: true,
                    }).open();
                    view.ui.description.on('keyup', function(e) {
                        that.textAreaChangeEvent(view);
                    });
                    that.modal.$el.find('button.ok').prop('disabled', true);
                    that.modal.on('ok', function() {
                        that.onPublishClick(view);
                    });
                    that.modal.on('closeModal', function() {
                        that.modal.trigger('cancel');
                    });
                });
            }
        });
    return TagAttributeDetailLayoutView;
});
