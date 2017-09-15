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
    'hbs!tmpl/business_catalog/BusinessCatalogDetailLayoutView_tmpl',
    'utils/Utils',
    'models/VEntity',
    'models/VCatalog',
    'utils/Messages'
], function(require, Backbone, BusinessCatalogDetailLayoutViewTmpl, Utils, VEntity, VCatalog, Messages) {
    'use strict';

    var BusinessCatalogDetailLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends BusinessCatalogDetailLayoutView */
        {
            _viewName: 'BusinessCatalogDetailLayoutView',

            template: BusinessCatalogDetailLayoutViewTmpl,

            /** Layout sub regions */
            regions: {},
            /** ui selector cache */
            ui: {
                title: '[data-id="title"]',
                editButton: '[data-id="editButton"]',
                description: '[data-id="description"]',
                editBox: '[data-id="editBox"]',
                createDate: '[data-id="createDate"]'
            },
            /** ui events hash */
            events: function() {
                var events = {};
                events["click " + this.ui.editButton] = 'onEditButton';
                events["click " + this.ui.cancelButton] = 'onCancelButtonClick';
                return events;
            },
            /**
             * intialize a new BusinessCatalogDetailLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'url', 'collection'));
                this.bindEvents();
            },
            bindEvents: function() {
                var that = this;
                this.listenTo(this.collection, 'error', function(model, response) {
                    this.$('.fontLoader').hide();
                    if (response && response.responseJSON && response.responseJSON.message) {
                        Utils.notifyError({
                            content: response.responseJSON.message
                        });
                    }

                }, this);
                this.listenTo(this.collection, 'reset', function() {
                    this.$('.fontLoader').hide();
                    this.$('.hide').removeClass('hide');
                    this.model = this.collection.first();
                    var name = this.model.get('name'),
                        description = this.model.get('description'),
                        createdDate = this.model.get('creation_time');
                    if (name) {
                        this.ui.title.show();
                        this.termName = Utils.checkTagOrTerm(name, true);
                        this.ui.title.html('<span>' + this.termName.name + '</span>');
                    } else {
                        this.ui.title.hide();
                    }
                    if (description) {
                        this.ui.description.show();
                        this.ui.description.html('<span>' + _.escape(description) + '</span>');
                    } else {
                        this.ui.description.hide();
                    }
                    if (createdDate) {
                        var splitDate = createdDate.split(":");
                        this.ui.createDate.html('<strong> Date Created: </strong> ' + splitDate[0] + " " + splitDate[1] + ":" + splitDate[2] + ":" + splitDate[3] + " (GMT)");
                    }
                    Utils.hideTitleLoader(this.$('.fontLoader'), this.$('.catlogDetail'));
                }, this);
            },
            onRender: function() {
                var that = this;
                Utils.showTitleLoader(this.$('.page-title .fontLoader'), this.$('.catlogDetail'));
            },
            fetchCollection: function() {
                this.collection.fetch({ reset: true });
            },
            onEditButton: function(e) {
                var that = this;
                $(e.currentTarget).blur();
                require([
                    'views/tag/CreateTagLayoutView',
                    'modules/Modal'
                ], function(CreateTagLayoutView, Modal) {
                    var view = new CreateTagLayoutView({ 'termCollection': that.collection, 'descriptionData': that.model.get('description'), 'tag': _.unescape(that.termName.name) });
                    var modal = new Modal({
                        title: 'Edit Term',
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
                        that.onSaveDescriptionClick(view);
                    });
                    modal.on('closeModal', function() {
                        modal.trigger('cancel');
                    });
                });
            },
            textAreaChangeEvent: function(view, modal) {
                if (view.description == view.ui.description.val()) {
                    modal.$el.find('button.ok').prop('disabled', true);
                } else {
                    modal.$el.find('button.ok').prop('disabled', false);
                }
            },
            onSaveDescriptionClick: function(view) {
                view.description = view.ui.description.val();
                this.onSaveButton(this.collection.first().toJSON(), Messages.tag.updateTermDescriptionMessage, view);
                this.ui.description.show();
            },
            onSaveButton: function(saveObject, message, view) {
                var that = this,
                    termModel = new VCatalog();
                termModel.url = function() {
                    return that.collection.url;
                };
                Utils.showTitleLoader(this.$('.page-title .fontLoader'), this.$('.catlogDetail'));
                termModel.set({
                    "description": view.ui.description.val()
                }).save(null, {
                    type: "PUT",
                    success: function(model, response) {
                        that.collection.fetch({ reset: true });
                        Utils.notifySuccess({
                            content: message
                        });
                    }
                });
            }
        });
    return BusinessCatalogDetailLayoutView;

});
