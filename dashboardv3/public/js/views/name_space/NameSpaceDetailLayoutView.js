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
    'hbs!tmpl/name_space/NameSpaceDetailLayoutView_tmpl',
    'utils/Utils',
    'views/tag/AddTagAttributeView',
    'collection/VTagList',
    'models/VTag',
    'utils/Messages',
    'utils/UrlLinks',
    "utils/Globals",
], function(require, Backbone, NameSpaceDetailLayoutViewTmpl, Utils, AddTagAttributeView, VTagList, VTag, Messages, UrlLinks, Globals) {
    'use strict';

    var NameSpaceDetailLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends NameSpaceDetailLayoutView */
        {
            template: NameSpaceDetailLayoutViewTmpl,
            /** Layout sub regions */
            regions: {},
            /** ui selector cache */
            ui: {
                title: '[data-id="title"]',
                editBox: '[data-id="editBox"]',
                saveButton: "[data-id='saveButton']",
                description: '[data-id="description"]',
                publishButton: '[data-id="publishButton"]',
                backButton: '[data-id="backButton"]'
            },
            /** ui events hash */
            events: function() {
                var events = {};
                events["click " + this.ui.backButton] = function() {
                    Utils.backButtonClick();
                };
                return events;
            },
            /**
             * intialize a new NameSpaceDetailLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'guid', 'entity', 'entityName', 'attributeDefs', 'enumDefCollection', 'typeHeaders', 'nameSpaceCollection', 'selectedNameSpace', 'nameSpaceAttr'));

            },
            bindEvents: function() {
                this.listenTo(this.nameSpaceCollection, 'reset', function() {
                    if (!this.model) {
                        this.model = this.nameSpaceCollection.fullCollection.findWhere({ guid: this.guid });
                        if (this.model) {
                            this.renderTagDetail();
                        } else {
                            this.$('.fontLoader').hide();
                            Utils.notifyError({
                                content: 'Tag Not Found'
                            });
                        }
                    }
                }, this);
                this.listenTo(this.collection, 'error', function(error, response) {
                    if (response.responseJSON && response.responseJSON.error) {
                        Utils.notifyError({
                            content: response.responseJSON.error
                        });
                    } else {
                        Utils.notifyError({
                            content: 'Something went wrong'
                        });
                    }
                    this.$('.fontLoader').hide();
                }, this);
            },
            onRender: function() {
                if (this.nameSpaceCollection.models.length && !this.model) {
                    this.model = this.nameSpaceCollection.fullCollection.findWhere({ guid: this.guid });
                    Utils.showTitleLoader(this.$('.page-title .fontLoader'), this.$('.tagDetail'));
                    this.renderTagDetail();
                }
                this.bindEvents();
            },
            renderTagDetail: function() {
                var that = this,
                    attributeData = "";
                this.attributeDefs = this.model.get('attributeDefs');
                this.ui.title.html('<span>' + that.model.get('name') + '</span>');
                if (that.model.get('description')) {
                    this.ui.description.text((that.model.get('description')));
                }
                Utils.hideTitleLoader(this.$('.fontLoader'), this.$('.tagDetail'));
            }
        });
    return NameSpaceDetailLayoutView;
});