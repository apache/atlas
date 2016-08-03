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
    'hbs!tmpl/tag/createTagLayoutView_tmpl',
    'utils/Utils'
], function(require, Backbone, CreateTagLayoutViewTmpl, Utils) {

    var CreateTagLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends CreateTagLayoutView */
        {
            _viewName: 'CreateTagLayoutView',

            template: CreateTagLayoutViewTmpl,

            templateHelpers: function() {
                return {
                    create: this.create,
                    description: this.description
                };
            },

            /** Layout sub regions */
            regions: {},

            /** ui selector cache */
            ui: {
                tagName: "[data-id='tagName']",
                parentTag: "[data-id='parentTagList']",
                description: "[data-id='description']",
                title: "[data-id='title']"
            },
            /** ui events hash */
            events: function() {
                var events = {};
                return events;
            },
            /**
             * intialize a new CreateTagLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'tagCollection', 'tag', 'termCollection', 'descriptionData'));
                if (this.tagCollection && this.tagCollection.length > 0 && this.tagCollection.first().get('traitTypes')) {
                    this.description = this.tagCollection.first().get('traitTypes')[0].typeDescription;
                } else if (this.termCollection) {
                    this.description = this.descriptionData;
                } else {
                    this.create = true;
                }
            },
            bindEvents: function() {},
            onRender: function() {
                if (this.create) {
                    this.tagCollectionList();
                } else {
                    this.ui.title.html('<span>' + this.tag + '</span>');
                }
            },
            tagCollectionList: function() {
                var str = '',
                    that = this;
                this.ui.parentTag.empty();
                this.tagCollection.each(function(val) {
                    str += '<option>' + val.get("tags") + '</option>';
                });
                that.ui.parentTag.html(str);
            }
        });
    return CreateTagLayoutView;
});
