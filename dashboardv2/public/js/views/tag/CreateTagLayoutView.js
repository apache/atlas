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

            /** Layout sub regions */
            regions: {},

            /** ui selector cache */
            ui: {

                tagName: "[data-id='tagName']",
                parentTag: "[data-id='parentTag']",
                description: "[data-id='description']"
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
                _.extend(this, _.pick(options, 'tagCollection'));
                this.bindEvents();
            },
            bindEvents: function() {
                // this.listenTo(this.tagCollection, 'reset', function() {
                //     this.tagCollectionList();
                // }, this);
            },
            onRender: function() {
                //this.fetchCollection();
                this.tagCollectionList();
            },

            tagCollectionList: function() {
                this.ui.parentTag.empty();
                var str = '';
                for (var i = 0; i < this.tagCollection.fullCollection.models.length; i++) {
                    var tags = this.tagCollection.fullCollection.models[i].get("tags");
                    str += '<option>' + tags + '</option>';
                    this.ui.parentTag.html(str);
                }
                this.ui.parentTag.select2({
                    placeholder: "Select parent Tag",
                    allowClear: true
                });
            },
            fetchCollection: function() {
                $.extend(this.tagCollection.queryParams, { type: 'TRAIT' });
                this.tagCollection.fetch({ reset: true });
            },
        });
    return CreateTagLayoutView;
});
