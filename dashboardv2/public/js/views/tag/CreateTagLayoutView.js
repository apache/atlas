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
    'utils/Utils',
    'views/tag/TagAttributeItemView',
    'platform'
], function(require, Backbone, CreateTagLayoutViewTmpl, Utils, TagAttributeItemView, platform) {

    var CreateTagLayoutView = Backbone.Marionette.CompositeView.extend(
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

            childView: TagAttributeItemView,

            childViewContainer: "[data-id='addAttributeDiv']",

            childViewOptions: function() {
                return {
                    // saveButton: this.ui.saveButton,
                    parentView: this
                };
            },
            /** ui selector cache */
            ui: {
                tagName: "[data-id='tagName']",
                parentTag: "[data-id='parentTagList']",
                description: "[data-id='description']",
                title: "[data-id='title']",
                attributeData: "[data-id='attributeData']",
                addAttributeDiv: "[data-id='addAttributeDiv']",
                createTagForm: '[data-id="createTagForm"]'
            },
            /** ui events hash */
            events: function() {
                var events = {};
                events["click " + this.ui.attributeData] = "onClickAddAttriBtn";
                return events;
            },
            /**
             * intialize a new CreateTagLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'tagCollection', 'model', 'tag', 'termCollection', 'descriptionData'));
                if (this.model) {
                    this.description = this.model.get('description');
                } else if (this.termCollection) {
                    this.description = this.descriptionData;
                } else {
                    this.create = true;
                }
                this.collection = new Backbone.Collection();
            },
            bindEvents: function() {},
            onRender: function() {
                if (this.create) {
                    this.tagCollectionList();
                } else {
                    this.ui.title.html('<span>' + _.escape(this.tag) + '</span>');
                }
                if (!('placeholder' in HTMLInputElement.prototype)) {
                    this.ui.createTagForm.find('input,textarea').placeholder();
                }
            },
            tagCollectionList: function() {
                var str = '',
                    that = this;
                this.ui.parentTag.empty();
                this.tagCollection.fullCollection.each(function(val) {
                    str += '<option>' + (Utils.getName(val.toJSON())) + '</option>';
                });
                that.ui.parentTag.html(str);
                // IE9 support
                if (platform.name === "IE") {
                    that.ui.parentTag.select2({
                        multiple: true,
                        placeholder: "Search Tags",
                        allowClear: true
                    });
                }
            },
            collectionAttribute: function() {
                this.collection.add(new Backbone.Model({
                    "name": "",
                    "typeName": "string",
                    "isOptional": true,
                    "cardinality": "SINGLE",
                    "valuesMinCount": 0,
                    "valuesMaxCount": 1,
                    "isUnique": false,
                    "isIndexable": false
                }));
            },
            onClickAddAttriBtn: function() {
                this.collectionAttribute();
                if (!('placeholder' in HTMLInputElement.prototype)) {
                    this.ui.addAttributeDiv.find('input,textarea').placeholder();
                }
            }
        });
    return CreateTagLayoutView;
});
