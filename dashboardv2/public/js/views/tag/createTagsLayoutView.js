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
    'views/tag/addTagAttributeItemView',
    'utils/Utils'
], function(require, Backbone, createTagLayoutViewTmpl, addTagAttributeItemView, Utils) {
    'use strict';

    var CreateTagView = Marionette.CompositeView.extend({
        template: createTagLayoutViewTmpl,
        regions: {},

        childView: addTagAttributeItemView,

        childViewContainer: "div[data-id='addAttributeDiv']",

        childViewOptions: function() {
            return {
                saveBtn: this.ui.saveButton
            };
        },
        /** ui selector cache */
        ui: {
            parentTag: "[data-id='parentTag']",
            addAttrBtn: "[data-id='addAttrBtn']",
            refreshBtn: "[data-id='refreshBtn']",
            typeName: "[data-id='typeName']",
            saveButton: "[data-id='saveButton']"
        },
        events: function() {
            var events = {},
                that = this;
            events["click " + this.ui.refreshBtn] = 'onRefreshClick';
            events["click " + this.ui.addAttrBtn] = 'onAddAttribute';
            events["keypress " + this.ui.typeName] = 'onTypeName';
            events["keyup " + this.ui.typeName] = 'onBackSpceName';
            events["click " + this.ui.saveButton] = 'onSaveButton';
            return events;
        },
        /**
         * intialize a new createTagLayoutView_tmpl Layout
         * @constructs
         */
        initialize: function(options) {
            _.extend(this, _.pick(options, 'globalVent', 'tagsCollection'));
            this.collection = new Backbone.Collection();
            this.bindEvents();
            this.json = {
                "enumTypes": [],
                "traitTypes": [],
                "structTypes": [],
                "classTypes": []
            };
        },
        bindEvents: function() {
            this.listenTo(this.tagsCollection, 'reset', function() {
                this.tagsCollectionList();
            }, this);
        },
        onRender: function() {
            this.fetchCollection();
        },
        tagsCollectionList: function() {
            this.ui.parentTag.empty();
            for (var i = 0; i < this.tagsCollection.fullCollection.models.length; i++) {
                var tags = this.tagsCollection.fullCollection.models[i].get("tags");
                var str = '<option>' + tags + '</option>';
                this.ui.parentTag.append(str);
            }
        },
        fetchCollection: function() {
            $.extend(this.tagsCollection.queryParams, { type: 'TRAIT' });
            this.tagsCollection.fetch({ reset: true });
        },
        onRefreshClick: function() {
            this.fetchCollection();
        },
        onAddAttribute: function(e) {
            e.preventDefault();
            this.collection.add(new Backbone.Model({
                "dataTypeName": "string",
                "multiplicity": "optional",
                "isComposite": false,
                "isUnique": false,
                "isIndexable": true,
                "reverseAttributeName": null
            }));
            if (this.ui.addAttrBtn.val() == "") {
                this.ui.saveButton.attr("disabled", "true");
            }
        },
        onTypeName: function() {
            if (this.ui.typeName.val() == "") {
                this.ui.saveButton.removeAttr("disabled");
                this.ui.addAttrBtn.removeAttr("disabled");
            }
        },
        onBackSpceName: function(e) {
            if (e.keyCode == 8 && this.ui.typeName.val() == "") {
                this.ui.saveButton.attr("disabled", "true");
                this.ui.addAttrBtn.attr("disabled", "true");
            }
        },
        onSaveButton: function() {
            var that = this;
            this.name = this.ui.typeName.val();
            this.json.traitTypes[0] = {
                attributeDefinitions: this.collection.toJSON(),
                typeName: this.name,
                typeDescription: null,
                superTypes: this.ui.parentTag.val(),
                hierarchicalMetaTypeName: "org.apache.atlas.typesystem.types.TraitType"
            };
            new this.tagsCollection.model().set(this.json).save(null, {
                success: function(model, response) {
                    that.fetchCollection();
                    that.ui.typeName.val("");
                    that.ui.saveButton.attr("disabled", "true");
                    Utils.notifySuccess({
                        content: that.name + "  has been created"
                    });
                    that.collection.reset();

                },
                error: function(model, response) {
                    if (response.responseJSON && response.responseJSON.error) {
                        Utils.notifyError({
                            content: response.responseJSON.error
                        });
                    }
                }
            });
        }
    });
    return CreateTagView;
});
