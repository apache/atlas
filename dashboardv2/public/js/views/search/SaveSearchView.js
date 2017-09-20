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
    'hbs!tmpl/search/SaveSearch_tmpl',
    'views/search/SaveSearchItemView',
    'collection/VSearchList',
    'utils/Utils',
    'utils/UrlLinks',
    'utils/CommonViewFunction',
    'utils/Messages'
], function(require, Backbone, SaveSearch_Tmpl, SaveSearchItemView, VSearchList, Utils, UrlLinks, CommonViewFunction, Messages) {
    'use strict';

    return Backbone.Marionette.CompositeView.extend({
        template: SaveSearch_Tmpl,
        childView: SaveSearchItemView,
        childViewContainer: "[data-id='itemViewContent']",
        ui: {
            saveAs: "[data-id='saveAsBtn']",
            save: "[data-id='saveBtn']"
        },
        childViewOptions: function() {
            return {
                collection: this.collection,
                typeHeaders: this.typeHeaders,
                applyValue: this.applyValue,
                isBasic: this.isBasic,
                classificationDefCollection: this.classificationDefCollection,
                entityDefCollection: this.entityDefCollection,
                fetchFavioriteCollection: this.fetchCollection.bind(this)
            };
        },
        childEvents: function() {
            return {
                "item:clicked": function() {
                    this.ui.save.attr('disabled', false);
                }
            }
        },
        events: function() {
            var events = {};
            events['click ' + this.ui.saveAs] = "saveAs";
            events['click ' + this.ui.save] = "save";
            return events;
        },
        initialize: function(options) {
            var that = this;
            _.extend(this, _.pick(options, 'collection', 'value', 'searchVent', 'typeHeaders', 'applyValue', 'getValue', 'isBasic', 'fetchCollection', 'classificationDefCollection', 'entityDefCollection'));
        },
        onRender: function() {
            this.bindEvents();
        },
        bindEvents: function() {
            this.listenTo(this.collection, "reset error", function(model, response) {
                this.$('.fontLoader-relative').hide();
                if (model && model.length) {
                    this.$("[data-id='itemViewContent']").text("");
                } else {
                    this.$("[data-id='itemViewContent']").text("You don't have any favorite search.")
                }
            }, this);
        },
        saveAs: function(e) {
            var that = this,
                value = this.getValue();
            if (value && (value.type || value.tag || value.query)) {
                require([
                    'views/search/SaveAsLayoutView'
                ], function(SaveAsLayoutView) {
                    new SaveAsLayoutView({ 'value': that.value, 'searchVent': that.searchVent, 'collection': that.collection, 'getValue': that.getValue, 'isBasic': that.isBasic });
                });
            } else {
                Utils.notifyInfo({
                    content: Messages.search.favoriteSearch.notSelectedSearchFilter
                })
            }
        },
        save: function() {
            var that = this,
                obj = {},
                notifyObj = {
                    modal: true,
                    html: true,
                    ok: function(argument) {
                        that.onSaveNotifyOk(obj);
                    },
                    cancel: function(argument) {}
                },
                selectedEl = this.$('.saveSearchList li.active').find('div.item');
            obj.name = selectedEl.find('a').text();
            obj.id = selectedEl.data('id');
            if (selectedEl && selectedEl.length) {
                notifyObj['text'] = Messages.search.favoriteSearch.save + " <b>" + obj.name + "</b> ?";
                Utils.notifyConfirm(notifyObj);
            } else {
                Utils.notifyInfo({
                    content: Messages.search.favoriteSearch.notSelectedElement
                })
            }
        },
        onSaveNotifyOk: function(obj) {
            var that = this
            if (obj && obj.id) {
                var model = new this.collection.model();
                obj.value = this.getValue();
                var saveObj = CommonViewFunction.generateObjectForSaveSearchApi(obj);
                saveObj['guid'] = obj.id;
                model.urlRoot = UrlLinks.saveSearchApiUrl();
                model.save(saveObj, {
                    type: 'PUT',
                    success: function(model, data) {
                        if (that.collection) {
                            var collectionRef = that.collection.find({ guid: data.guid });
                            if (collectionRef) {
                                collectionRef.set(data);
                            }
                        }
                        Utils.notifySuccess({
                            content: obj.name + Messages.editSuccessMessage
                        });
                    }
                });
            }
        }
    });
});