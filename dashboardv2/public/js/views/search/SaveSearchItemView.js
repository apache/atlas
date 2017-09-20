/*
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
    'hbs!tmpl/search/SaveSearchItemView_tmpl',
    'utils/UrlLinks',
    'utils/Utils',
    'utils/CommonViewFunction',
    'utils/Messages'
], function(require, Backbone, SaveSearchItemView_tmpl, UrlLinks, Utils, CommonViewFunction, Messages) {
    'use strict';
    return Backbone.Marionette.ItemView.extend({
        template: SaveSearchItemView_tmpl,
        tagName: 'li',
        className: 'parent-node',
        ui: {
            stateChange: '.item',
            tools: '.tools'
        },
        events: function() {
            var events = {};
            events['click ' + this.ui.stateChange] = 'stateChange';
            events['click ' + this.ui.tools] = function(e) {
                e.stopPropagation();
            };
            return events;
        },
        initialize: function(options) {
            _.extend(this, _.pick(options, 'collection', 'typeHeaders', 'applyValue', 'fetchFavioriteCollection', 'isBasic', 'classificationDefCollection', 'entityDefCollection'));
            this.model.id = this.model.get('guid');
            this.model.idAttribute = 'guid';
            this.searchTypeObj = {
                'searchType': 'dsl',
                'dslChecked': 'true'
            }
            if (this.isBasic) {
                this.searchTypeObj.dslChecked = false;
                this.searchTypeObj.searchType = 'basic';
            }
        },
        onRender: function() {
            this.showToolTip();
        },
        stateChange: function() {
            this.applyValue(this.model, this.searchTypeObj);
            this.trigger('item:clicked');
            this.ui.stateChange.parent('li').addClass('active').siblings().removeClass('active');
        },
        showToolTip: function(e) {
            var that = this;
            Utils.generatePopover({
                el: this.$('.tagPopover'),
                container: this.$el,
                popoverOptions: {
                    content: function() {
                        return "<ul class='saveSearchPopoverList'>" +
                            "<li class='th' ><i class='fa fa-search'></i> <a href='javascript:void(0)' data-fn='onSearch'>Search </a></li>" +
                            "<li class='listTerm' ><i class='fa fa-trash-o'></i> <a href='javascript:void(0)' data-fn='onDelete'>Delete</a></li>" +
                            "</ul>";
                    }
                }
            }).parent('div.tools').on('click', 'li', function(e) {
                e.stopPropagation();
                that.$('.tagPopover').popover('hide');
                that[$(this).find('a').data('fn')](e)
            });
        },
        onSearch: function() {
            var searchParameters = this.model.toJSON().searchParameters,
                params = CommonViewFunction.generateUrlFromSaveSearchObject({
                    value: searchParameters,
                    classificationDefCollection: this.classificationDefCollection,
                    entityDefCollection: this.entityDefCollection
                });
            Utils.setUrl({
                url: '#!/search/searchResult',
                urlParams: _.extend(params, this.searchTypeObj),
                mergeBrowserUrl: false,
                trigger: true,
                updateTabState: true
            });
        },
        onDelete: function() {
            var that = this;
            var notifyObj = {
                modal: true,
                html: true,
                text: Messages.conformation.deleteMessage + "<b>" + this.model.get('name') + "</b>" + " ?",
                ok: function(argument) {
                    that.onDeleteNotifyOk();
                },
                cancel: function(argument) {}
            }
            Utils.notifyConfirm(notifyObj);
        },
        onDeleteNotifyOk: function() {
            var that = this;
            this.model.urlRoot = UrlLinks.saveSearchApiUrl();
            this.model.destroy({
                wait: true,
                success: function(model, data) {
                    if (that.collection) {
                        that.collection.remove(data);
                    }
                    Utils.notifySuccess({
                        content: that.model.get('name') + Messages.deleteSuccessMessage
                    });
                }
            });
        }
    });
});