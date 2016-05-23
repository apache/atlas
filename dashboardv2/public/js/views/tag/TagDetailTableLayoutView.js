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
    'hbs!tmpl/tag/TagDetailTableLayoutView_tmpl',
    'utils/CommonViewFunction'
], function(require, Backbone, TagDetailTableLayoutView_tmpl, CommonViewFunction) {
    'use strict';

    var TagDetailTableLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends TagDetailTableLayoutView */
        {
            _viewName: 'TagDetailTableLayoutView',

            template: TagDetailTableLayoutView_tmpl,

            /** Layout sub regions */
            regions: {},

            /** ui selector cache */
            ui: {
                detailValue: "[data-id='detailValue']",
                addTag: "[data-id='addTag']",
                deleteTag: "[data-id='delete']",
            },
            /** ui events hash */
            events: function() {
                var events = {};
                events["click " + this.ui.addTag] = function(e) {
                    this.addModalView(e);
                };
                events["click " + this.ui.deleteTag] = function(e) {
                    this.deleteTagDataModal(e);
                };
                return events;
            },
            /**
             * intialize a new EntityDetailTableLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'globalVent', 'collection', 'guid'));
                this.collectionObject = this.collection.toJSON();

            },
            bindEvents: function() {},
            onRender: function() {
                this.tagTableGenerate();
            },
            tagTableGenerate: function() {
                var that = this,
                    table = "",
                    valueObject = this.collectionObject[0].traits;
                if (_.isEmpty(valueObject)) {
                    this.$(".noTags").show();
                } else {
                    this.$(".noTags").hide();
                    _.keys(valueObject).map(function(key) {
                        var keyValue = valueObject[key];
                        var tagValue = 'NA';
                        if (_.isObject(keyValue)) {
                            //tagValue = "";
                            if (!_.isEmpty(keyValue.values)) {
                                var stringArr = [];
                                tagValue = "";
                                _.each(keyValue.values, function(val, key) {

                                    var attrName = "<span>" + key + ":" + val + "</span>";
                                    stringArr.push(attrName);
                                });
                                tagValue += stringArr.join(", ");
                            }
                            table += '<tr><td>' + keyValue.typeName + '</td><td>' + tagValue + '</td><td>' + '<a href="javascript:void(0)"><i class="fa fa-trash" data-id="delete" data-name="' + keyValue.typeName + '"></i></a></tr>';
                        } else {}
                    });
                    that.ui.detailValue.append(table);
                }
            },
            addModalView: function(e) {
                var that = this;
                require(['views/tag/addTagModalView'], function(AddTagModalView) {
                    var view = new AddTagModalView({
                        vent: that.vent,
                        guid: that.guid,
                        modalCollection: that.collection
                    });
                    // view.saveTagData = function() {
                    //override saveTagData function
                    // }
                });
            },
            deleteTagDataModal: function(e) {
                var tagName = $(e.currentTarget).data("name"),
                    that = this,
                    modal = CommonViewFunction.deleteTagModel(tagName);
                modal.on('ok', function() {
                    that.deleteTagData(e);
                });
                modal.on('closeModal', function() {
                    modal.trigger('cancel');
                });
            },
            deleteTagData: function(e) {
                var that = this,
                    tagName = $(e.currentTarget).data("name");
                CommonViewFunction.deleteTag({
                    'tagName': tagName,
                    'guid': that.guid,
                    callback: function() {
                        that.$('.fontLoader').show();
                        that.collection.fetch({ reset: true });
                    }
                });
            }
        });
    return TagDetailTableLayoutView;
});
