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
    'hbs!tmpl/tag/TagLayoutView_tmpl',
    'collection/VTagList',
    'utils/Utils',
], function(require, Backbone, TagLayoutViewTmpl, VTagList, Utils) {
    'use strict';

    var TagLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends TagLayoutView */
        {
            _viewName: 'TagLayoutView',

            template: TagLayoutViewTmpl,

            /** Layout sub regions */
            regions: {},

            /** ui selector cache */
            ui: {
                listTag: "[data-id='listTag']",
                listType: "[data-id='listType']",
                tagElement: "[data-id='tags']",
                referesh: "[data-id='referesh']",
                searchTag: "[data-id='searchTag']"
            },
            /** ui events hash */
            events: function() {
                var events = {};
                events["click " + this.ui.tagElement] = 'onTagClick';
                events["click " + this.ui.referesh] = 'refereshClick';
                events["keyup " + this.ui.searchTag] = 'offlineSearchTag';
                return events;
            },
            /**
             * intialize a new TagLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'globalVent', 'vent'));
                this.tagCollection = new VTagList();
                $.extend(this.tagCollection.queryParams, { type: 'TRAIT' });
                this.typeCollection = new VTagList();
                $.extend(this.typeCollection.queryParams, { type: 'CLASS' });
                this.bindEvents();

            },
            bindEvents: function() {
                this.listenTo(this.tagCollection, 'reset', function() {
                    this.tagsAndTypeGenerator('tagCollection', 'listTag');
                }, this);
                this.listenTo(this.typeCollection, 'reset', function() {
                    this.tagsAndTypeGenerator('typeCollection', 'listType');
                }, this);
            },
            onRender: function() {
                this.fetchCollections();
            },
            fetchCollections: function() {
                this.tagCollection.fetch({ reset: true });
                this.typeCollection.fetch({ reset: true });
            },
            tagsAndTypeGenerator: function(collection, element, searchString) {
                if (element == "listType") {
                    var searchType = "dsl";
                    var icon = "fa fa-cogs"
                } else {
                    var searchType = "fulltext";
                    var icon = "fa fa-tags"
                }
                var str = '';
                _.each(this[collection].fullCollection.models, function(model) {
                    var tagName = model.get("tags");
                    if (searchString) {
                        if (tagName.search(new RegExp(searchString, "i")) != -1) {
                            str += '<a href="javascript:void(0)" data-id="tags" data-type="' + searchType + '" class="list-group-item"><i class="' + icon + '"></i>' + tagName + '</a>';
                        } else {
                            return;
                        }
                    } else {
                        str += '<a href="javascript:void(0)" data-id="tags" data-type="' + searchType + '" class="list-group-item"><i class="' + icon + '"></i>' + tagName + '</a>';
                    }
                });
                this.ui[element].empty().append(str);
            },
            onTagClick: function(e) {
                var data = $(e.currentTarget).data();
                this.vent.trigger("tag:click", { 'query': e.currentTarget.text, 'searchType': data.type });
                Utils.setUrl({
                    url: '#!/dashboard/assetPage',
                    urlParams: {
                        query: e.currentTarget.text,
                        searchType: data.type
                    },
                    mergeBrowserUrl: true,
                    trigger: false
                });
            },
            refereshClick: function() {
                this.fetchCollections();
            },
            offlineSearchTag: function(e) {
                var type = $(e.currentTarget).data('type');
                var collectionType = type == "listTag" ? "tagCollection" : "typeCollection";
                this.tagsAndTypeGenerator(collectionType, type, $(e.currentTarget).val());
            }
        });
    return TagLayoutView;
});
