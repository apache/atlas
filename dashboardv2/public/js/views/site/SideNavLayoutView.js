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
    'hbs!tmpl/site/SideNavLayoutView_tmpl',
    'utils/Utils',
    'utils/Globals',
    'utils/UrlLinks'
], function(require, tmpl, Utils, Globals, UrlLinks) {
    'use strict';

    var SideNavLayoutView = Marionette.LayoutView.extend({
        template: tmpl,

        regions: {
            RBusinessCatalogLayoutView: "#r_businessCatalogLayoutView",
            RTagLayoutView: "#r_tagLayoutView",
            RSearchLayoutView: "#r_searchLayoutView",
        },
        ui: {
            tabs: '.tabs li a',
        },
        templateHelpers: function() {
            return {
                tabClass: this.tabClass,
                apiBaseUrl: UrlLinks.apiBaseUrl
            };
        },
        events: function() {
            var events = {},
                that = this;
            events["click " + this.ui.tabs] = function(e) {
                var urlString = "",
                    elementName = $(e.currentTarget).data(),
                    tabStateUrls = Globals.saveApplicationState.tabState,
                    urlStateObj = Utils.getUrlState,
                    hashUrl = Utils.getUrlState.getQueryUrl().hash;

                if (urlStateObj.isTagTab()) {
                    if (hashUrl != tabStateUrls.tagUrl) {
                        Globals.saveApplicationState.tabState.tagUrl = hashUrl;
                    }
                } else if (urlStateObj.isSearchTab()) {
                    if (hashUrl != tabStateUrls.searchUrl) {
                        Globals.saveApplicationState.tabState.searchUrl = hashUrl;
                    }
                }

                if (elementName.name == "tab-tag") {
                    urlString = tabStateUrls.tagUrl; //'#!/tag';
                } else if (elementName.name == "tab-search") {
                    urlString = tabStateUrls.searchUrl; // '#!/search';
                }
                Utils.setUrl({
                    url: urlString,
                    mergeBrowserUrl: false,
                    trigger: true,
                    updateTabState: true
                });
            };
            return events;
        },
        initialize: function(options) {
            _.extend(this, _.pick(options, 'url', 'value', 'tag', 'selectFirst', 'classificationDefCollection', 'typeHeaders', 'searchVent', 'entityDefCollection', 'enumDefCollection', 'searchTableColumns', 'searchTableFilters'));
            this.tabClass = "tab col-sm-6";
        },
        onRender: function() {
            this.renderTagLayoutView();
            this.renderSearchLayoutView();
            this.selectTab();

        },
        renderTagLayoutView: function() {
            var that = this;
            require(['views/tag/TagLayoutView'], function(TagLayoutView) {
                that.RTagLayoutView.show(new TagLayoutView({
                    collection: that.classificationDefCollection,
                    tag: that.tag,
                    value: that.value,
                    typeHeaders: that.typeHeaders
                }));
            });
        },
        renderSearchLayoutView: function() {
            var that = this;
            require(['views/search/SearchLayoutView'], function(SearchLayoutView) {
                that.RSearchLayoutView.show(new SearchLayoutView({
                    value: that.value,
                    searchVent: that.searchVent,
                    typeHeaders: that.typeHeaders,
                    entityDefCollection: that.entityDefCollection,
                    enumDefCollection: that.enumDefCollection,
                    classificationDefCollection: that.classificationDefCollection,
                    searchTableColumns: that.searchTableColumns,
                    searchTableFilters: that.searchTableFilters
                }));
            });
        },
        selectTab: function() {
            if (Utils.getUrlState.isTagTab()) {
                this.$('.tabs').find('li a[aria-controls="tab-tag"]').parents('li').addClass('active').siblings().removeClass('active');
                this.$('.tab-content').find('div#tab-tag').addClass('active').siblings().removeClass('active');
            } else if (Utils.getUrlState.isSearchTab() || (Utils.getUrlState.isDetailPage()) || Utils.getUrlState.isInitial()) {
                this.$('.tabs').find('li a[aria-controls="tab-search"]').parents('li').addClass('active').siblings().removeClass('active');
                this.$('.tab-content').find('div#tab-search').addClass('active').siblings().removeClass('active');
            }
        },
    });
    return SideNavLayoutView;
});