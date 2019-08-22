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
    'hbs!tmpl/site/Header',
    'utils/CommonViewFunction',
    'utils/Globals',
    'utils/Utils',
    'utils/UrlLinks'
], function(require, tmpl, CommonViewFunction, Globals, Utils, UrlLinks) {
    'use strict';

    var Header = Marionette.LayoutView.extend({
        template: tmpl,
        regions: {
            RGlobalSearchLayoutView: "#r_globalSearchLayoutView",
            RFilterBrowserLayoutView: "#r_filterBrowserLayoutView"
        },
        ui: {
            backButton: "[data-id='backButton']",
            menuHamburger: "[data-id='menuHamburger']",
            signOut: "[data-id='signOut']"
        },
        events: function() {
            var events = {};
            var that = this;
            events['click ' + this.ui.menuHamburger] = function() {
                this.setSearchBoxWidth({
                    updateWidth: function(atlasHeaderWidth) {
                        return $('body').hasClass('full-screen') ? atlasHeaderWidth - 350 : atlasHeaderWidth + 350
                    }
                });
                $('body').toggleClass("full-screen");
            };
            events['click ' + this.ui.signOut] = function() {
                Utils.localStorage.setValue("atlas_ui","beta");
                var path = Utils.getBaseUrl(window.location.pathname);
                window.location = path + "/logout.html";
            };
            return events;

        },
        initialize: function(options) {
            this.bindEvent();
            this.options = options;
        },
        setSearchBoxWidth: function(options) {
            var atlasHeaderWidth = this.$el.find(".atlas-header").width(),
                minusWidth = Utils.getUrlState.isDetailPage() ? 413 : 263;
            if (options && options.updateWidth) {
                atlasHeaderWidth = options.updateWidth(atlasHeaderWidth);
            }
            if (atlasHeaderWidth > minusWidth) {
                this.$el.find(".global-search-container").width(atlasHeaderWidth - minusWidth);
            }
        },
        bindEvent: function() {
            var that = this;
            $(window).resize(function() {
                that.setSearchBoxWidth();
            });

            $('body').on('click', '.userPopoverOptions li', function(e) {
                that.$('.user-dropdown').popover('hide');
            });
        },
        onRender: function() {
            var that = this;
            if (Globals.userLogedIn.status) {
                that.$('.userName').html(Globals.userLogedIn.response.userName);
            }
            if (this.options.fromDefaultSearch !== true) {
                this.renderGlobalSearch();
            }
        },
        onShow: function() {
            this.setSearchBoxWidth();
        },
        manualRender: function(options) {
            this.setSearchBoxWidth();
            if (options === undefined || options && options.fromDefaultSearch === undefined) {
                options = _.extend({}, options, { fromDefaultSearch: false });
            }
            _.extend(this.options, options)
            if (this.options.fromDefaultSearch === true) {
                this.$('.global-search-container>div,.global-filter-browser').hide();
            } else {
                if (this.RGlobalSearchLayoutView.currentView === undefined) {
                    this.renderGlobalSearch();
                }
                this.$('.global-search-container>div').show();
            }
        },
        renderGlobalSearch: function() {
            var that = this;
            require(["views/search/GlobalSearchLayoutView"], function(GlobalSearchLayoutView) {
                that.RGlobalSearchLayoutView.show(new GlobalSearchLayoutView(that.options));
            });
        },
        renderFliterBrowser: function() {
            var that = this;
            require(["views/search/SearchFilterBrowseLayoutView"], function(SearchFilterBrowseLayoutView) {
                that.RFilterBrowserLayoutView.show(new SearchFilterBrowseLayoutView(_.extend({ toggleLayoutClass: that.toggleLayoutClass }, that.options)));
            });
        },
    });
    return Header;
});