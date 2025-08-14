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

define(['App','require',
    'hbs!tmpl/site/Header',
    'utils/CommonViewFunction',
    'utils/Globals',
    'utils/Utils',
    'utils/UrlLinks',
    'collection/VDownloadList',
], function(App,require, tmpl, CommonViewFunction, Globals, Utils, UrlLinks, VDownloadList) {
    'use strict';

    var Header = Marionette.LayoutView.extend({
        template: tmpl,
        regions: {
            RGlobalSearchLayoutView: "#r_globalSearchLayoutView",
            RFilterBrowserLayoutView: "#r_filterBrowserLayoutView",
            RDownloadSearchResult: "[data-id='r_DownloadSearchResult']"
        },
        templateHelpers: function() {
            return {
                apiDocUrl: UrlLinks.apiDocUrl(),
                isDebugMetricsEnabled: Globals.isDebugMetricsEnabled
            };
        },
        ui: {
            backButton: "[data-id='backButton']",
            menuHamburger: "[data-id='menuHamburger']",
            administrator: "[data-id='administrator']",
            showDebug: "[data-id='showDebug']",
            signOut: "[data-id='signOut']",
            reactUISwitch: "[data-id='reactUISwitch']",
            uiSwitch: "[data-id='uiSwitch']",
            showDownloads: "[data-id='showDownloads']"
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
            events['click ' + this.ui.signOut] = 'checkKnoxSSO';
            events['click ' + this.ui.administrator] = function() {
                Utils.setUrl({
                    url: "#!/administrator",
                    mergeBrowserUrl: false,
                    trigger: true,
                    updateTabState: true
                });
            };
            events['click ' + this.ui.showDownloads] = function(e) {
                this.exportVent.trigger("downloads:showDownloads");
            };
            events['click ' + this.ui.showDebug] = function() {
                Utils.setUrl({
                    url: "#!/debugMetrics",
                    mergeBrowserUrl: false,
                    trigger: true,
                    updateTabState: true
                });
            };
            events["click " + this.ui.reactUISwitch] = function() {
                var path = Utils.getBaseUrl(window.location.pathname) + "/n3/index.html";
                if (window.location.hash.length > 2) {
                    path += window.location.hash;
                }
                window.location.href = path;
            };
            events["click " + this.ui.uiSwitch] = function() {
                var path = Utils.getBaseUrl(window.location.pathname) + "/index.html";
                if (window.location.hash.length > 2) {
                    path += window.location.hash;
                }
                window.location.href = path;
            };


            return events;

        },
        initialize: function(options) {
            _.extend(this, _.pick(options, 'exportVent'));
            this.bindEvent();
            this.options = options;
        },
                onLogout: function(checksso) {
            var url = UrlLinks.logOutUrl(),
            that = this;
            $.ajax({
                url: url,
                type : 'GET',
                headers : {
                    "cache-control" : "no-cache"
                },
                success : function() {
                    if(!_.isUndefined(checksso) && checksso){
                        if(checksso == 'false'){
                            window.location.replace('locallogin');
                        }else{
                            that.errorAction();
                        }
                    } else {
                        Utils.localStorage.setValue("atlas_ui", "classic");
                        window.location.replace('login.jsp');
                    }
                }
            });
        },
        errorAction: function() {
            require(["views/common/ErrorView"], function(ErrorView) {
                var errorView = new ErrorView({ status: "checkSSOTrue" });
                App.rContent.show(errorView);
            });

        },
        checkKnoxSSO: function() {
            var that =this
            var url = UrlLinks.checkKnoxSsoApiUrl();
            $.ajax({
                url : url,
                type : 'GET',
                headers : {
                    "cache-control" : "no-cache"
                },
                success : function(resp) {
                    if (localStorage.getItem('idleTimerLoggedOut') == "true" && resp == "true") {
                        window.location.replace('index.html?action=timeout');
                    } else {
//                        if (Globals.idealTimeoutSeconds > 0 && resp == "true") {
//                            window.location.replace('index.html?action=timeout');
//                        } else {
                            that.onLogout(resp);
//                        }
                    }
                },
                error : function(jqXHR, textStatus, err ) {
                    if( jqXHR.status == 419 ){
                        window.location.replace('login.jsp');
                    }
                }
            });
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
            this.renderDownloadSearchResultview();
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
        renderDownloadSearchResultview: function() {
            var that = this;
            require(['views/site/DownloadSearchResultLayoutView'], function(DownloadSearchResultLayoutView) {
                that.RDownloadSearchResult.show(new DownloadSearchResultLayoutView(that.options));
            });
        }
    });
    return Header;
});