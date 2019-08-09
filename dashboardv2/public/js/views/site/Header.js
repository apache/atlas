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
    'utils/UrlLinks',
    'jquery-ui'
], function(require, tmpl, CommonViewFunction, Globals, Utils, UrlLinks) {
    'use strict';

    var Header = Marionette.LayoutView.extend({
        template: tmpl,
        regions: {},
        ui: {
            backButton: "[data-id='backButton']",
            menuHamburger: "[data-id='menuHamburger']",
            globalSearch: "[data-id='globalSearch']",
            clearGlobalSearch: "[data-id='clearGlobalSearch']",
            signOut: "[data-id='signOut']"
        },
        events: function() {
            var events = {};
            events['click ' + this.ui.backButton] = function() {
                var queryParams = Utils.getUrlState.getQueryParams(),
                    urlPath = "searchUrl";
                if (queryParams && queryParams.from) {
                    if (queryParams.from == "classification") {
                        urlPath = "tagUrl";
                    } else if (queryParams.from == "glossary") {
                        urlPath = "glossaryUrl";
                    }
                }
                Utils.setUrl({
                    url: Globals.saveApplicationState.tabState[urlPath],
                    mergeBrowserUrl: false,
                    trigger: true,
                    updateTabState: true
                });

            };
            events['click ' + this.ui.clearGlobalSearch] = function() {
                this.ui.globalSearch.val("");
                this.ui.globalSearch.autocomplete("search");
                this.ui.clearGlobalSearch.removeClass("in");
            };
            events['click ' + this.ui.menuHamburger] = function() {
                this.setSearchBoxWidth({
                    updateWidth: function(atlasHeaderWidth) {
                        return $('body').hasClass('full-screen') ? atlasHeaderWidth - 350 : atlasHeaderWidth + 350
                    }
                });
                $('body').toggleClass("full-screen");
            };
            events['click ' + this.ui.signOut] = function() {
                var path = Utils.getBaseUrl(window.location.pathname);
                window.location = path + "/logout.html";
            };
            return events;

        },
        initialize: function(options) {
            this.bindEvent();
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
        },
        onRender: function() {
            var that = this;
            if (Globals.userLogedIn.status) {
                that.$('.userName').html(Globals.userLogedIn.response.userName);
            }
            this.initializeGlobalSearch();
        },
        onShow: function() {
            this.setSearchBoxWidth();
        },
        onBeforeDestroy: function() {
            this.ui.globalSearch.atlasAutoComplete("destroy");
        },
        manualRender: function() {
            this.setSearchBoxWidth();
        },
        fetchSearchData: function(options) {
            var that = this,
                request = options.request,
                response = options.response,
                term = request.term,
                data = {},
                sendResponse = function() {
                    var query = data.query,
                        suggestions = data.suggestions;
                    if (query !== undefined && suggestions !== undefined) {
                        response(data);
                    }
                };
            $.ajax({
                url: UrlLinks.searchApiUrl('quick'),
                contentType: 'application/json',
                data: {
                    "query": this.getSearchString(term),
                    "limit": 5,
                    "offset": 0
                },
                cache: true,
                success: function(response) {
                    var rData = response.searchResults.entities || [];
                    data.query = { category: "entities", data: rData, order: 1 };
                    sendResponse();
                }
            });

            $.ajax({
                url: UrlLinks.searchApiUrl('suggestions'),
                contentType: 'application/json',
                data: {
                    "prefixString": term
                },
                cache: true,
                success: function(response) {
                    var rData = response.suggestions || [];
                    data.suggestions = { category: "suggestions", data: rData, order: 2 };
                    sendResponse();
                }
            });
        },
        getSearchString: function(str) {
            if (str && str.length) {
                return (str.match(/[+\-&|!(){}[\]^"~*?:/]/g) === null ? (str + "*") : str);
            } else {
                return str;
            }
        },
        triggerBuasicSearch: function(query) {
            Utils.setUrl({
                url: '#!/search/searchResult?query=' + encodeURIComponent(query) + '&searchType=basic',
                mergeBrowserUrl: false,
                trigger: true,
                updateTabState: true
            });
        },
        initializeGlobalSearch: function() {
            var that = this;
            this.ui.globalSearch.atlasAutoComplete({
                minLength: 1,
                autoFocus: false,
                search: function() {
                    $(this).siblings('span.fa-search').removeClass("fa-search").addClass("fa-refresh fa-spin-custom");
                },
                focus: function(event, ui) {
                    return false;
                },
                open: function() {
                    $(this).siblings('span.fa-refresh').removeClass("fa-refresh fa-spin-custom").addClass("fa-search");
                },
                select: function(event, ui) {
                    var item = ui && ui.item;
                    event.preventDefault();
                    event.stopPropagation();
                    var $el = $(this);
                    if (_.isString(item)) {
                        $el.val(item);
                        $el.data("valSelected", true);
                        that.triggerBuasicSearch(item);
                    } else if (_.isObject(item) && item.guid) {
                        Utils.setUrl({
                            url: '#!/detailPage/' + item.guid,
                            mergeBrowserUrl: false,
                            trigger: true
                        });
                    }
                    $el.blur();
                    return true;
                },
                source: function(request, response) {
                    that.fetchSearchData({
                        request: request,
                        response: response
                    });
                }
            }).focus(function() {
                $(this).atlasAutoComplete("search");
            }).keyup(function(event) {
                if ($(this).val().trim() === "") {
                    that.ui.clearGlobalSearch.removeClass("in");
                } else {
                    that.ui.clearGlobalSearch.addClass("in");
                    if (event.keyCode == 13) {
                        if ($(this).data("valSelected") !== true) {
                            that.triggerBuasicSearch(that.getSearchString($(this).val()));
                        } else {
                            $(this).data("valSelected", false);
                        }
                    }
                }
            }).atlasAutoComplete("instance")._renderItem = function(ul, searchItem) {

                if (searchItem) {
                    var data = searchItem.data,
                        searchTerm = this.term,
                        getHighlightedTerm = function(resultStr) {
                            try {
                                return resultStr.replace(new RegExp(searchTerm, "gi"), function(foundStr) {
                                    return "<span class='searched-term'>" + foundStr + "</span>";
                                });
                            } catch (error) {
                                return resultStr;
                            }
                        }
                    if (data) {
                        if (data.length == 0) {
                            return $("<li class='empty'></li>")
                                .append("<span class='empty-message'>No " + searchItem.category + " found</span>")
                                .appendTo(ul);
                        } else {
                            var items = [];
                            _.each(data, function(item) {
                                var li = null;
                                if (_.isObject(item)) {
                                    item.itemText = Utils.getName(item) + " (" + item.typeName + ")";
                                    var options = {},
                                        table = '';
                                    options.entityData = item;
                                    var img = $('<img src="' + Utils.getEntityIconPath(options) + '">').on('error', function(error, s) {
                                        this.src = Utils.getEntityIconPath(_.extend(options, { errorUrl: this.src }));
                                    });
                                    var span = $("<span>" + (getHighlightedTerm(item.itemText)) + "</span>")
                                        .prepend(img);
                                    li = $("<li class='with-icon'>")
                                        .append(span);
                                } else {
                                    li = $("<li>")
                                        .append("<span>" + (getHighlightedTerm(item)) + "</span>");
                                }
                                li.data("ui-autocomplete-item", item);
                                if (searchItem.category) {
                                    items.push(li.attr("aria-label", searchItem.category + " : " + (_.isObject(item) ? item.itemText : item)));
                                }
                            });
                            return ul.append(items);
                        }
                    }
                }
            };
        }
    });
    return Header;
});