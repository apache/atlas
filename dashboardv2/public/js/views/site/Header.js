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
            clearGlobalSearch: "[data-id='clearGlobalSearch']"
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
                $('body').toggleClass("full-screen");
            };
            return events;

        },
        initialize: function(options) {},

        onRender: function() {
            var that = this;
            if (Globals.userLogedIn.status) {
                that.$('.userName').html(Globals.userLogedIn.response.userName);
            }
            this.initializeGlobalSearch();
        },
        getSearchUrlQueryParam: function(request) {
            var term = request.term;
            return {
                "excludeDeletedEntities": true,
                "includeSubClassifications": true,
                "includeSubTypes": true,
                "includeClassificationAttributes": true,
                "entityFilters": null,
                "tagFilters": null,
                "attributes": null,
                "query": this.getSearchString(term),
                "limit": 5,
                "offset": 0,
                "typeName": null,
                "classification": null,
                "termName": null
            }
        },
        getSearchString: function(str) {
            if (str && str.length) {
                return (str.match(/[+\-&|!(){}[\]^"~*?:/]/g) === null ? (str + "*") : str);
            } else {
                return str;
            }
        },
        initializeGlobalSearch: function() {
            var that = this;
            this.cache = {};
            this.ui.globalSearch.autocomplete({
                minLength: 1,
                autoFocus: false,
                search: function() {
                    $(this).siblings('span.fa-search').removeClass("fa-search").addClass("fa-refresh fa-spin-custom");
                },
                focus: function(event, ui) {
                    //$(this).val(ui.item.itemText);
                    return false;
                },
                open: function() {
                    $(this).siblings('span.fa-refresh').removeClass("fa-refresh fa-spin-custom").addClass("fa-search");
                },
                select: function(event, ui) {
                    if (ui && ui.item && ui.item.value == "Empty") {
                        return false
                    } else {
                        Utils.setUrl({
                            url: '#!/detailPage/' + ui.item.guid,
                            mergeBrowserUrl: false,
                            trigger: true
                        });
                        return true
                    }
                },
                source: function(request, response) {
                    var term = request.term;
                    if (term in that.cache) {
                        response(that.cache[term]);
                        return;
                    }

                    $.ajax({
                        type: 'POST',
                        url: UrlLinks.searchApiUrl('basic'),
                        contentType: 'application/json',
                        data: JSON.stringify(that.getSearchUrlQueryParam(request)),
                        cache: true,
                        success: function(data) {
                            var data = data.entities;
                            if (data === undefined) {
                                data = ["Empty"];
                            }
                            that.cache[term] = data;
                            response(data);
                        }
                    });
                }
            }).focus(function() {
                $(this).autocomplete("search");
            }).keyup(function(event) {
                if ($(this).val().trim() === "") {
                    that.ui.clearGlobalSearch.removeClass("in");
                } else {
                    that.ui.clearGlobalSearch.addClass("in");
                    if (event.keyCode == 13) {
                        Utils.setUrl({
                            url: '#!/search/searchResult?query=' + encodeURIComponent(that.getSearchString($(this).val())) + '&searchType=basic',
                            mergeBrowserUrl: false,
                            trigger: true
                        });
                    }
                }
            }).autocomplete("instance")._renderItem = function(ul, item) {
                if (item && item.value == "Empty") {
                    return $("<li>")
                        .append("<span class='empty'>No record found</span>")
                        .appendTo(ul);
                }
                item.itemText = Utils.getName(item) + " (" + item.typeName + ")";
                var options = {},
                    table = '';
                options.entityData = item;
                var img = $('<img src="' + Utils.getEntityIconPath(options) + '">').on('error', function(error, s) {
                    this.src = Utils.getEntityIconPath(_.extend(options, { errorUrl: this.src }));
                });
                var link = $("<a class='search-entity-anchor ellipsis' href='#!/detailPage/" + item.guid + "'>" + item.itemText + "</a>").prepend(img);
                return $("<li class='with-icon'>")
                    .append(link)
                    .appendTo(ul);
            };
        }
    });
    return Header;
});