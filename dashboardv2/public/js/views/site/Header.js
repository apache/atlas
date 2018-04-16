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
    'utils/Utils'
], function(require, tmpl, CommonViewFunction, Globals, Utils) {
    'use strict';

    var Header = Marionette.LayoutView.extend({
        template: tmpl,
        regions: {},
        ui: {
            backButton: "[data-id='backButton']",
            menuHamburger: "[data-id='menuHamburger']",
        },
        events: function() {
            var events = {};
            events['click ' + this.ui.backButton] = function() {
                var queryParams = Utils.getUrlState.getQueryParams(),
                    urlPath = "searchUrl";
                if (queryParams && queryParams.from) {
                    if (queryParams.from == "classification") {
                        urlPath = "tagUrl";
                    } else if(queryParams.from == "glossary"){
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
        },
    });
    return Header;
});