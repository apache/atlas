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
    'hbs!tmpl/site/header',
], function(require, tmpl) {
    'use strict';

    var Header = Marionette.LayoutView.extend({
        template: tmpl,
        templateHelpers: function() {
            return {
                urlType: this.urlType
            };
        },
        regions: {},
        events: {},
        initialize: function(options) {
            var url = window.location.href.split("/");
            this.urlType = url[url.length - 1];
            /*if we us only old ui then uncomment this condition*/
            if (this.urlType == "") {
                this.urlType = "assetPage";
            }
        },
        onRender: function() {},
        addTagsFileds: function() {
            var that = this;
            require(['views/tag/createTagsLayoutView'], function(createTagsLayoutView) {
                var view = new createTagsLayoutView({
                    vent: that.vent
                });
            });
        }
    });
    return Header;
});
