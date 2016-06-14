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
    'hbs!tmpl/business_catalog/BusinessCatalogLayoutView_tmpl'
], function(require, Backbone, BusinessCatalogLayoutViewTmpl) {
    'use strict';

    var BusinessCatalogLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends BusinessCatalogLayoutView */
        {
            _viewName: 'BusinessCatalogLayoutView',

            template: BusinessCatalogLayoutViewTmpl,

            /** Layout sub regions */
            regions: {
                RTreeLayoutView: "#r_treeLayoutView"
            },
            /** ui selector cache */
            ui: {},
            /** ui events hash */
            events: function() {},
            /**
             * intialize a new BusinessCatalogLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'url'));
            },
            bindEvents: function() {},
            onRender: function() {
                this.renderTreeLayoutView();
            },
            renderTreeLayoutView: function() {
                var that = this;
                require(['views/business_catalog/TreeLayoutView'], function(TreeLayoutView) {
                    that.RTreeLayoutView.show(new TreeLayoutView({
                        url: that.url,
                        viewBased: true
                    }));
                });
            },
            manualRender: function(url, isParent, back) {
                this.RTreeLayoutView.currentView.manualRender(url, isParent, back);
            }
        });
    return BusinessCatalogLayoutView;
});
