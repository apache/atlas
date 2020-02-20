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

define([
    "require",
    "backbone",
    "hbs!tmpl/name_space/NameSpaceContainerLayoutView_tmpl",
    'collection/VEntityList',
    "utils/Utils",
    "utils/Messages",
    "utils/Globals",
    "utils/UrlLinks",
    "models/VTag"
], function(require, Backbone, NameSpaceContainerLayoutViewTmpl, VEntityList, Utils, Messages, Globals, UrlLinks, VTag) {
    "use strict";

    var NameSpaceContainerLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends NameSpaceContainerLayoutView */
        {
            _viewName: "NameSpaceContainerLayoutView",

            template: NameSpaceContainerLayoutViewTmpl,

            /** Layout sub regions */
            regions: {
                RNameSpaceDetailContainer: "#r_nameSpaceDetailContainer",
                RNameSpaceAttrContainer: "#r_nameSpaceAttrContainer"
            },

            /** ui selector cache */
            ui: {},
            /** ui events hash */
            events: function() {},
            /**
             * intialize a new TagLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this.options, options);
                this.selectedNameSpace = new VEntityList();
                this.selectedNameSpace.url = UrlLinks.nameSpaceGuidApiUrl(this.options.namespaceID);
                this.nameSpaceAttr = new VEntityList();
            },
            bindEvents: function() {},
            onRender: function() {
                this.fetchNameSpaceGuid();
                this.options.nameSpaceCollection.fullCollection.sort({ silent: true });
                this.options.nameSpaceCollection.comparator = function(model) {
                    return -model.get('timestamp');
                }
                this.renderNameSpaceDetailLayoutView(this.options);
                this.renderNameSpaceAttrLayoutView(this.options);
            },
            fetchNameSpaceGuid: function() {
                var that = this;
                this.selectedNameSpace.fetch({
                    complete: function(model, status) {
                        that.nameSpaceAttr.fullCollection.add(model.responseJSON.attributeDefs);
                    }
                });
            },
            renderNameSpaceDetailLayoutView: function(options) {
                var that = this;
                require(["views/name_space/NameSpaceDetailLayoutView"], function(NameSpaceDetailLayoutView) {
                    if (that.isDestroyed) {
                        return;
                    }
                    that.RNameSpaceDetailContainer.show(
                        new NameSpaceDetailLayoutView({
                            nameSpaceVent: that.options.nameSpaceVent,
                            nameSpaceCollection: that.options.nameSpaceCollection,
                            nameSpaceAttr: that.nameSpaceAttr,
                            guid: that.options.namespaceID,
                            enumDefCollection: that.enumDefCollection,
                            typeHeaders: that.typeHeaders
                        })
                    );
                });
            },
            renderNameSpaceAttrLayoutView: function(options) {
                var that = this;
                require(['views/name_space/NameSpaceAttrTableLayoutView'], function(NameSpaceAttrTableLayoutView) {
                    if (that.isDestroyed) {
                        return;
                    }
                    that.RNameSpaceAttrContainer.show(
                        new NameSpaceAttrTableLayoutView({
                            nameSpaceVent: that.options.nameSpaceVent,
                            nameSpaceCollection: that.options.nameSpaceCollection,
                            nameSpaceAttr: that.nameSpaceAttr,
                            guid: that.options.namespaceID,
                            typeHeaders: that.typeHeaders,
                            enumDefCollection: that.enumDefCollection,
                            selectedNameSpace:that.selectedNameSpace
                        }));
                });
            }
        }
    );
    return NameSpaceContainerLayoutView;
});