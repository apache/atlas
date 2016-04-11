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
    'jquery',
    'underscore',
    'backbone',
    'App',
    'utils/Globals',
    'utils/Utils'
], function($, _, Backbone, App, Globals, Utils) {
    var AppRouter = Backbone.Router.extend({
        routes: {
            // Define some URL routes
            '': 'assetPageLoad',
            '!/': 'assetPageLoad',
            "!/dashboard/assetPage": 'assetPageLoad',
            '!/dashboard/detailPage/:id': 'detailPageLoad',
            '!/dashboard/createTags': 'tagPageLoad',
            // Default
            '*actions': 'defaultAction'
        },
        initialize: function() {
            this.showRegions();
            this.bindCommonEvents();
            this.listenTo(this, 'route', this.postRouteExecute, this);
            this.globalVent = new Backbone.Wreqr.EventAggregator();
            this.catalogVent = new Backbone.Wreqr.EventAggregator();
            this.tagVent = new Backbone.Wreqr.EventAggregator();
        },
        bindCommonEvents: function() {
            var that = this;
            $('body').on('click', 'li.aboutAtlas', function() {
                that.aboutAtlas();
            });
        },
        aboutAtlas: function() {
            var that = this;
            require([
                'hbs!tmpl/common/aboutAtlas_tmpl',
                'modules/Modal'
            ], function(aboutAtlasTmpl, Modal) {

                var aboutAtlas = Marionette.LayoutView.extend({
                    template: aboutAtlasTmpl,
                    events: {},
                });
                var view = new aboutAtlas();
                var modal = new Modal({
                    title: 'About',
                    content: view,
                    okCloses: true,
                    showFooter: true,
                }).open();

                view.on('closeModal', function() {
                    modal.trigger('cancel');
                });

            });
        },
        showRegions: function() {},

        /**
         * @override
         * Execute a route handler with the provided parameters. This is an
         * excellent place to do pre-route setup or post-route cleanup.
         * @param  {Function} callback - route handler
         * @param  {Array}   args - route params
         */
        execute: function(callback, args) {
            this.preRouteExecute();
            if (callback) callback.apply(this, args);
            this.postRouteExecute();
        },
        preRouteExecute: function() {
            // console.log("Pre-Route Change Operations can be performed here !!");
        },
        postRouteExecute: function(name, args) {
            // console.log("Post-Route Change Operations can be performed here !!");
            // console.log("Route changed: ", name);
        },
        /**
         * Define route handlers here
         */
        assetPageLoad: function() {
            var that = this;
            $('#old').show();
            $('#new').hide();
            require([
                'views/asset/AssetPageLayoutView',
                'collection/VSearchList',
                'views/site/Header',
                /*'views/site/footer',*/
            ], function(AssetPageLayoutView, VSearchList, HeaderView /*, FooterView*/ ) {
                that.searchCollection = new VSearchList([], {
                    state: {
                        firstPage: 0,
                        pageSize: 10
                    }
                });
                App.rHeader.show(new HeaderView({ 'globalVent': that.globalVent }));
                App.rContent.show(new AssetPageLayoutView({
                    'globalVent': that.globalVent,
                    'collection': that.searchCollection,
                    'vent': that.tagVent
                }));
            });
        },
        detailPageLoad: function(id) {
            var that = this;
            $('#old').show();
            $('#new').hide();
            if (id) {
                require([
                    'views/detail_page/DetailPageLayoutView',
                    'collection/VEntityList',
                    'views/site/Header',
                ], function(DetailPageLayoutView, VEntityList, HeaderView) {
                    this.entityCollection = new VEntityList([], {});
                    App.rHeader.show(new HeaderView({ 'globalVent': that.globalVent }));
                    App.rContent.show(new DetailPageLayoutView({
                        'globalVent': that.globalVent,
                        'collection': entityCollection,
                        'id': id,
                        'vent': that.tagVent
                    }));
                    entityCollection.url = "/api/atlas/entities/" + id;
                    entityCollection.fetch({ reset: true });
                });
            }
        },
        tagPageLoad: function() {
            var that = this;
            $('#old').show();
            $('#new').hide();
            require([
                'views/tag/createTagsLayoutView',
                'collection/VTagList',
                'views/site/Header',
            ], function(CreateTagsLayoutView, VTagList, HeaderView) {
                this.tagsCollection = new VTagList([], {});
                App.rHeader.show(new HeaderView({ 'globalVent': that.globalVent }));
                App.rContent.show(new CreateTagsLayoutView({
                    'globalVent': that.globalVent,
                    'tagsCollection': tagsCollection,
                }));

            });
        },

        defaultAction: function(actions) {
            // We have no matching route, lets just log what the URL was
            console.log('No route:', actions);
        }
    });
    return AppRouter;
});
