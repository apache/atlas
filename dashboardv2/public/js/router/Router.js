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
    'utils/Utils',
    'utils/UrlLinks',
    'collection/VGlossaryList'
], function($, _, Backbone, App, Globals, Utils, UrlLinks, VGlossaryList) {
    var AppRouter = Backbone.Router.extend({
        routes: {
            // Define some URL routes
            '': 'defaultAction',
            '!/': 'tagAttributePageLoad',
            '!/tag/tagAttribute/(*name)': 'tagAttributePageLoad',
            '!/search/searchResult': 'searchResult',
            '!/detailPage/:id': 'detailPage',
            '!/tag': 'commonAction',
            '!/search': 'commonAction',
            '!/glossary': 'commonAction',
            '!/glossary/:id': 'glossaryDetailPage',
            // Default
            '*actions': 'defaultAction'
        },
        initialize: function(options) {
            _.extend(this, _.pick(options, 'entityDefCollection', 'typeHeaders', 'enumDefCollection', 'classificationDefCollection', 'entityCountCollection'));
            this.showRegions();
            this.bindCommonEvents();
            this.listenTo(this, 'route', this.postRouteExecute, this);
            this.searchVent = new Backbone.Wreqr.EventAggregator();
            this.glossaryCollection = new VGlossaryList([], {
                comparator: function(item) {
                    return item.get("name");
                }
            });
            this.preFetchedCollectionLists = {
                'entityDefCollection': this.entityDefCollection,
                'typeHeaders': this.typeHeaders,
                'enumDefCollection': this.enumDefCollection,
                'classificationDefCollection': this.classificationDefCollection,
                'glossaryCollection': this.glossaryCollection,
                'entityCountCollection': this.entityCountCollection
            }
            this.sharedObj = {
                searchTableColumns: {},
                glossary: {
                    selectedItem: {}
                },
                searchTableFilters: {
                    tagFilters: {},
                    entityFilters: {}
                }
            }
        },
        bindCommonEvents: function() {
            var that = this;
            $('body').on('click', 'a.show-stat', function() {
                require([
                    'views/site/Statistics',
                ], function(Statistics) {
                    new Statistics();
                });
            });
            $('body').on('click', 'li.aboutAtlas', function() {
                require([
                    'views/site/AboutAtlas',
                ], function(AboutAtlas) {
                    new AboutAtlas();
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
            $(".tooltip").tooltip("hide");
            // console.log("Pre-Route Change Operations can be performed here !!");
        },
        postRouteExecute: function(name, args) {
            // console.log("Post-Route Change Operations can be performed here !!");
            // console.log("Route changed: ", name);
        },
        detailPage: function(id) {
            var that = this;
            if (id) {
                require([
                    'views/site/Header',
                    'views/detail_page/DetailPageLayoutView',
                    'views/site/SideNavLayoutView',
                    'collection/VEntityList'
                ], function(Header, DetailPageLayoutView, SideNavLayoutView, VEntityList) {
                    this.entityCollection = new VEntityList([], {});
                    var paramObj = Utils.getUrlState.getQueryParams();
                    App.rNHeader.show(new Header());
                    if (!App.rSideNav.currentView) {
                        App.rSideNav.show(new SideNavLayoutView(
                            _.extend({}, that.preFetchedCollectionLists, that.sharedObj)
                        ));
                    } else {
                        App.rSideNav.currentView.selectTab();
                    }
                    App.rNContent.show(new DetailPageLayoutView(_.extend({
                        'collection': this.entityCollection,
                        'id': id,
                        'value': paramObj
                    }, that.preFetchedCollectionLists, that.sharedObj)));
                    this.entityCollection.url = UrlLinks.entitiesApiUrl({ guid: id, minExtInfo: true });
                    this.entityCollection.fetch({ reset: true });
                });
            }
        },
        tagAttributePageLoad: function(tagName) {
            var that = this;
            require([
                'views/site/Header',
                'views/site/SideNavLayoutView',
                'views/tag/TagDetailLayoutView',
            ], function(Header, SideNavLayoutView, TagDetailLayoutView) {
                var paramObj = Utils.getUrlState.getQueryParams(),
                    url = Utils.getUrlState.getQueryUrl().queyParams[0];
                App.rNHeader.show(new Header());
                if (!App.rSideNav.currentView) {
                    if (paramObj && paramObj.dlttag) {
                        Utils.setUrl({
                            url: url,
                            trigger: false,
                            updateTabState: true
                        });
                    }
                    App.rSideNav.show(new SideNavLayoutView(
                        _.extend({
                            'tag': tagName,
                            'value': paramObj
                        }, that.preFetchedCollectionLists, that.sharedObj)
                    ));
                } else {
                    if (paramObj && paramObj.dlttag) {
                        Utils.setUrl({
                            url: url,
                            trigger: false,
                            updateTabState: true
                        });
                    }
                    App.rSideNav.currentView.RTagLayoutView.currentView.manualRender(_.extend({}, paramObj, { 'tagName': tagName }));
                    App.rSideNav.currentView.selectTab();
                }
                if (tagName) {
                    // updating paramObj to check for new queryparam.
                    paramObj = Utils.getUrlState.getQueryParams();
                    if (paramObj && paramObj.dlttag) {
                        return false;
                    }
                    App.rNContent.show(new TagDetailLayoutView(
                        _.extend({
                            'tag': tagName,
                            'value': paramObj
                        }, that.preFetchedCollectionLists, that.sharedObj)
                    ));
                }
            });
        },
        glossaryDetailPage: function(id) {
            var that = this;
            if (id) {
                require([
                    'views/site/Header',
                    'views/glossary/GlossaryDetailLayoutView',
                    'views/site/SideNavLayoutView'
                ], function(Header, GlossaryDetailLayoutView, SideNavLayoutView) {
                    var paramObj = Utils.getUrlState.getQueryParams();
                    App.rNHeader.show(new Header());
                    if (!App.rSideNav.currentView) {
                        App.rSideNav.show(new SideNavLayoutView(
                            _.extend({}, that.preFetchedCollectionLists, that.sharedObj, { 'guid': id, 'value': paramObj })
                        ));
                    } else {
                        App.rSideNav.currentView.RGlossaryLayoutView.currentView.manualRender(_.extend({}, { 'guid': id, 'value': paramObj }));
                        App.rSideNav.currentView.selectTab();
                    }
                    App.rNContent.show(new GlossaryDetailLayoutView(_.extend({
                        'guid': id,
                        'value': paramObj
                    }, that.preFetchedCollectionLists, that.sharedObj)));
                });
            }
        },
        commonAction: function() {
            var that = this;
            require([
                'views/site/Header',
                'views/site/SideNavLayoutView',
                'views/search/SearchDetailLayoutView',
            ], function(Header, SideNavLayoutView, SearchDetailLayoutView) {
                var paramObj = Utils.getUrlState.getQueryParams();
                App.rNHeader.show(new Header());
                if (!App.rSideNav.currentView) {
                    App.rSideNav.show(new SideNavLayoutView(
                        _.extend({
                            'searchVent': that.searchVent
                        }, that.preFetchedCollectionLists, that.sharedObj)
                    ));
                } else {
                    App.rSideNav.currentView.selectTab();
                    if (Utils.getUrlState.isTagTab()) {
                        App.rSideNav.currentView.RTagLayoutView.currentView.manualRender();
                    } else if (Utils.getUrlState.isGlossaryTab()) {
                        App.rSideNav.currentView.RGlossaryLayoutView.currentView.manualRender(_.extend({ "isTrigger": true }, { "value": paramObj }));
                    }
                }

                if (Globals.entityCreate && Utils.getUrlState.isSearchTab()) {
                    App.rNContent.show(new SearchDetailLayoutView(
                        _.extend({
                            'value': paramObj,
                            'initialView': true,
                            'searchVent': that.searchVent
                        }, that.preFetchedCollectionLists, that.sharedObj)
                    ));
                } else {
                    App.rNContent.$el.html("");
                    App.rNContent.destroy();
                }
            });
        },
        searchResult: function() {
            var that = this;
            require([
                'views/site/Header',
                'views/site/SideNavLayoutView',
                'views/search/SearchDetailLayoutView'
            ], function(Header, SideNavLayoutView, SearchDetailLayoutView) {
                var paramObj = Utils.getUrlState.getQueryParams();
                var isinitialView = true,
                    isTypeTagNotExists = false,
                    tempParam = _.extend({}, paramObj);
                App.rNHeader.show(new Header());
                if (!App.rSideNav.currentView) {
                    App.rSideNav.show(new SideNavLayoutView(
                        _.extend({
                            'value': paramObj,
                            'searchVent': that.searchVent
                        }, that.preFetchedCollectionLists, that.sharedObj)
                    ));
                } else {
                    App.rSideNav.currentView.RSearchLayoutView.currentView.manualRender(paramObj);
                }
                App.rSideNav.currentView.selectTab();
                if (paramObj) {
                    isinitialView = (paramObj.type || (paramObj.dslChecked == "true" ? "" : (paramObj.tag || paramObj.term)) || (paramObj.query ? paramObj.query.trim() : "")).length === 0;
                }
                App.rNContent.show(new SearchDetailLayoutView(
                    _.extend({
                        'value': paramObj,
                        'searchVent': that.searchVent,
                        'initialView': isinitialView,
                        'isTypeTagNotExists': ((paramObj.type != tempParam.type) || (tempParam.tag != paramObj.tag))
                    }, that.preFetchedCollectionLists, that.sharedObj)
                ));
            });
        },
        defaultAction: function(actions) {
            // We have no matching route, lets just log what the URL was
            Utils.setUrl({
                url: '#!/search',
                mergeBrowserUrl: false,
                trigger: true,
                updateTabState: true
            });

            console.log('No route:', actions);
        }
    });
    return AppRouter;
});