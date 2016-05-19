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
    'hbs!tmpl/business_catalog/BusinessCatalogLayoutView_tmpl',
    'utils/Utils',
    'collection/VCatalogList',
    'utils/CommonViewFunction'
], function(require, Backbone, BusinessCatalogLayoutViewTmpl, Utils, VCatalogList, CommonViewFunction) {
    'use strict';

    var BusinessCatalogLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends BusinessCatalogLayoutView */
        {
            _viewName: 'BusinessCatalogLayoutView',

            template: BusinessCatalogLayoutViewTmpl,

            /** Layout sub regions */
            regions: {},
            /** ui selector cache */
            ui: {
                Parent: '[data-id="Parent"]',
                chiledList: '[data-id="chiledList"]',
                liClick: 'li a[data-href]',
                backTaxanomy: '[data-id="backTaxanomy"]',
            },
            /** ui events hash */
            events: function() {
                var events = {};
                events['dblclick ' + this.ui.liClick] = function(e) {
                    $(e.currentTarget).parent('li').find('.tools .taxanomyloader').show();
                    this.singleClick = false;
                    this.forwardClick(e, true);
                };
                events['click ' + this.ui.liClick] = function(e) {
                    this.dblClick = false;
                    this.forwardClick(e);
                };
                events['click ' + this.ui.backTaxanomy] = function(e) {
                    this.backButtonTaxanomy();
                };
                return events;
            },
            /**
             * intialize a new BusinessCatalogLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'globalVent', 'url'));
                this.parentCollection = new VCatalogList();
                this.chiledCollection = new VCatalogList();
                this.dblClick = false;
                this.singleClick = false;
            },
            bindEvents: function() {
                var that = this;
                this.listenTo(this.parentCollection, 'reset', function() {
                    this.dblClick = false;
                    this.generateTree(true);
                }, this);
                this.listenTo(this.chiledCollection, 'reset', function() {
                    this.dblClick = false;
                    this.generateTree();
                }, this);
                this.listenTo(this.chiledCollection, 'error', function(model, response) {
                    if (response && response.responseJSON && response.responseJSON.message) {
                        Utils.notifyError({
                            content: response.responseJSON.message
                        });
                    }
                    this.$('.taxanomyloader').hide();
                    this.$('.contentLoading').hide();
                }, this);
                this.listenTo(this.parentCollection, 'error', function(model, response) {
                    if (response && response.responseJSON && response.responseJSON.message) {
                        Utils.notifyError({
                            content: response.responseJSON.message
                        });
                    }
                    this.$('.taxanomyloader').hide();
                    this.$('.contentLoading').hide();
                }, this);
            },
            onRender: function() {
                var that = this;
                this.bindEvents();
                that.ui.backTaxanomy.hide();
                this.fetchCollection(this.url, true);
                $('body').on("click", '.termPopoverList li', function(e) {
                    that[$(this).find("a").data('fn')](e);
                });
                $('body').click(function(e) {
                    if ($('.termPopoverList').length) {
                        if ($(e.target).hasClass('termPopover')) {
                            return;
                        }
                        that.$('.termPopover').popover('hide');
                    }
                });
            },
            manualRender: function(url, isParent) {
                this.fetchCollection(url, isParent);
            },
            fetchCollection: function(url, isParent) {
                if (url) {
                    this.url = url;
                } else {
                    var parentURL = this.ui.Parent.find('a').data('href');
                    if (parentURL) {
                        this.url = parentURL;
                    } else {
                        this.url = "api/atlas/v1/taxonomies";
                    }

                }
                this.$('.taxanomyloader').show();
                this.$('.contentLoading').show();
                if (isParent) {
                    this.parentCollection.url = this.url;
                    this.parentCollection.fullCollection.reset(undefined, { silent: true });
                    this.parentCollection.fetch({ reset: true });
                } else {
                    this.chiledCollection.url = this.url + "?hierarchy/path:.";
                    this.chiledCollection.fullCollection.reset(undefined, { silent: true });
                    this.chiledCollection.fetch({ reset: true });
                }
            },
            forwardClick: function(e, forward) {
                var hrefUrl = $(e.currentTarget).data('href');
                if (forward) {
                    this.dblClick = true;
                    this.fetchCollection(hrefUrl, true);
                } else {
                    this.singleClick = true;
                }
                Utils.setUrl({
                    url: '#!/taxonomy/detailCatalog' + hrefUrl,
                    mergeBrowserUrl: false,
                    updateTabState: function() {
                        return { taxonomyUrl: this.url, stateChanged: false };
                    },
                    trigger: true
                });
                this.addActiveClass(e);
            },
            addActiveClass: function(e) {
                $(e.currentTarget).parents('ul.taxonomyTree').find('li').removeClass('active');
                $(e.currentTarget).parent('li').addClass('active');
            },
            generateTree: function(isParent) {
                var parentLi = "",
                    chiledLi = "",
                    that = this;

                function createTaxonomy(url) {
                    var href = false;
                    _.each(that.parentCollection.fullCollection.models, function(model, key) {

                        if (model.get('terms')) {
                            href = model.get('terms').href;
                        }
                        var hrefUrl = "/api" + model.get('href').split("/api")[1];
                        if (hrefUrl) {
                            var backUrlCheck = hrefUrl.split("taxonomies/");
                            if (backUrlCheck.length > 1) {
                                if (backUrlCheck[1].split("/terms").length <= 1) {
                                    that.ui.backTaxanomy.hide();
                                } else {
                                    that.ui.backTaxanomy.show();
                                }
                            }
                        }
                        parentLi = '<div class="tools"><i class="fa fa-refresh fa-spin-custom taxanomyloader"></i><i class="fa fa-ellipsis-h termPopover"></i></div><a href="javascript:void(0)" data-href=' + hrefUrl + '>' + model.get('name') + '</a>';
                    });
                    if (href) {
                        that.fetchCollection(href);
                    }
                    that.ui.chiledList.html('');
                    that.ui.Parent.addClass('active');
                    that.ui.Parent.html(parentLi);
                }

                function createTerm() {
                    _.each(that.chiledCollection.fullCollection.models, function(model, key) {
                        chiledLi += '<li class="children"><div class="tools"><i class="fa fa-refresh fa-spin-custom taxanomyloader"></i><i class="fa fa-ellipsis-h termPopover" ></i></div><a href="javascript:void(0)" data-href=/api' + model.get('href').split("/api")[1] + '>' + model.get('name') + '</a></li>';
                    });
                    that.ui.chiledList.html(chiledLi);
                }

                if (isParent) {
                    createTaxonomy();
                } else {
                    createTerm();

                }
                this.$('.taxanomyloader').hide();
                this.$('.contentLoading').hide();
                this.$('.termPopover').popover({
                    placement: 'bottom',
                    html: true,
                    trigger: 'manual',
                    container: 'body',
                    content: function() {
                        return "<ul class='termPopoverList'>" +
                            "<li class='listTerm' ><i class='fa fa-search'></i> <a href='javascript:void(0)' data-fn='onSearchTerm'>Search Asset</a></li>" +
                            "<li class='listTerm'><i class='fa fa-plus'></i> <a href='javascript:void(0)' data-fn='onAddTerm'>Add Subterm</a></li>" +
                            /* "<li class='listTerm' ><i class='fa fa-arrow-right'></i> <a href='javascript:void(0)' data-fn='moveTerm'>Move Term</a></li>" +
                             "<li class='listTerm' ><i class='fa fa-edit'></i> <a href='javascript:void(0)' data-fn='onEditTerm'>Edit Term</a></li>" +
                             "<li class='listTerm'><i class='fa fa-trash'></i> <a href='javascript:void(0)' data-fn='deleteTerm'>Delete Term</a></li>" +*/
                            "</ul>";
                    }
                });
                this.$('.termPopover').off('click').on('click', function(e) {
                    // if any other popovers are visible, hide them
                    e.preventDefault();
                    that.$('.termPopover').not(this).popover('hide');
                    $(this).popover('toggle');
                });
            },
            onAddTerm: function(e) {
                var that = this;
                require([
                    'views/business_catalog/AddTermLayoutView',
                    'modules/Modal'
                ], function(AddTermLayoutView, Modal) {
                    var view = new AddTermLayoutView({
                        url: that.$('.taxonomyTree').find('li.active').find("a").data("href"),
                        model: new that.parentCollection.model()
                    });
                    var modal = new Modal({
                        title: 'Add Term',
                        content: view,
                        okCloses: true,
                        showFooter: true,
                        allowCancel: true,
                        okText: 'Create',
                    }).open();
                    modal.on('ok', function() {
                        that.saveAddTerm(view);
                    });
                    view.on('closeModal', function() {
                        modal.trigger('cancel');
                    });

                });
            },
            saveAddTerm: function(view) {
                var that = this;
                view.model.url = view.url + "/terms/" + view.ui.termName.val();
                view.model.set({ description: view.ui.termDetail.val() }).save(null, {
                    success: function(model, response) {
                        that.fetchCollection(that.url);
                        Utils.notifySuccess({
                            content: "Term Created successfully"
                        });
                    },
                    error: function(model, response) {
                        Utils.notifyError({
                            content: response.responseJSON.message
                        });
                    }
                });
            },
            deleteTerm: function(e) {
                var tagName = this.$('.taxonomyTree').find('li.active').find("a").text(),
                    that = this,
                    modal = CommonViewFunction.deleteTagModel(tagName);
                modal.on('ok', function() {
                    that.deleteTagData(e);
                });
                modal.on('closeModal', function() {
                    modal.trigger('cancel');
                });
            },
            deleteTagData: function(e) {
                var that = this,
                    tagName = this.$('.taxonomyTree').find('li.active').find("a").text(),
                    guid = $(e.target).data("guid");
                CommonViewFunction.deleteTag({
                    'tagName': tagName,
                    'guid': guid,
                    'collection': that.parentCollection
                });
            },
            moveTerm: function() {
                var that = this;
                require([
                    'views/business_catalog/MoveTermLayoutView',
                    'modules/Modal'
                ], function(MoveTermLayoutView, Modal) {
                    var view = new MoveTermLayoutView({
                        taxanomyCollection: that.collection
                    });
                    var modal = new Modal({
                        title: 'Move Term',
                        content: view,
                        okCloses: true,
                        showFooter: true,
                        allowCancel: true,
                        okText: 'Move',
                    }).open();
                    // modal.on('ok', function() {
                    //     that.saveAddTerm(view);
                    // });
                    view.on('closeModal', function() {
                        modal.trigger('cancel');
                    });
                });
            },
            onSearchTerm: function() {
                Utils.setUrl({
                    url: '#!/search/searchResult',
                    urlParams: {
                        query: this.$('.taxonomyTree').find('li.active').find("a").text(),
                        searchType: "fulltext",
                        dslChecked: false
                    },
                    updateTabState: function() {
                        return { searchUrl: this.url, stateChanged: true };
                    },
                    mergeBrowserUrl: false,
                    trigger: true
                });
            },
            backButtonTaxanomy: function(e) {
                var that = this;
                this.dblClick = false;
                var dataURL = this.$('.taxonomyTree').find('li[data-id="Parent"]').find("a").data('href').split("/terms");
                var backUrl = dataURL.pop();
                if (dataURL.join("/terms").length) {
                    this.ui.backTaxanomy.show();
                    var currentURL = "!/taxonomy/detailCatalog" + dataURL.join("/terms");
                    Utils.setUrl({
                        url: currentURL,
                        mergeBrowserUrl: false,
                        trigger: true,
                        updateTabState: function() {
                            return { taxonomyUrl: currentURL, stateChanged: false };
                        }
                    });
                }
            }
        });
    return BusinessCatalogLayoutView;
});
