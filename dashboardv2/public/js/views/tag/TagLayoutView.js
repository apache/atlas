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
    'hbs!tmpl/tag/TagLayoutView_tmpl',
    'collection/VTagList',
    'collection/VEntityList',
    'utils/Utils',
], function(require, Backbone, TagLayoutViewTmpl, VTagList, VEntityList, Utils) {
    'use strict';

    var TagLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends TagLayoutView */
        {
            _viewName: 'TagLayoutView',

            template: TagLayoutViewTmpl,

            /** Layout sub regions */
            regions: {},

            /** ui selector cache */
            ui: {
                tagsParent: "[data-id='tagsParent']",
                createTag: "[data-id='createTag']",
                tags: "[data-id='tags']",
                offLineSearchTag: "[data-id='offlineSearchTag']",
                deleteTerm: "[data-id='deleteTerm']",

            },
            /** ui events hash */
            events: function() {
                var events = {};
                events["click " + this.ui.createTag] = 'onClickCreateTag';
                /* events["dblclick " + this.ui.tags] = function(e) {
                     this.onTagList(e, true);
                 }*/
                events["click " + this.ui.tags] = 'onTagList';
                    // events["click " + this.ui.referesh] = 'refereshClick';
                events["keyup " + this.ui.offLineSearchTag] = 'offlineSearchTag';
                events["click " + this.ui.deleteTerm] = 'onDeleteTerm';
                return events;
            },
            /**
             * intialize a new TagLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'globalVent', 'tag'));
                this.tagCollection = new VTagList();
                this.collection = new Backbone.Collection();

                this.json = {
                    "enumTypes": [],
                    "traitTypes": [],
                    "structTypes": [],
                    "classTypes": []
                };
            },
            bindEvents: function() {
                var that = this;
                this.listenTo(this.tagCollection, "reset", function() {
                    this.tagsAndTypeGenerator('tagCollection');
                    this.createTagAction();
                }, this);
                this.ui.tagsParent.on('click', 'li.parent-node a', function() {
                    that.setUrl(this.getAttribute("href"));
                });
            },
            onRender: function() {
                var that = this;
                this.bindEvents();
                this.fetchCollections();
                $('body').on("click", '.tagPopoverList li', function(e) {
                    that[$(this).find("a").data('fn')](e);
                });
                $('body').click(function(e) {
                    if ($('.tagPopoverList').length) {
                        if ($(e.target).hasClass('tagPopover')) {
                            return;
                        }
                        that.$('.tagPopover').popover('hide');
                    }
                });
            },
            fetchCollections: function() {
                $.extend(this.tagCollection.queryParams, { type: 'TRAIT', });
                this.tagCollection.fetch({ reset: true });
            },
            manualRender: function(tagName) {
                this.setValues(tagName);
            },
            setValues: function(tagName) {
                if (Utils.getUrlState.isTagTab() || Utils.getUrlState.isInitial()) {
                    if (!this.tag && !tagName) {
                        this.selectFirst = false;
                        this.ui.tagsParent.find('li').first().addClass('active');
                        Utils.setUrl({
                            url: this.ui.tagsParent.find('li a').first().attr("href"),
                            mergeBrowserUrl: false,
                            trigger: true
                        });
                    } else {
                        var tag = Utils.getUrlState.getLastValue();
                        if (tagName) {
                            tag = tagName;
                        } else if (this.tag) {
                            tag = this.tag;
                        }
                        this.ui.tagsParent.find('li').removeClass('active');
                        this.ui.tagsParent.find('li').filter(function() {
                            return $(this).text() === tag;
                        }).addClass('active');
                    }
                }
            },
            tagsAndTypeGenerator: function(collection, searchString) {
                var that = this,
                    str = '';
                _.each(this[collection].fullCollection.models, function(model) {
                    var tagName = model.get("tags");
                    if (searchString) {
                        if (tagName.search(new RegExp(searchString, "i")) != -1) {
                            str = '<li class="parent-node" data-id="tags"><div class="tools"><i class="fa fa-trash-o" data-id="deleteTerm"></i></div><a href="#!/tag/tagAttribute/' + tagName + '">' + tagName + '</a></li>' + str;
                        } else {
                            return;
                        }
                    } else {
                        //str = '<li class="parent-node" data-id="tags"><div class="tools"><i class="fa fa-trash-o" data-id="deleteTerm"></i></div><a href="#!/tag/tagAttribute/' + tagName + '">' + tagName + '</a></li>' + str;
                        str = '<li class="parent-node" data-id="tags"><div class="tools"><i class="fa fa-ellipsis-h tagPopover"></i></div><a href="#!/tag/tagAttribute/' + tagName + '">' + tagName + '</a></li>' + str;
                    }
                });
                this.ui.tagsParent.empty().html(str);
                this.setValues();

            },

            onClickCreateTag: function(e) {
                var that = this;
                $(e.currentTarget).blur();
                require([
                    'views/tag/CreateTagLayoutView',
                    'modules/Modal'
                ], function(CreateTagLayoutView, Modal) {
                    var view = new CreateTagLayoutView({ 'tagCollection': that.tagCollection });
                    var modal = new Modal({
                        title: 'Create a new tag',
                        content: view,
                        cancelText: "Cancel",
                        okText: 'Create',
                        allowCancel: true,
                    }).open();
                    modal.$el.find('button.ok').attr("disabled", "true");
                    view.ui.tagName.on('keyup', function(e) {
                        modal.$el.find('button.ok').removeAttr("disabled");
                    });
                    view.ui.tagName.on('keyup', function(e) {
                        if (e.keyCode == 8 && e.currentTarget.value == "") {
                            modal.$el.find('button.ok').attr("disabled", "true");
                        }
                    });
                    modal.on('ok', function() {
                        that.onCreateButton(view);
                    });
                    modal.on('closeModal', function() {
                        modal.trigger('cancel');
                    });
                });
            },
            onCreateButton: function(ref) {
                var that = this;
                this.name = ref.ui.tagName.val();

                if (ref.ui.parentTag.val().length <= 1 && ref.ui.parentTag.val()[0] == "") {
                    var superTypes = [];
                } else {
                    var superTypes = ref.ui.parentTag.val();
                }
                this.json.traitTypes[0] = {
                    attributeDefinitions: this.collection.toJSON(),
                    typeName: this.name,
                    typeDescription: null,
                    superTypes: superTypes,
                    hierarchicalMetaTypeName: "org.apache.atlas.typesystem.types.TraitType"
                };
                new this.tagCollection.model().set(this.json).save(null, {
                    success: function(model, response) {
                        that.fetchCollections();
                        that.setUrl('#!/tag/tagAttribute/' + ref.ui.tagName.val());
                        Utils.notifySuccess({
                            content: that.name + "  has been created"
                        });
                        that.collection.reset([]);
                    },
                    error: function(model, response) {
                        if (response.responseJSON && response.responseJSON.error) {
                            Utils.notifyError({
                                content: response.responseJSON.error
                            });
                        }
                    }
                });
            },

            setUrl: function(url) {
                Utils.setUrl({
                    url: url,
                    mergeBrowserUrl: false,
                    trigger: true,
                    updateTabState: function() {
                        return { tagUrl: this.url, stateChanged: true };
                    }
                });
            },
            onTagList: function(e, toggle) {
                /*if (toggle) {
                    var assetUl = $(e.currentTarget).siblings('.tagAsset')
                    assetUl.slideToggle("slow");
                }*/
                this.ui.tagsParent.find('li').removeClass("active");
                $(e.currentTarget).addClass("active");
            },
            offlineSearchTag: function(e) {
                var type = $(e.currentTarget).data('type');
                this.tagsAndTypeGenerator('tagCollection', $(e.currentTarget).val());
            },
            createTagAction: function() {
                var that = this;
                this.$('.tagPopover').popover({
                    placement: 'bottom',
                    html: true,
                    trigger: 'manual',
                    container: 'body',
                    content: function() {
                        return "<ul class='tagPopoverList'>" +
                            "<li class='listTerm' ><i class='fa fa-search'></i> <a href='javascript:void(0)' data-fn='onSearchTerm'>Search Tag</a></li>" +
                            "</ul>";
                    }
                });
                this.$('.tagPopover').off('click').on('click', function(e) {
                    // if any other popovers are visible, hide them
                    e.preventDefault();
                    that.$('.tagPopover').not(this).popover('hide');
                    $(this).popover('toggle');
                });
            },
            onSearchTerm: function() {
                Utils.setUrl({
                    url: '#!/search/searchResult',
                    urlParams: {
                        query:  this.ui.tagsParent.find('li.active').find("a").text(),
                        searchType: "fulltext",
                        dslChecked: false
                    },
                    updateTabState: function() {
                        return { searchUrl: this.url, stateChanged: true };
                    },
                    mergeBrowserUrl: false,
                    trigger: true
                });
            }
        });
    return TagLayoutView;
});
