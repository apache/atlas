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
    'utils/Messages',
    'utils/Globals'
], function(require, Backbone, TagLayoutViewTmpl, VTagList, VEntityList, Utils, Messages, Globals) {
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
                refreshTag: '[data-id="refreshTag"]'

            },
            /** ui events hash */
            events: function() {
                var events = {};
                events["click " + this.ui.createTag] = 'onClickCreateTag';
                events["click " + this.ui.tags] = 'onTagList';
                events["keyup " + this.ui.offLineSearchTag] = 'offlineSearchTag';
                events["click " + this.ui.deleteTerm] = 'onDeleteTerm';
                events['click ' + this.ui.refreshTag] = 'fetchCollections';
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
                $.extend(this.tagCollection.queryParams, { type: 'TRAIT', notsupertype: 'TaxonomyTerm' });
                this.tagCollection.fetch({ reset: true });
                this.ui.offLineSearchTag.val("");
            },
            manualRender: function(tagName) {
                this.tag = tagName;
                if (!this.createTag) {
                    this.setValues(true);
                }
            },
            setValues: function(manual) {
                if (Utils.getUrlState.isTagTab() || (Utils.getUrlState.isInitial() && !Globals.taxonomy)) {
                    if (!this.tag) {
                        this.selectFirst = false;
                        this.ui.tagsParent.find('li').first().addClass('active');
                        if (this.ui.tagsParent.find('li a').first().length) {
                            Utils.setUrl({
                                url: this.ui.tagsParent.find('li a').first().attr("href"),
                                mergeBrowserUrl: false,
                                trigger: true,
                                updateTabState: function() {
                                    return { tagUrl: this.url, stateChanged: true };
                                }
                            });
                        }
                    } else {
                        Utils.setUrl({
                            url: Utils.getUrlState.getQueryUrl().hash,
                            updateTabState: function() {
                                return { tagUrl: this.url, stateChanged: true };
                            }
                        });
                        var tag = Utils.getUrlState.getLastValue();
                        if (this.tag) {
                            tag = this.tag;
                        }
                        this.ui.tagsParent.find('li').removeClass('active');
                        var target = this.ui.tagsParent.find('li').filter(function() {
                            return $(this).text() === tag;
                        }).addClass('active');
                        if (this.createTag || !manual) {
                            if (target && target.offset()) {
                                $('#sidebar-wrapper').animate({
                                    scrollTop: target.offset().top - 100
                                }, 500);
                            }
                        }

                    }
                }
            },
            tagsAndTypeGenerator: function(collection, searchString) {
                var that = this,
                    str = '';
                that.tagCollection.fullCollection.comparator = function(model) {
                    return model.get('tags').toLowerCase();
                };
                that.tagCollection.fullCollection.sort().each(function(model) {
                    if (searchString) {
                        if (model.get('tags').search(new RegExp(searchString, "i")) != -1) {
                            // data-name="<space>'<tagName>'"  Space is required for DSL search Input 
                            str += '<li class="parent-node" data-id="tags"><div class="tools"><i class="fa fa-ellipsis-h tagPopover"></i></div><a href="#!/tag/tagAttribute/' + model.get('tags') + '"  data-name=" `' + model.get('tags') + '`" >' + model.get('tags') + '</a></li>';
                        } else {
                            return;
                        }
                    } else {
                        str += '<li class="parent-node" data-id="tags"><div class="tools"><i class="fa fa-ellipsis-h tagPopover"></i></div><a href="#!/tag/tagAttribute/' + model.get('tags') + '"  data-name=" `' + model.get('tags') + '`">' + model.get('tags') + '</a></li>';
                    }
                });
                this.ui.tagsParent.empty().html(str);
                this.setValues();
                this.createTagAction();
                if (this.createTag) {
                    this.createTag = false;
                }

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
                    modal.on('shownModal', function() {
                        view.ui.parentTag.select2({
                            multiple: true,
                            placeholder: "Search Tags",
                            allowClear: true
                        });
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
                this.description = ref.ui.description.val();
                var superTypes = [];
                if (ref.ui.parentTag.val() && ref.ui.parentTag.val()) {
                    superTypes = ref.ui.parentTag.val();
                }
                this.json.traitTypes[0] = {
                    attributeDefinitions: this.collection.toJSON(),
                    typeName: this.name,
                    typeDescription: this.description,
                    superTypes: superTypes,
                    hierarchicalMetaTypeName: "org.apache.atlas.typesystem.types.TraitType"
                };
                new this.tagCollection.model().set(this.json).save(null, {
                    success: function(model, response) {
                        that.createTag = true;
                        that.fetchCollections();
                        that.setUrl('#!/tag/tagAttribute/' + ref.ui.tagName.val(), true);
                        Utils.notifySuccess({
                            content: "Tag " + that.name + Messages.addSuccessMessage
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

            setUrl: function(url, create) {
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
                        query: this.ui.tagsParent.find('li.active').find("a").data('name'),
                        searchType: "dsl",
                        dslChecked: true
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
