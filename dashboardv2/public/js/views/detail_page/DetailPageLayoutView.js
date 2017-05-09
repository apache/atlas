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
    'hbs!tmpl/detail_page/DetailPageLayoutView_tmpl',
    'utils/Utils',
    'utils/CommonViewFunction',
    'utils/Globals',
    'utils/Enums',
    'utils/Messages',
    'utils/UrlLinks',
    'jquery-ui'
], function(require, Backbone, DetailPageLayoutViewTmpl, Utils, CommonViewFunction, Globals, Enums, Messages, UrlLinks) {
    'use strict';

    var DetailPageLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends DetailPageLayoutView */
        {
            _viewName: 'DetailPageLayoutView',

            template: DetailPageLayoutViewTmpl,

            /** Layout sub regions */
            regions: {
                REntityDetailTableLayoutView: "#r_entityDetailTableLayoutView",
                RSchemaTableLayoutView: "#r_schemaTableLayoutView",
                RTagTableLayoutView: "#r_tagTableLayoutView",
                RLineageLayoutView: "#r_lineageLayoutView",
                RAuditTableLayoutView: "#r_auditTableLayoutView",
                RTermTableLayoutView: "#r_termTableLayoutView"

            },
            /** ui selector cache */
            ui: {
                tagClick: '[data-id="tagClick"]',
                title: '[data-id="title"]',
                editButton: '[data-id="editButton"]',
                description: '[data-id="description"]',
                editBox: '[data-id="editBox"]',
                deleteTag: '[data-id="deleteTag"]',
                backButton: "[data-id='backButton']",
                addTag: '[data-id="addTag"]',
                addTerm: '[data-id="addTerm"]',
                tagList: '[data-id="tagList"]',
                termList: '[data-id="termList"]',
                fullscreenPanel: "#fullscreen_panel"
            },
            templateHelpers: function() {
                return {
                    taxonomy: Globals.taxonomy,
                    entityUpdate: Globals.entityUpdate
                };
            },
            /** ui events hash */
            events: function() {
                var events = {};
                events["click " + this.ui.editButton] = 'onClickEditEntity';
                events["click " + this.ui.tagClick] = function(e) {
                    if (e.target.nodeName.toLocaleLowerCase() != "i") {
                        var scope = $(e.currentTarget);
                        if (scope.hasClass('term')) {
                            var url = scope.data('href').split(".").join("/terms/");
                            Globals.saveApplicationState.tabState.stateChanged = false;
                            Utils.setUrl({
                                url: '#!/taxonomy/detailCatalog' + UrlLinks.taxonomiesApiUrl() + '/' + url,
                                mergeBrowserUrl: false,
                                trigger: true
                            });
                        } else {
                            Utils.setUrl({
                                url: '#!/tag/tagAttribute/' + e.currentTarget.textContent,
                                mergeBrowserUrl: false,
                                trigger: true
                            });
                        }
                    }
                };
                events["click " + this.ui.deleteTag] = 'onClickTagCross';
                events["click " + this.ui.addTag] = 'onClickAddTagBtn';
                events["click " + this.ui.addTerm] = 'onClickAddTermBtn';
                events['click ' + this.ui.backButton] = function() {
                    Backbone.history.history.back();
                };
                return events;
            },
            /**
             * intialize a new DetailPageLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'collection', 'id', 'entityDefCollection', 'typeHeaders', 'enumDefCollection'));
                this.bindEvents();
            },
            bindEvents: function() {
                var that = this;
                this.listenTo(this.collection, 'reset', function() {
                    this.entityObject = this.collection.first().toJSON();
                    var collectionJSON = this.entityObject.entity;
                    if (collectionJSON && collectionJSON.guid) {
                        var tagGuid = collectionJSON.guid;
                        this.readOnly = Enums.entityStateReadOnly[collectionJSON.status];
                    } else {
                        var tagGuid = this.id;
                    }
                    if (this.readOnly) {
                        this.$el.addClass('readOnly');
                    } else {
                        this.$el.removeClass('readOnly');
                    }
                    if (collectionJSON) {
                        this.name = Utils.getName(collectionJSON);
                        if (collectionJSON.attributes) {
                            if (this.name && collectionJSON.typeName) {
                                this.name = this.name + ' (' + _.escape(collectionJSON.typeName) + ')';
                            }
                            if (!this.name && collectionJSON.typeName) {
                                this.name = _.escape(collectionJSON.typeName);
                            }
                            this.description = collectionJSON.attributes.description;
                            if (this.name) {
                                this.ui.title.show();
                                var titleName = '<span>' + this.name + '</span>';
                                if (this.readOnly) {
                                    titleName += '<button title="Deleted" class="btn btn-atlasAction btn-atlas deleteBtn"><i class="fa fa-trash"></i> Deleted</button>';
                                }
                                this.ui.title.html(titleName);
                            } else {
                                this.ui.title.hide();
                            }
                            if (this.description) {
                                this.ui.description.show();
                                this.ui.description.html('<span>' + _.escape(this.description) + '</span>');
                            } else {
                                this.ui.description.hide();
                            }
                        }
                        if (collectionJSON.classifications) {
                            this.addTagToTerms(collectionJSON.classifications);
                        } else {
                            this.addTagToTerms([]);
                        }
                        if (Globals.entityTypeConfList && _.isEmptyArray(Globals.entityTypeConfList)) {
                            this.ui.editButton.show();
                        } else {
                            if (_.contains(Globals.entityTypeConfList, collectionJSON.typeName)) {
                                this.ui.editButton.show();
                            }
                        }
                    }
                    this.hideLoader();
                    var obj = {
                        entity: collectionJSON,
                        referredEntities: this.entityObject.referredEntities,
                        guid: this.id,
                        entityName: this.name,
                        typeHeaders: this.typeHeaders,
                        entityDefCollection: this.entityDefCollection,
                        fetchCollection: this.fetchCollection.bind(that),
                        enumDefCollection: this.enumDefCollection
                    }
                    this.getEntityDef(obj);
                    this.renderTagTableLayoutView(obj);
                    this.renderTermTableLayoutView(_.extend({}, obj, { term: true }));
                    // To render Schema check attribute "schemaElementsAttribute"
                    var schemaOptions = this.entityDefCollection.fullCollection.find({ name: collectionJSON.typeName }).get('options');
                    if (schemaOptions && schemaOptions.hasOwnProperty('schemaElementsAttribute') && schemaOptions.schemaElementsAttribute !== "") {
                        this.$('.schemaTable').show();
                        this.renderSchemaLayoutView(_.extend({}, obj, {
                            attribute: collectionJSON.attributes[schemaOptions.schemaElementsAttribute]
                        }));
                    }
                }, this);
                this.listenTo(this.collection, 'error', function(model, response) {
                    this.$('.fontLoader').hide();
                    if (response.responseJSON) {
                        Utils.notifyError({
                            content: response.responseJSON.errorMessage || response.responseJSON.error
                        });
                    }
                }, this);
            },
            onRender: function() {
                var that = this;
                Utils.showTitleLoader(this.$('.page-title .fontLoader'), this.$('.entityDetail'));
                this.$('.fontLoader').show(); // to show tab loader
                this.renderLineageLayoutView({
                    guid: this.id,
                    entityDefCollection: this.entityDefCollection,
                    actionCallBack: function() {
                        that.$('#expand_collapse_panel').click();
                    }
                });
                this.$(".resize-graph").resizable({
                    handles: ' s',
                    minHeight: 375,
                    stop: function(event, ui) {
                        that.$('.resize-graph').height(($(this).height()));
                    },
                });
                this.ui.fullscreenPanel.on('fullscreen_done', function(e, panel) {
                    var svgEl = panel.find('.panel-body svg'),
                        scaleEl = svgEl.find('>g'),
                        zoom = that.RLineageLayoutView.currentView.zoom,
                        svg = that.RLineageLayoutView.currentView.svg,
                        viewThis = that.RLineageLayoutView.currentView,
                        setGraphZoomPositionCal = that.RLineageLayoutView.currentView.setGraphZoomPositionCal,
                        zoomed = that.RLineageLayoutView.currentView.zoomed;;

                    if (zoom) {
                        setGraphZoomPositionCal.call(viewThis);
                        zoomed.call(viewThis);
                        if ($(e.currentTarget).find('i').hasClass('fa fa-compress')) {
                            svg.call(zoom)
                                .on("dblclick.zoom", null);

                        } else {
                            svg.call(zoom)
                                .on("wheel.zoom", null)
                                .on("dblclick.zoom", null);
                        }
                    }
                })
            },
            fetchCollection: function() {
                this.collection.fetch({ reset: true });
            },
            getEntityDef: function(obj) {
                var data = this.entityDefCollection.fullCollection.findWhere({ name: obj.entity.typeName }).toJSON();
                var entityDef = Utils.getNestedSuperTypeObj({
                    data: data,
                    attrMerge: true,
                    collection: this.entityDefCollection
                });
                obj['entityDef'] = entityDef;
                this.renderEntityDetailTableLayoutView(obj);
                this.renderAuditTableLayoutView(obj);
            },
            onClickTagCross: function(e) {
                var tagName = $(e.currentTarget).parent().text(),
                    tagOrTerm = $(e.target).data("type"),
                    that = this;
                if (tagOrTerm === "term") {
                    var modal = CommonViewFunction.deleteTagModel({
                        msg: "<div class='ellipsis'>Remove: " + "<b>" + _.escape(tagName) + "</b> assignment from" + " " + "<b>" + this.name + "?</b></div>",
                        titleMessage: Messages.removeTerm,
                        buttonText: "Remove"
                    });
                } else if (tagOrTerm === "tag") {
                    var modal = CommonViewFunction.deleteTagModel({
                        msg: "<div class='ellipsis'>Remove: " + "<b>" + _.escape(tagName) + "</b> assignment from" + " " + "<b>" + this.name + "?</b></div>",
                        titleMessage: Messages.removeTag,
                        buttonText: "Remove"
                    });
                }
                if (modal) {
                    modal.on('ok', function() {
                        that.deleteTagData(e, tagOrTerm);
                    });
                    modal.on('closeModal', function() {
                        modal.trigger('cancel');
                    });
                }
            },
            deleteTagData: function(e, tagOrTerm) {
                var that = this,
                    tagName = $(e.currentTarget).text();
                Utils.showTitleLoader(this.$('.page-title .fontLoader'), this.$('.entityDetail'));
                CommonViewFunction.deleteTag({
                    'tagName': tagName,
                    'guid': that.id,
                    'tagOrTerm': tagOrTerm,
                    callback: function() {
                        that.fetchCollection();
                    }
                });
            },
            addTagToTerms: function(tagObject) {
                var that = this,
                    tagData = "",
                    termData = "";

                _.each(tagObject, function(val) {
                    var checkTagOrTerm = Utils.checkTagOrTerm(val);
                    if (checkTagOrTerm.term) {
                        termData += '<span class="inputTag term" data-id="tagClick" data-href="' + val.typeName + '"><span class="inputValue">' + val.typeName + '</span><i class="fa fa-close" data-id="deleteTag" data-type="term"></i></span>';
                    } else {
                        tagData += '<span class="inputTag" data-id="tagClick"><span class="inputValue">' + val.typeName + '</span><i class="fa fa-close" data-id="deleteTag" data-type="tag"></i></span>';
                    }
                });
                this.ui.tagList.find("span.inputTag").remove();
                this.ui.termList.find("span.inputTag").remove();
                this.ui.tagList.prepend(tagData);
                this.ui.termList.prepend(termData);
            },
            hideLoader: function() {
                Utils.hideTitleLoader(this.$('.page-title .fontLoader'), this.$('.entityDetail'));
            },
            showLoader: function() {
                Utils.showTitleLoader(this.$('.page-title .fontLoader'), this.$('.entityDetail'));
            },
            onClickAddTagBtn: function(e) {
                var that = this;
                require(['views/tag/addTagModalView'], function(AddTagModalView) {
                    var view = new AddTagModalView({
                        guid: that.id,
                        tagList: _.map(that.entityObject.entity.classifications, function(obj) {
                            return obj.typeName;
                        }),
                        callback: function() {
                            that.fetchCollection();
                        },
                        showLoader: that.showLoader.bind(that),
                        hideLoader: that.hideLoader.bind(that),
                        enumDefCollection: that.enumDefCollection
                    });
                    view.modal.on('ok', function() {
                        Utils.showTitleLoader(that.$('.page-title .fontLoader'), that.$('.entityDetail'));
                    });
                });
            },
            onClickAddTermBtn: function(e) {
                var that = this;
                require([
                    'views/business_catalog/AddTermToEntityLayoutView',
                ], function(AddTermToEntityLayoutView) {
                    var view = new AddTermToEntityLayoutView({
                        guid: that.id,
                        callback: function() {
                            that.fetchCollection();
                        },
                        showLoader: that.showLoader.bind(that),
                        hideLoader: that.hideLoader.bind(that)
                    });
                    view.modal.on('ok', function() {
                        Utils.showTitleLoader(that.$('.page-title .fontLoader'), that.$('.entityDetail'));
                    });
                });

            },
            renderEntityDetailTableLayoutView: function(obj) {
                var that = this;
                require(['views/entity/EntityDetailTableLayoutView'], function(EntityDetailTableLayoutView) {
                    that.REntityDetailTableLayoutView.show(new EntityDetailTableLayoutView(obj));
                });
            },
            renderTagTableLayoutView: function(obj) {
                var that = this;
                require(['views/tag/TagDetailTableLayoutView'], function(TagDetailTableLayoutView) {
                    that.RTagTableLayoutView.show(new TagDetailTableLayoutView(obj));
                });
            },
            renderTermTableLayoutView: function(obj) {
                var that = this;
                require(['views/tag/TagDetailTableLayoutView'], function(TagDetailTableLayoutView) {
                    that.RTermTableLayoutView.show(new TagDetailTableLayoutView(obj));
                });
            },
            renderLineageLayoutView: function(obj) {
                var that = this;
                require(['views/graph/LineageLayoutView'], function(LineageLayoutView) {
                    that.RLineageLayoutView.show(new LineageLayoutView(obj));
                });
            },
            renderSchemaLayoutView: function(obj) {
                var that = this;
                require(['views/schema/SchemaLayoutView'], function(SchemaLayoutView) {
                    that.RSchemaTableLayoutView.show(new SchemaLayoutView(obj));
                });
            },
            renderAuditTableLayoutView: function(obj) {
                var that = this;
                require(['views/audit/AuditTableLayoutView'], function(AuditTableLayoutView) {
                    that.RAuditTableLayoutView.show(new AuditTableLayoutView(obj));
                });
            },
            onClickEditEntity: function(e) {
                var that = this;
                $(e.currentTarget).blur();
                require([
                    'views/entity/CreateEntityLayoutView'
                ], function(CreateEntityLayoutView) {
                    var view = new CreateEntityLayoutView({
                        guid: that.id,
                        entityDefCollection: that.entityDefCollection,
                        typeHeaders: that.typeHeaders,
                        callback: function() {
                            that.fetchCollection();
                        }
                    });

                });
            }
        });
    return DetailPageLayoutView;
});
