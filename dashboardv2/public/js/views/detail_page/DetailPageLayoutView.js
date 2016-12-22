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
    'collection/VTagList',
    'models/VEntity',
    'utils/CommonViewFunction',
    'utils/Globals',
    'utils/Enums',
    'utils/Messages',
    'utils/UrlLinks'
], function(require, Backbone, DetailPageLayoutViewTmpl, Utils, VTagList, VEntity, CommonViewFunction, Globals, Enums, Messages, UrlLinks) {
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
                cancelButton: '[data-id="cancelButton"]',
                publishButton: '[data-id="publishButton"]',
                description: '[data-id="description"]',
                descriptionTextArea: '[data-id="descriptionTextArea"]',
                editBox: '[data-id="editBox"]',
                createDate: '[data-id="createDate"]',
                updateDate: '[data-id="updateDate"]',
                createdUser: '[data-id="createdUser"]',
                deleteTag: '[data-id="deleteTag"]',
                backButton: "[data-id='backButton']",
                addTag: '[data-id="addTag"]',
                addTerm: '[data-id="addTerm"]',
                tagList: '[data-id="tagList"]',
                termList: '[data-id="termList"]'
            },
            templateHelpers: function() {
                return {
                    taxonomy: Globals.taxonomy
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
                // events["click " + this.ui.publishButton] = 'onPublishButtonClick';
                events["click " + this.ui.cancelButton] = 'onCancelButtonClick';
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
                _.extend(this, _.pick(options, 'globalVent', 'collection', 'vent', 'id'));
                this.bindEvents();
                this.auditVent = new Backbone.Wreqr.EventAggregator();
            },
            bindEvents: function() {
                var that = this;
                this.listenTo(this.collection, 'reset', function() {

                    var collectionJSON = this.collection.first().toJSON();
                    if (collectionJSON && collectionJSON.guid) {
                        var tagGuid = collectionJSON.guid;
                        //this.readOnly = Enums.entityStateReadOnly[collectionJSON[0].id.state];
                    } else {
                        var tagGuid = this.id;
                    }
                    // if (this.readOnly) {
                    //     this.$el.addClass('readOnly');
                    // } else {
                    //     this.$el.removeClass('readOnly');
                    // }
                    if (collectionJSON) {
                        if (collectionJSON.attributes) {
                            if (collectionJSON.attributes.name) {
                                this.name = collectionJSON.attributes.name
                            }
                            if (!this.name && collectionJSON.attributes.qualifiedName) {
                                this.name = collectionJSON.attributes.qualifiedName;
                            }
                            if (this.name && collectionJSON.typeName) {
                                this.name = this.name + ' (' + collectionJSON.typeName + ')';
                            }
                            if (!this.name && collectionJSON.typeName) {
                                this.name = collectionJSON.typeName;
                            }

                            if (!this.name && this.id) {
                                this.name = this.id;
                            }
                            this.description = collectionJSON.attributes.description;
                            if (this.name) {
                                this.ui.title.show();
                                var titleName = '<span>' + _.escape(this.name) + '</span>';
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
                    }
                    this.auditVent.trigger("reset:collection");
                    this.renderEntityDetailTableLayoutView();
                    this.renderTagTableLayoutView(tagGuid);
                    this.renderTermTableLayoutView(tagGuid);
                }, this);
            },
            onRender: function() {
                var that = this;
                this.ui.editBox.hide();
                this.renderLineageLayoutView(this.id);
                this.renderSchemaLayoutView(this.id);
                this.renderAuditTableLayoutView(this.id);
            },
            fetchCollection: function() {
                this.collection.fetch({ reset: true });
            },
            onCancelButtonClick: function() {
                this.ui.description.show();
                this.ui.editButton.show();
                this.ui.editBox.hide();
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
                    var isTerm = Utils.checkTagOrTerm(val);
                    if (isTerm.tag) {
                        tagData += '<span class="inputTag" data-id="tagClick"><span class="inputValue">' + isTerm.fullName + '</span><i class="fa fa-close" data-id="deleteTag" data-type="tag"></i></span>';
                    }
                    if (isTerm.term) {
                        termData += '<span class="inputTag term" data-id="tagClick" data-href="' + isTerm.fullName + '"><span class="inputValue">' + isTerm.fullName + '</span><i class="fa fa-close" data-id="deleteTag" data-type="term"></i></span>';
                    }
                });
                this.ui.tagList.find("span.inputTag").remove();
                this.ui.termList.find("span.inputTag").remove();
                this.ui.tagList.prepend(tagData);
                this.ui.termList.prepend(termData);
            },
            saveTagFromList: function(ref) {
                var that = this;
                this.entityModel = new VEntity();
                var tagName = ref.text();
                var json = {
                    "jsonClass": "org.apache.atlas.typesystem.json.InstanceSerialization$_Struct",
                    "typeName": tagName,
                    "values": {}
                };
                this.entityModel.saveEntity(this.id, {
                    data: JSON.stringify(json),
                    success: function(data) {
                        that.fetchCollection();
                    },
                    error: function(error, data, status) {
                        if (error && error.responseText) {
                            var data = JSON.parse(error.responseText);
                        }
                    },
                    complete: function() {}
                });
            },
            onClickAddTagBtn: function(e) {
                var that = this;
                require(['views/tag/addTagModalView'], function(AddTagModalView) {
                    var view = new AddTagModalView({
                        vent: that.vent,
                        guid: that.id,
                        callback: function() {
                            that.fetchCollection();
                        }
                    });
                    /*view.saveTagData = function() {
                    override saveTagData function 
                    }*/
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
                        }
                    });
                });

            },
            renderEntityDetailTableLayoutView: function() {
                var that = this;
                require(['views/entity/EntityDetailTableLayoutView'], function(EntityDetailTableLayoutView) {
                    that.REntityDetailTableLayoutView.show(new EntityDetailTableLayoutView({
                        globalVent: that.globalVent,
                        collection: that.collection
                    }));
                });
            },
            renderTagTableLayoutView: function(tagGuid) {
                var that = this;
                require(['views/tag/TagDetailTableLayoutView'], function(TagDetailTableLayoutView) {
                    that.RTagTableLayoutView.show(new TagDetailTableLayoutView({
                        globalVent: that.globalVent,
                        collection: that.collection,
                        guid: tagGuid,
                        assetName: that.name
                    }));
                });
            },
            renderLineageLayoutView: function(tagGuid) {
                var that = this;
                require(['views/graph/LineageLayoutView'], function(LineageLayoutView) {
                    that.RLineageLayoutView.show(new LineageLayoutView({
                        globalVent: that.globalVent,
                        guid: tagGuid
                    }));
                });
            },
            renderSchemaLayoutView: function(tagGuid) {
                var that = this;
                require(['views/schema/SchemaLayoutView'], function(SchemaLayoutView) {
                    that.RSchemaTableLayoutView.show(new SchemaLayoutView({
                        globalVent: that.globalVent,
                        guid: tagGuid
                    }));
                });
            },
            renderAuditTableLayoutView: function(tagGuid) {
                var that = this;
                require(['views/audit/AuditTableLayoutView'], function(AuditTableLayoutView) {
                    that.RAuditTableLayoutView.show(new AuditTableLayoutView({
                        globalVent: that.globalVent,
                        guid: tagGuid,
                        vent: that.auditVent
                    }));
                });
            },
            renderTermTableLayoutView: function(tagGuid) {
                var that = this;
                require(['views/tag/TagDetailTableLayoutView'], function(TagDetailTableLayoutView) {
                    that.RTermTableLayoutView.show(new TagDetailTableLayoutView({
                        globalVent: that.globalVent,
                        collection: that.collection,
                        guid: tagGuid,
                        assetName: that.name,
                        term: true
                    }));
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
                        callback: function() {
                            that.fetchCollection();
                        }
                    });

                });
            }
        });
    return DetailPageLayoutView;
});
