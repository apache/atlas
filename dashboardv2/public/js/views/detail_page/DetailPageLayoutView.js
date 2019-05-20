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
    'hbs!tmpl/common/buttons_tmpl',
    'utils/Utils',
    'utils/CommonViewFunction',
    'utils/Globals',
    'utils/Enums',
    'utils/Messages',
    'utils/UrlLinks'
], function(require, Backbone, DetailPageLayoutViewTmpl, ButtonsTmpl, Utils, CommonViewFunction, Globals, Enums, Messages, UrlLinks) {
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
                RReplicationAuditTableLayoutView: "#r_replicationAuditTableLayoutView",
                RProfileLayoutView: "#r_profileLayoutView",
                RRelationshipLayoutView: "#r_relationshipLayoutView"
            },
            /** ui selector cache */
            ui: {
                tagClick: '[data-id="tagClick"]',
                termClick: '[data-id="termClick"]',
                propagatedTagDiv: '[data-id="propagatedTagDiv"]',
                title: '[data-id="title"]',
                editButton: '[data-id="editButton"]',
                editButtonContainer: '[data-id="editButtonContainer"]',
                description: '[data-id="description"]',
                editBox: '[data-id="editBox"]',
                deleteTag: '[data-id="deleteTag"]',
                deleteTerm: '[data-id="deleteTerm"]',
                addTag: '[data-id="addTag"]',
                addTerm: '[data-id="addTerm"]',
                tagList: '[data-id="tagList"]',
                termList: '[data-id="termList"]',
                propagatedTagList: '[data-id="propagatedTagList"]',
                tablist: '[data-id="tab-list"] li',
                entityIcon: '[data-id="entityIcon"]'
            },
            templateHelpers: function() {
                return {
                    entityUpdate: Globals.entityUpdate
                };
            },
            /** ui events hash */
            events: function() {
                var events = {};
                events["click " + this.ui.editButton] = 'onClickEditEntity';
                events["click " + this.ui.tagClick] = function(e) {
                    if (e.target.nodeName.toLocaleLowerCase() != "i") {
                        Utils.setUrl({
                            url: '#!/tag/tagAttribute/' + e.currentTarget.textContent,
                            mergeBrowserUrl: false,
                            trigger: true
                        });
                    }
                };
                events["click " + this.ui.termClick] = function(e) {
                    if (e.target.nodeName.toLocaleLowerCase() != "i") {
                        Utils.setUrl({
                            url: '#!/glossary/' + $(e.currentTarget).find('i').data('guid'),
                            mergeBrowserUrl: false,
                            urlParams: { gType: "term", viewType: "term", fromView: "entity" },
                            trigger: true
                        });
                    }
                };
                events["click " + this.ui.addTerm] = 'onClickAddTermBtn';
                events["click " + this.ui.deleteTag] = 'onClickTagCross';
                events["click " + this.ui.deleteTerm] = 'onClickTermCross';
                events["click " + this.ui.addTag] = 'onClickAddTagBtn';
                events["click " + this.ui.tablist] = function(e) {
                    var tabValue = $(e.currentTarget).attr('role');
                    Utils.setUrl({
                        url: Utils.getUrlState.getQueryUrl().queyParams[0],
                        urlParams: { tabActive: tabValue || 'properties' },
                        mergeBrowserUrl: false,
                        trigger: false,
                        updateTabState: true
                    });

                };
                return events;
            },
            /**
             * intialize a new DetailPageLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'value', 'collection', 'id', 'entityDefCollection', 'typeHeaders', 'enumDefCollection', 'classificationDefCollection', 'glossaryCollection'));
                $('body').addClass("detail-page");
            },
            bindEvents: function() {
                var that = this;
                this.listenTo(this.collection, 'reset', function() {
                    this.entityObject = this.collection.first().toJSON();
                    var collectionJSON = this.entityObject.entity;
                    this.activeEntityDef = this.entityDefCollection.fullCollection.find({ name: collectionJSON.typeName });

                    if (collectionJSON && _.startsWith(collectionJSON.typeName, "AtlasGlossary")) {
                        this.$(".termBox").hide();
                    }
                    // MergerRefEntity.
                    Utils.findAndMergeRefEntity({
                        attributeObject: collectionJSON.attributes,
                        referredEntities: this.entityObject.referredEntities
                    });

                    Utils.findAndMergeRefEntity({
                        attributeObject: collectionJSON.relationshipAttributes,
                        referredEntities: this.entityObject.referredEntities
                    });

                    Utils.findAndMergeRelationShipEntity({
                        attributeObject: collectionJSON.attributes,
                        relationshipAttributes: collectionJSON.relationshipAttributes
                    });

                    // check if entity is process
                    var isProcess = false,
                        superTypes = Utils.getNestedSuperTypes({ data: this.activeEntityDef.toJSON(), collection: this.entityDefCollection }),
                        isLineageRender = _.find(superTypes, function(type) {
                            if (type === "DataSet" || type === "Process") {
                                if (type === "Process") {
                                    isProcess = true;
                                }
                                return true;
                            }
                        });

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
                                    titleName += '<button title="Deleted" class="btn btn-action btn-md deleteBtn"><i class="fa fa-trash"></i> Deleted</button>';
                                }
                                this.ui.title.html(titleName);
                                var entityData = _.extend({ "serviceType": this.activeEntityDef && this.activeEntityDef.get('serviceType'), "isProcess": isProcess }, collectionJSON);
                                if (this.readOnly) {
                                    this.ui.entityIcon.addClass('disabled');
                                } else {
                                    this.ui.entityIcon.removeClass('disabled');
                                }
                                this.ui.entityIcon.attr('title', _.escape(collectionJSON.typeName)).html('<img src="' + Utils.getEntityIconPath({ entityData: entityData }) + '"/>').find("img").on('error', function() {
                                    this.src = Utils.getEntityIconPath({ entityData: entityData, errorUrl: this.src });
                                });
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
                            this.generateTag(collectionJSON.classifications);
                        } else {
                            this.generateTag([]);
                        }
                        if (collectionJSON.relationshipAttributes && collectionJSON.relationshipAttributes.meanings) {
                            this.generateTerm(collectionJSON.relationshipAttributes.meanings);
                        }
                        if (Globals.entityTypeConfList && _.isEmptyArray(Globals.entityTypeConfList)) {
                            this.ui.editButtonContainer.html(ButtonsTmpl({ btn_edit: true }));
                        } else {
                            if (_.contains(Globals.entityTypeConfList, collectionJSON.typeName)) {
                                this.ui.editButtonContainer.html(ButtonsTmpl({ btn_edit: true }));
                            }
                        }
                        if (collectionJSON.attributes && collectionJSON.attributes.columns) {
                            var valueSorted = _.sortBy(collectionJSON.attributes.columns, function(val) {
                                return val.attributes && val.attributes.position
                            });
                            collectionJSON.attributes.columns = valueSorted;
                        }
                    }
                    this.hideLoader();
                    var obj = {
                        entity: collectionJSON,
                        guid: this.id,
                        entityName: this.name,
                        typeHeaders: this.typeHeaders,
                        entityDefCollection: this.entityDefCollection,
                        fetchCollection: this.fetchCollection.bind(that),
                        enumDefCollection: this.enumDefCollection,
                        classificationDefCollection: this.classificationDefCollection,
                        glossaryCollection: this.glossaryCollection,
                        attributeDefs: (function() {
                            return that.getEntityDef(collectionJSON);
                        })()
                    }
                    this.renderEntityDetailTableLayoutView(obj);
                    this.renderRelationshipLayoutView(obj);
                    this.renderAuditTableLayoutView(obj);
                    this.renderTagTableLayoutView(obj);

                    // To render profile tab check for attribute "profileData" or typeName = "hive_db","hbase_namespace"
                    if (collectionJSON && (!_.isUndefined(collectionJSON.attributes['profileData']) || collectionJSON.typeName === "hive_db" || collectionJSON.typeName === "hbase_namespace")) {
                        if (collectionJSON.typeName === "hive_db" || collectionJSON.typeName === "hbase_namespace") {
                            this.$('.profileTab a').text("Tables")
                        }
                        this.$('.profileTab').show();
                        this.renderProfileLayoutView(_.extend({}, obj, {
                            entityDetail: collectionJSON.attributes,
                            profileData: collectionJSON.attributes.profileData,
                            typeName: collectionJSON.typeName,
                            value: that.value
                        }));
                    }

                    if (this.activeEntityDef) {
                        //to display ReplicationAudit tab
                        if (collectionJSON && collectionJSON.typeName === "AtlasServer") {
                            this.$('.replicationTab').show();
                            this.renderReplicationAuditTableLayoutView(obj);
                        }
                        // To render Schema check attribute "schemaElementsAttribute"
                        var schemaOptions = this.activeEntityDef.get('options');
                        var schemaElementsAttribute = schemaOptions && schemaOptions.schemaElementsAttribute;
                        if (!_.isEmpty(schemaElementsAttribute)) {
                            this.$('.schemaTable').show();
                            this.renderSchemaLayoutView(_.extend({}, obj, {
                                attribute: collectionJSON.relationshipAttributes[schemaElementsAttribute] || collectionJSON.attributes[schemaElementsAttribute]
                            }));
                        } else if (this.value && this.value.tabActive == "schema") {
                            Utils.setUrl({
                                url: Utils.getUrlState.getQueryUrl().queyParams[0],
                                urlParams: { tabActive: 'properties' },
                                mergeBrowserUrl: false,
                                trigger: true,
                                updateTabState: true
                            });
                        }

                        if (isLineageRender) {
                            this.$('.lineageGraph').show();
                            this.renderLineageLayoutView(_.extend(obj, {
                                processCheck: isProcess,
                                fetchCollection: this.fetchCollection.bind(this),
                            }));
                        } else if (this.value && this.value.tabActive == "lineage") {
                            Utils.setUrl({
                                url: Utils.getUrlState.getQueryUrl().queyParams[0],
                                urlParams: { tabActive: 'properties' },
                                mergeBrowserUrl: false,
                                trigger: true,
                                updateTabState: true
                            });
                        }
                    }


                }, this);
                this.listenTo(this.collection, 'error', function(model, response) {
                    this.$('.fontLoader-relative').removeClass('show');
                    if (response.responseJSON) {
                        Utils.notifyError({
                            content: response.responseJSON.errorMessage || response.responseJSON.error
                        });
                    }
                }, this);
            },
            onRender: function() {
                var that = this;
                this.bindEvents();
                Utils.showTitleLoader(this.$('.page-title .fontLoader'), this.$('.entityDetail'));
                this.$('.fontLoader-relative').addClass('show'); // to show tab loader
            },
            onShow: function() {
                if (this.value && this.value.tabActive) {
                    this.$('.nav.nav-tabs').find('[role="' + this.value.tabActive + '"]').addClass('active').siblings().removeClass('active');
                    this.$('.tab-content').find('[role="' + this.value.tabActive + '"]').addClass('active').siblings().removeClass('active');
                    $("html, body").animate({ scrollTop: (this.$('.tab-content').offset().top + 1200) }, 1000);
                }
            },
            onDestroy: function() {
                if (!Utils.getUrlState.isDetailPage()) {
                    $('body').removeClass("detail-page");
                }
            },
            fetchCollection: function() {
                this.collection.fetch({ reset: true });
            },
            getEntityDef: function(entityObj) {
                if (this.activeEntityDef) {
                    var data = this.activeEntityDef.toJSON();
                    var attributeDefs = Utils.getNestedSuperTypeObj({
                        data: data,
                        attrMerge: true,
                        collection: this.entityDefCollection
                    });
                    return attributeDefs;
                } else {
                    return [];
                }
            },
            onClickTagCross: function(e) {
                var that = this,
                    tagName = $(e.currentTarget).parent().text(),
                    entityGuid = $(e.currentTarget).data("entityguid");
                CommonViewFunction.deleteTag(_.extend({}, {
                    guid: that.id,
                    associatedGuid: that.id != entityGuid ? entityGuid : null,
                    msg: "<div class='ellipsis'>Remove: " + "<b>" + _.escape(tagName) + "</b> assignment from" + " " + "<b>" + this.name + "?</b></div>",
                    titleMessage: Messages.removeTag,
                    okText: "Remove",
                    showLoader: that.showLoader.bind(that),
                    hideLoader: that.hideLoader.bind(that),
                    tagName: tagName,
                    callback: function() {
                        that.fetchCollection();
                    }
                }));
            },
            onClickTermCross: function(e) {
                var $el = $(e.currentTarget),
                    termGuid = $el.data('guid'),
                    termName = $el.text(),
                    that = this,
                    termObj = _.find(this.collection.first().get('entity').relationshipAttributes.meanings, { guid: termGuid });
                CommonViewFunction.removeCategoryTermAssociation({
                    termGuid: termGuid,
                    model: {
                        guid: that.id,
                        relationshipGuid: termObj.relationshipGuid
                    },
                    collection: that.glossaryCollection,
                    msg: "<div class='ellipsis'>Remove: " + "<b>" + _.escape(termName) + "</b> assignment from" + " " + "<b>" + this.name + "?</b></div>",
                    titleMessage: Messages.glossary.removeTermfromEntity,
                    isEntityView: true,
                    buttonText: "Remove",
                    showLoader: that.showLoader.bind(that),
                    hideLoader: that.hideLoader.bind(that),
                    callback: function() {
                        that.fetchCollection();
                    }
                });
            },
            generateTag: function(tagObject) {
                var that = this,
                    tagData = "",
                    propagatedTagListData = "",
                    tag = {
                        'self': [],
                        'propagated': []
                    };
                _.each(tagObject, function(val) {
                    val.entityGuid === that.id ? tag['self'].push(val) : tag['propagated'].push(val);
                });
                _.each(tag.self, function(val) {
                    tagData += '<span class="btn btn-action btn-sm btn-icon btn-blue" data-id="tagClick"><span title=' + val.typeName + ' >' + val.typeName + '</span><i class="fa fa-close" data-id="deleteTag" data-type="tag" title="Remove Tag"></i></span>';
                });
                _.each(tag.propagated, function(val) {
                    var crossButton = '<i class="fa fa-close" data-id="deleteTag" data-entityguid="' + val.entityGuid + '" data-type="tag" title="Remove Tag"></i>';
                    propagatedTagListData += '<span class="btn btn-action btn-sm btn-icon btn-blue" title=' + val.typeName + ' data-id="tagClick"><span>' + val.typeName + '</span>' + ((that.id !== val.entityGuid && val.entityStatus === "DELETED") ? crossButton : "") + '</span>';
                });
                propagatedTagListData !== "" ? this.ui.propagatedTagDiv.show() : this.ui.propagatedTagDiv.hide();
                this.ui.tagList.find("span.btn").remove();
                this.ui.propagatedTagList.find("span.btn").remove();
                this.ui.tagList.prepend(tagData);
                this.ui.propagatedTagList.html(propagatedTagListData);

            },
            generateTerm: function(data) {
                var that = this,
                    termData = "";
                _.each(data, function(val) {
                    console.log(val.guid)
                    if (val.relationshipStatus == "ACTIVE") {
                        termData += '<span class="btn btn-action btn-sm btn-icon btn-blue" title=' + _.escape(val.displayText) + ' data-id="termClick"><span>' + _.escape(val.displayText) + '</span><i class="fa fa-close" data-id="deleteTerm" data-guid="' + val.guid + '" data-type="term" title="Remove Term"></i></span>';
                    }
                });
                this.ui.termList.find("span.btn").remove();
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
                require(['views/tag/AddTagModalView'], function(AddTagModalView) {
                    var tagList = [];
                    _.map(that.entityObject.entity.classifications, function(obj) {
                        if (obj.entityGuid === that.id) {
                            tagList.push(obj.typeName);
                        }
                    });
                    var view = new AddTagModalView({
                        guid: that.id,
                        tagList: tagList,
                        callback: function() {
                            that.fetchCollection();
                        },
                        showLoader: that.showLoader.bind(that),
                        hideLoader: that.hideLoader.bind(that),
                        collection: that.classificationDefCollection,
                        enumDefCollection: that.enumDefCollection
                    });
                    view.modal.on('ok', function() {
                        Utils.showTitleLoader(that.$('.page-title .fontLoader'), that.$('.entityDetail'));
                    });
                });
            },
            onClickAddTermBtn: function(e) {
                var that = this,
                    entityGuid = that.id,
                    associatedTerms = this.collection.first().get('entity').relationshipAttributes.meanings;


                require(['views/glossary/AssignTermLayoutView'], function(AssignTermLayoutView) {
                    var view = new AssignTermLayoutView({
                        guid: that.id,
                        callback: function() {
                            that.fetchCollection();
                        },
                        associatedTerms: associatedTerms,
                        showLoader: that.showLoader.bind(that),
                        hideLoader: that.hideLoader.bind(that),
                        glossaryCollection: that.glossaryCollection
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
            renderLineageLayoutView: function(obj) {
                var that = this;
                require(['views/graph/LineageLayoutView'], function(LineageLayoutView) {
                    that.RLineageLayoutView.show(new LineageLayoutView(obj));
                });
            },
            renderRelationshipLayoutView: function(obj) {
                var that = this;
                require(['views/graph/RelationshipLayoutView'], function(RelationshipLayoutView) {
                    that.RRelationshipLayoutView.show(new RelationshipLayoutView(obj));
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
            renderReplicationAuditTableLayoutView: function(obj) {
                var that = this;
                require(['views/audit/ReplicationAuditTableLayoutView'], function(ReplicationAuditTableLayoutView) {
                    that.RReplicationAuditTableLayoutView.show(new ReplicationAuditTableLayoutView(obj));
                });
            },
            renderProfileLayoutView: function(obj) {
                var that = this;
                require(['views/profile/ProfileLayoutView'], function(ProfileLayoutView) {
                    that.RProfileLayoutView.show(new ProfileLayoutView(obj));
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