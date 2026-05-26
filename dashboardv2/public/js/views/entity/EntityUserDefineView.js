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
    'hbs!tmpl/entity/EntityUserDefineView_tmpl',
    'models/VEntity',
    'utils/Utils',
    'utils/Enums',
    'utils/Messages',
    'utils/CommonViewFunction',
    'utils/UrlLinks',
], function(require, Backbone, EntityUserDefineView_tmpl, VEntity, Utils, Enums, Messages, CommonViewFunction, UrlLinks) {
    'use strict';

    return Backbone.Marionette.LayoutView.extend({
        _viewName: 'EntityUserDefineView',
        template: EntityUserDefineView_tmpl,
        templateHelpers: function() {
            return {
                customAttibutes: this.customAttibutes,
                readOnlyEntity: this.readOnlyEntity,
                swapItem: this.swapItem,
                saveAttrItems: this.saveAttrItems,
                divId_1: this.dynamicId_1,
                divId_2: this.dynamicId_2
            };
        },
        ui: {
            addAttr: "[data-id='addAttr']",
            saveAttrItems: "[data-id='saveAttrItems']",
            cancel: "[data-id='cancel']",
            addItem: "[data-id='addItem']",
            userDefineHeader: ".userDefinePanel .panel-heading"
        },
        events: function() {
            var events = {};
            events["click " + this.ui.addAttr] = 'onAddAttrClick';
            events["click " + this.ui.addItem] = 'onAddAttrClick';
            events["click " + this.ui.saveAttrItems] = 'onEditAttrClick';
            events["click " + this.ui.cancel] = 'onCancelClick';
            events["click " + this.ui.userDefineHeader] = 'onHeaderClick';
            return events;
        },
        initialize: function(options) {
            _.extend(this, _.pick(options, 'entity', 'customFilter', 'renderAuditTableLayoutView', 'entityDefCollection'));
            this.userDefineAttr = this.entity && this.entity.customAttributes || [];
            this.initialCall = false;
            this.swapItem = false, this.saveAttrItems = false;
            this.readOnlyEntity = this.customFilter === undefined ? Enums.entityStateReadOnly[this.entity.status] : this.customFilter;
            this.entityModel = new VEntity(this.entity);
            this.dynamicId_1 = CommonViewFunction.getRandomIdAndAnchor();
            this.dynamicId_2 = CommonViewFunction.getRandomIdAndAnchor();
            this.generateTableFields();
        },
        onRender: function() {},
        renderEntityUserDefinedItems: function() {
            var that = this;
            require(['views/entity/EntityUserDefineItemView'], function(EntityUserDefineItemView) {
                that.itemView = new EntityUserDefineItemView({ items: that.customAttibutes, updateButtonState: that.updateButtonState.bind(that) });
                that.REntityUserDefinedItemView.show(that.itemView);
            });
        },
        bindEvents: {},
        addChildRegion: function() {
            this.addRegions({
                REntityUserDefinedItemView: "#r_entityUserDefinedItemView"
            });
            this.renderEntityUserDefinedItems();
        },
        onHeaderClick: function() {
            var that = this;
            $(".userDefinePanel").on("hidden.bs.collapse", function() {
                that.swapItem = false;
                that.saveAttrItems = false;
                that.initialCall = false;
                that.render();
                if (that.customAttibutes.length > 0) {
                    $('.userDefinePanel').find(that.ui.userDefineHeader.attr('href')).removeClass('in');
                    that.ui.userDefineHeader.addClass('collapsed').attr('aria-expanded', false);
                }
            });
        },
        onAddAttrClick: function() {
            this.swapItem = !this.swapItem;
            if (this.customFilter === undefined) {
                this.saveAttrItems = this.swapItem === true ? true : false;
            } else {
                this.saveAttrItems = false;
            }
            this.initialCall = true;
            this.render();
            if (this.swapItem === true) {
                this.addChildRegion();
            }
        },
        generateTableFields: function() {
            var that = this;
            this.customAttibutes = [];
            _.each(Object.keys(that.userDefineAttr), function(key, i) {
                that.customAttibutes.push({
                    key: key,
                    value: that.userDefineAttr[key]
                });
            });
        },
        onEditAttrClick: function() {
            this.initialCall = this.customAttibutes.length > 0 ? false : true;
            this.setAttributeModal(this.itemView);
        },
        updateButtonState: function() {
            if (this.customAttibutes.length === 0) {
                this.swapItem = false;
                this.saveAttrItems = false;
                this.render();
            } else {
                return false;
            }
        },
        onCancelClick: function() {
            this.initialCall = false;
            this.swapItem = false;
            this.saveAttrItems = false;
            this.render();
        },
        structureAttributes: function(list) {
            var obj = {}
            list.map(function(o) {
                obj[o.key] = o.value;
            });
            return obj;
        },
        getMergedRelationshipAttributeDefs: function() {
            var typeName = this.entity && this.entity.typeName;
            if (!this.entityDefCollection || !typeName) {
                return [];
            }
            var entityDef = this.entityDefCollection.fullCollection.findWhere({ name: typeName });
            if (!entityDef) {
                return [];
            }
            var merged = Utils.getNestedSuperTypeObj({
                data: entityDef.toJSON(),
                collection: this.entityDefCollection,
                attrMerge: true,
                seperateRelatioshipAttr: true
            });
            return (merged && merged.relationshipAttributeDefs) || [];
        },
        isRelationshipAttrValueMissing: function(val) {
            if (val === undefined || val === null) {
                return true;
            }
            if (Array.isArray(val)) {
                return val.length === 0;
            }
            if (typeof val === 'object') {
                return !val.guid;
            }
            return false;
        },
        /**
         * Detail page GET keeps ignoreRelationships=true. For UDP save, one
         * extra GET with ignoreRelationships=false fills relationshipAttributes
         * so mandatory refs match the real edges (avoids multi-type attrs
         * e.g. table vs iceberg_table_storagedesc where search/entities[0]
         * picks the wrong neighbor).
         */
        enrichEntityPayloadForRelationshipSave: function(entityJson, done) {
            var that = this;
            entityJson.relationshipAttributes = entityJson.relationshipAttributes || {};
            var srcMeanings = that.entity && that.entity.relationshipAttributes &&
                that.entity.relationshipAttributes.meanings;
            var hadMeanings = !!(srcMeanings && srcMeanings.length);
            if (hadMeanings) {
                entityJson.relationshipAttributes.meanings = srcMeanings;
            }
            var fullUrl = UrlLinks.baseUrlV2 + '/entity/guid/' +
                encodeURIComponent(entityJson.guid) +
                '?ignoreRelationships=false';
            $.ajax({
                url: fullUrl,
                type: 'GET',
                dataType: 'json'
            }).done(function(resp) {
                var fullEntity = resp && resp.entity;
                if (fullEntity && fullEntity.relationshipAttributes) {
                    _.each(fullEntity.relationshipAttributes, function(val, key) {
                        if (key === 'meanings' && hadMeanings) {
                            return;
                        }
                        if (!that.isRelationshipAttrValueMissing(
                                entityJson.relationshipAttributes[key])) {
                            return;
                        }
                        entityJson.relationshipAttributes[key] = val;
                    });
                }
                done();
            }).fail(function() {
                that.enrichMandatoryRelsViaRelationshipSearch(entityJson, done);
            });
        },
        /**
         * Fallback when full entity GET fails: fill mandatory missing rel attrs
         * via relationship search (ambiguous if several types share attr name).
         */
        enrichMandatoryRelsViaRelationshipSearch: function(entityJson, done) {
            var that = this;
            var relDefs = this.getMergedRelationshipAttributeDefs();
            var toFetch = [];
            _.each(relDefs, function(rd) {
                if (rd.isOptional !== false) {
                    return;
                }
                var name = rd.name;
                if (!that.isRelationshipAttrValueMissing(
                        entityJson.relationshipAttributes[name])) {
                    return;
                }
                toFetch.push(rd);
            });
            if (!toFetch.length) {
                done();
                return;
            }
            var ajaxCalls = _.map(toFetch, function(rd) {
                var relationName = rd.name;
                return $.ajax({
                    url: UrlLinks.relationshipSearchV2ApiUrl(),
                    type: 'GET',
                    dataType: 'json',
                    data: {
                        limit: 100,
                        offset: 0,
                        guid: entityJson.guid,
                        disableDefaultSorting: true,
                        excludeDeletedEntities: false,
                        includeSubClassifications: true,
                        includeSubTypes: true,
                        includeClassificationAttributes: true,
                        relation: relationName,
                        getApproximateCount: true
                    }
                }).then(function(response) {
                    var entities = (response && response.entities) ? response.entities : [];
                    if (!entities.length) {
                        return;
                    }
                    var isSet = rd.cardinality === 'SET' ||
                        (rd.typeName && rd.typeName.indexOf('array') === 0);
                    if (isSet) {
                        entityJson.relationshipAttributes[relationName] = _.map(entities, function(ent) {
                            return { guid: ent.guid, typeName: ent.typeName };
                        });
                    } else {
                        var t = entities[0];
                        entityJson.relationshipAttributes[relationName] = {
                            guid: t.guid,
                            typeName: t.typeName
                        };
                    }
                });
            });
            $.when.apply($, ajaxCalls).always(function() {
                done();
            });
        },
        saveAttributes: function(list) {
            var that = this;
            var entityJson = that.entityModel.toJSON();
            var properties = that.structureAttributes(list);
            entityJson.customAttributes = properties;
            that.enrichEntityPayloadForRelationshipSave(entityJson, function() {
                var payload = { entity: entityJson };
                that.entityModel.createOreditEntity({
                    data: JSON.stringify(payload),
                    type: 'POST',
                    success: function() {
                        var msg = that.initialCall ? 'addSuccessMessage' : 'editSuccessMessage',
                            caption = "One or more user-defined propertie"; // 's' added in abbreviation fn
                        that.customAttibutes = list;
                        if (list.length === 0) {
                            msg = 'removeSuccessMessage';
                            caption = "One or more existing user-defined propertie";
                        }
                        Utils.notifySuccess({
                            content: caption + Messages.getAbbreviationMsg(true, msg)
                        });
                        that.swapItem = false;
                        that.saveAttrItems = false;
                        that.render();
                        if (that.renderAuditTableLayoutView) {
                            that.renderAuditTableLayoutView();
                        }
                    },
                    error: function(e) {
                        that.initialCall = false;
                        Utils.notifyError({
                            content: (e && e.message) ? e.message :
                                'Failed to save user-defined properties'
                        });
                        that.ui.saveAttrItems.attr("disabled", false);
                    },
                    complete: function() {
                        that.ui.saveAttrItems.attr("disabled", false);
                        that.initialCall = false;
                    }
                });
            });
        },
        setAttributeModal: function(itemView) {
            var self = this;
            this.ui.saveAttrItems.attr("disabled", true);
            var list = itemView.$el.find("[data-type]"),
                dataList = [];
            Array.prototype.push.apply(dataList, itemView.items);
            var field = CommonViewFunction.CheckDuplicateAndEmptyInput(list, dataList);
            if (field.validation && !field.hasDuplicate) {
                self.saveAttributes(itemView.items);
            } else {
                this.ui.saveAttrItems.attr("disabled", false);
            }
        }
    });
});