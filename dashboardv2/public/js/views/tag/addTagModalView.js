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
    'hbs!tmpl/tag/addTagModalView_tmpl',
    'collection/VTagList',
    'collection/VCommonList',
    'modules/Modal',
    'models/VEntity',
    'utils/Utils',
    'utils/UrlLinks',
    'utils/Enums',
    'utils/Messages',
], function(require, AddTagModalViewTmpl, VTagList, VCommonList, Modal, VEntity, Utils, UrlLinks, Enums, Messages) {
    'use strict';

    var AddTagModel = Marionette.LayoutView.extend({
        template: AddTagModalViewTmpl,
        templateHelpers: function() {
            return {
                tagModel: this.tagModel
            };
        },

        regions: {},
        ui: {
            addTagOptions: "[data-id='addTagOptions']",
            tagAttribute: "[data-id='tagAttribute']"
        },
        events: function() {
            var events = {};
            events["change " + this.ui.addTagOptions] = 'onChangeTagDefination';
            return events;
        },
        /**
         * intialize a new AddTagModel Layout
         * @constructs
         */
        initialize: function(options) {
            _.extend(this, _.pick(options, 'modalCollection', 'guid', 'callback', 'multiple', 'showLoader', 'hideLoader', 'tagList', 'tagModel', 'enumDefCollection'));
            this.collection = new VTagList();
            this.commonCollection = new VTagList();
            var that = this,
                modalObj = {
                    title: 'Add Tag',
                    content: this,
                    okText: 'Add',
                    cancelText: "Cancel",
                    allowCancel: true,
                },
                state = this.tagModel ? false : true;
            if (this.tagModel) {
                modalObj.title = 'Edit Tag';
                modalObj.okText = 'Update';
            }
            this.modal = new Modal(modalObj).open();
            this.modal.$el.find('button.ok').attr("disabled", state);
            this.on('ok', function() {
                var tagName = this.tagModel ? this.tagModel.typeName : this.ui.addTagOptions.val(),
                    tagAttributes = {},
                    tagAttributeNames = this.$(".attrName"),
                    obj = {
                        tagName: tagName,
                        tagAttributes: tagAttributes,
                        guid: [],
                        skipEntity: [],
                        deletedEntity: []
                    };
                tagAttributeNames.each(function(i, item) {
                    var selection = $(item).data("key");
                    tagAttributes[selection] = $(item).val() || null;
                });

                if (that.multiple) {
                    _.each(that.multiple, function(entity, i) {
                        var name = Utils.getName(entity.model);
                        if (Enums.entityStateReadOnly[entity.model.status]) {
                            obj.deletedEntity.push(name);
                        } else {
                            if (_.indexOf((entity.model.classificationNames || _.pluck(entity.model.classifications, 'typeName')), tagName) === -1) {
                                obj.guid.push(entity.model.guid)
                            } else {
                                obj.skipEntity.push(name);
                            }
                        }
                    });
                    if (obj.deletedEntity.length) {
                        Utils.notifyError({
                            html: true,
                            content: "<b>" + obj.deletedEntity.join(', ') +
                                "</b> " + (obj.deletedEntity.length === 1 ? "entity " : "entities ") +
                                Messages.assignDeletedEntity
                        });
                    }
                    if (obj.skipEntity.length) {
                        var text = "<b>" + obj.skipEntity.length + " of " + that.multiple.length +
                            "</b> entities selected have already been associated with <b>" + tagName +
                            "</b> tag, Do you want to associate the tag with other entities ?",
                            removeCancelButton = false;
                        if ((obj.skipEntity.length + obj.deletedEntity.length) === that.multiple.length) {
                            text = (obj.skipEntity.length > 1 ? "All selected" : "Selected") + " entities have already been associated with <b>" + tagName + "</b> tag";
                            removeCancelButton = true;
                        }
                        var notifyObj = {
                            text: text,
                            ok: function(argument) {
                                if (obj.guid.length) {
                                    that.saveTagData(obj);
                                } else {
                                    that.hideLoader();
                                }
                            },
                            cancel: function(argument) {
                                that.hideLoader();
                                obj = {
                                    tagName: tagName,
                                    tagAttributes: tagAttributes,
                                    guid: [],
                                    skipEntity: [],
                                    deletedEntity: []
                                }
                            }
                        }
                        if (removeCancelButton) {
                            notifyObj['confirm'] = {
                                confirm: true,
                                buttons: [{
                                        text: 'Ok',
                                        addClass: 'btn-primary',
                                        click: function(notice) {
                                            notice.remove();
                                            obj = {
                                                tagName: tagName,
                                                tagAttributes: tagAttributes,
                                                guid: [],
                                                skipEntity: [],
                                                deletedEntity: []
                                            }
                                        }
                                    },
                                    null
                                ]
                            }
                        }

                        Utils.notifyConfirm(notifyObj)
                    } else {
                        if (obj.guid.length) {
                            that.saveTagData(obj);
                        } else {
                            that.hideLoader();
                        }
                    }
                } else {
                    obj.guid.push(that.guid);
                    that.saveTagData(obj);
                }
            });
            this.on('closeModal', function() {
                this.modal.trigger('cancel');
            });
            this.bindEvents();
        },

        onRender: function() {
            var that = this;
            $.extend(this.collection.queryParams, { type: 'classification' });
            this.hideAttributeBox();
            this.collection.fetch({
                reset: true,
                complete: function() {
                    if (that.tagModel) {
                        that.fetchTagSubData(that.tagModel.typeName);
                    }
                    that.showAttributeBox();
                },
            });
        },
        bindEvents: function() {
            var that = this;
            this.enumArr = [];
            this.listenTo(this.collection, 'reset', function() {
                this.tagsCollection();
            }, this);
            this.listenTo(this.commonCollection, 'reset', function() {
                this.subAttributeData();
            }, this);
        },
        tagsCollection: function() {
            var that = this;
            this.collection.fullCollection.comparator = function(model) {
                return Utils.getName(model.toJSON(), 'name').toLowerCase();
            }

            var str = '<option selected="selected" disabled="disabled">-- Select a tag from the dropdown list --</option>';
            this.collection.fullCollection.sort().each(function(obj, key) {
                var name = Utils.getName(obj.toJSON(), 'name');
                if (name === "TaxonomyTerm") {
                    return;
                }
                // using obj.get('name') insted of name variable because if html is presen in name then escaped name will not found in tagList.
                if (_.indexOf(that.tagList, obj.get('name')) === -1) {
                    str += '<option ' + (that.tagModel && that.tagModel.typeName === name ? 'selected' : '') + '>' + name + '</option>';
                }
            });
            this.ui.addTagOptions.html(str);
            this.ui.addTagOptions.select2({
                placeholder: "Select Tag",
                allowClear: false
            });
        },
        onChangeTagDefination: function() {
            this.ui.addTagOptions.select2("open").select2("close");
            this.ui.tagAttribute.empty();
            var saveBtn = this.modal.$el.find('button.ok');
            saveBtn.prop("disabled", false);
            var tagname = this.ui.addTagOptions.val();
            this.hideAttributeBox();
            this.fetchTagSubData(tagname);
        },
        fetchTagSubData: function(tagname) {
            var attributeDefs = Utils.getNestedSuperTypeObj({
                data: this.collection.fullCollection.find({ name: tagname }).toJSON(),
                collection: this.collection,
                attrMerge: true
            });
            this.subAttributeData(attributeDefs);
        },
        showAttributeBox: function() {
            this.$('.attrLoader').hide();
            this.$('.form-group.hide').removeClass('hide');
            if (this.ui.tagAttribute.children().length !== 0) {
                this.ui.tagAttribute.parent().show();
            }
        },
        hideAttributeBox: function() {
            this.ui.tagAttribute.children().empty();
            this.ui.tagAttribute.parent().hide();
            this.$('.attrLoader').show();
        },
        subAttributeData: function(attributeDefs) {
            var that = this;
            if (attributeDefs) {
                _.each(attributeDefs, function(obj) {
                    var name = Utils.getName(obj, 'name');
                    var typeName = Utils.getName(obj, 'typeName');
                    var typeNameValue = that.enumDefCollection.fullCollection.findWhere({ 'name': typeName });
                    if (typeNameValue) {
                        var str = "<option disabled='disabled'" + (!that.tagModel ? 'selected' : '') + ">-- Select " + typeName + " --</option>";
                        var enumValue = typeNameValue.get('elementDefs');
                        _.each(enumValue, function(key, value) {
                            str += '<option ' + (that.tagModel && key.value === _.values(that.tagModel.attributes)[0] ? 'selected' : '') + '>' + key.value + '</option>';
                        })
                        that.ui.tagAttribute.append('<div class="form-group"><label>' + name + '</label>' +
                            '<select class="form-control attributeInputVal attrName" data-key="' + name + '">' + str + '</select></div>');
                    } else {
                        that.ui.tagAttribute.append('<div class="form-group"><label>' + name + '</label>' +
                            '<input type="text" value="' + (that.tagModel ? (that.tagModel.attributes[name] == null ? '' : that.tagModel.attributes[name]) : '') + '" class="form-control attributeInputVal attrName" data-key="' + name + '" ></input></div>');
                    }
                });
                this.showAttributeBox();
            }
        },
        saveTagData: function(options) {
            var that = this;
            this.entityModel = new VEntity();
            var tagName = options.tagName,
                tagAttributes = options.tagAttributes,
                json = {
                    "classification": {
                        "typeName": tagName,
                        "attributes": tagAttributes
                    },
                    "entityGuids": options.guid
                };
            if (this.tagModel) {
                json = [{
                    "typeName": tagName,
                    "attributes": tagAttributes
                }]
            }
            if (this.showLoader) {
                this.showLoader();
            }
            this.entityModel.saveTraitsEntity(this.tagModel ? options.guid : null, {
                skipDefaultError: true,
                data: JSON.stringify(json),
                type: this.tagModel ? 'PUT' : 'POST',
                success: function(data) {
                    var addupdatetext = that.tagModel ? 'updated successfully to ' : 'added to ';
                    Utils.notifySuccess({
                        content: "Tag " + tagName + " has been " + addupdatetext + (that.multiple ? "entities" : "entity")
                    });
                    if (options.modalCollection) {
                        options.modalCollection.fetch({ reset: true });
                    }
                    if (that.callback) {
                        that.callback();
                    }
                },
                cust_error: function(model, response) {
                    var message = "Tag " + tagName + " could not be added";
                    if (response && response.responseJSON) {
                        message = response.responseJSON.errorMessage;
                    }
                    Utils.notifyError({
                        content: message
                    });
                    if (that.hideLoader) {
                        that.hideLoader();
                    }
                }
            });
        },
    });
    return AddTagModel;
});
