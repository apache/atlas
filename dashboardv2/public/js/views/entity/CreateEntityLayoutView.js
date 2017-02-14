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
    'hbs!tmpl/entity/CreateEntityLayoutView_tmpl',
    'utils/Utils',
    'collection/VTagList',
    'collection/VEntityList',
    'models/VEntity',
    'modules/Modal',
    'utils/Messages',
    'datetimepicker',
    'moment',
    'utils/UrlLinks',
    'collection/VSearchList',
    'utils/Enums',
    'utils/Globals'
], function(require, Backbone, CreateEntityLayoutViewTmpl, Utils, VTagList, VEntityList, VEntity, Modal, Messages, datepicker, moment, UrlLinks, VSearchList, Enums, Globals) {

    var CreateEntityLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends CreateEntityLayoutView */
        {
            _viewName: 'CreateEntityLayoutView',

            template: CreateEntityLayoutViewTmpl,

            templateHelpers: function() {
                return {
                    guid: this.guid
                };
            },

            /** Layout sub regions */
            regions: {},

            /** ui selector cache */
            ui: {
                entityName: "[data-id='entityName']",
                entityList: "[data-id='entityList']",
                entityInputData: "[data-id='entityInputData']",
                toggleRequired: 'input[name="toggleRequired"]',
                assetName: "[data-id='assetName']",
                entityInput: "[data-id='entityInput']"
            },
            /** ui events hash */
            events: function() {
                var events = {};
                events["change " + this.ui.entityList] = "onEntityChange";
                events["change " + this.ui.toggleRequired] = function(e) {
                    this.requiredAllToggle(e.currentTarget.checked)
                };
                return events;
            },
            /**
             * intialize a new CreateEntityLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'guid', 'callback', 'showLoader', 'entityDefCollection'));
                var that = this,
                    entityTitle, okLabel;
                this.searchCollection = new VSearchList();
                this.searchCollection.url = UrlLinks.searchApiUrl(Enums.searchUrlType.DSL);
                this.selectStoreCollection = new Backbone.Collection();
                this.collection = new VEntityList();
                this.entityModel = new VEntity();
                if (this.guid) {
                    this.collection.modelAttrName = "createEntity"
                }
                this.searchQueryList = [];
                this.asyncFetchCounter = 0;
                this.asyncFetchLOVCounter = 0;
                this.required = true;
                if (this.guid) {
                    entityTitle = 'Edit entity';
                    okLabel = 'Update';
                } else {
                    entityTitle = 'Create entity';
                    okLabel = 'Create';
                }
                this.modal = new Modal({
                    title: entityTitle,
                    content: this,
                    cancelText: "Cancel",
                    okText: okLabel,
                    allowCancel: true,
                    okCloses: false,
                    resizable: true,
                    resizableOpts: {
                        minWidth: 600,
                        minHeight: 284,
                        handles: "n, e, s, w",
                        resize: function(event, ui) {
                            that.modal.$el.find('.modal-body').css('min-height', ui.size.height - 134 + 'px');
                            that.modal.$el.find('.modal-body').css('max-height', ui.size.height - 134 + 'px');
                        }
                    }
                }).open();

                this.modal.$el.find('button.ok').attr("disabled", true);
                this.ui.entityList.val("");
                this.modal.on('ok', function(e) {
                    that.okButton();
                });
                this.modal.on('closeModal', function() {
                    that.modal.trigger('cancel');
                });
            },
            bindEvents: function() {
                var that = this;
                this.listenTo(this.collection, "reset", function() {
                    this.decrementCounter('asyncFetchCounter');
                    this.entityCollectionList();
                }, this);
                this.listenTo(this.collection, 'error', function() {
                    this.decrementCounter('asyncFetchCounter');
                    if (this.asyncFetchCounter === 0) {
                        this.hideLoader();
                    }
                }, this);
                this.listenTo(this.searchCollection, "reset", function() {
                    var that = this;
                    this.decrementCounter('asyncFetchLOVCounter');
                    _.each(this.searchCollection.fullCollection.models, function(model) {
                        var obj = model.toJSON();
                        obj['queryText'] = model.collection.queryText;
                        that.selectStoreCollection.push(obj);
                    })
                    this.addJsonSearchData();
                }, this);
                this.listenTo(this.searchCollection, 'error', function(data, key) {
                    this.decrementCounter('asyncFetchLOVCounter');
                    this.addJsonSearchData();
                }, this);
                this.ui.entityInputData.on("keyup", "textarea", function() {
                    var value = this.value;
                    try {
                        if (value && value.length) {
                            JSON.parse(value);
                            $(this).removeClass('errorClass');
                        }
                    } catch (err) {
                        $(this).addClass('errorClass');
                    }
                });
                this.ui.entityInputData.on('keyup change dp.change', 'input.true,select.true', function(e) {
                    if (this.value !== "") {
                        if ($(this).data('select2')) {
                            $(this).data('select2').$container.find('.select2-selection').removeClass("errorClass")
                        } else {
                            $(this).removeClass('errorClass');
                        }
                    } else {
                        if ($(this).data('select2')) {
                            $(this).data('select2').$container.find('.select2-selection').addClass("errorClass")
                        } else {
                            $(this).addClass('errorClass');
                        }
                    }
                });
            },
            onRender: function() {
                this.bindEvents();
                this.fetchCollections();
            },
            decrementCounter: function(counter) {
                if (this[counter] > 0) {
                    --this[counter];
                }
            },
            fetchCollections: function() {
                if (this.guid) {
                    this.collection.url = UrlLinks.entitiesApiUrl(this.guid);
                    this.collection.fetch({ reset: true });
                } else {
                    this.entityCollectionList();
                }
            },
            entityCollectionList: function() {
                this.ui.entityList.empty();
                var that = this,
                    name = "",
                    value;
                if (this.guid) {
                    this.collection.each(function(val) {
                        name += _.escape(val.get("entity").attributes.name) || _.escape(val.get("entity").attributes.qualifiedName) || _.escape(val.get("entity").attributes.id);
                        that.entityData = val;
                    });
                    this.ui.assetName.html(name);
                    this.onEntityChange(null, this.entityData);
                } else {
                    var str = '<option selected="selected" disabled="disabled">--Select entity-type--</option>';
                    this.entityDefCollection.fullCollection.comparator = function(model) {
                        return model.get('name');
                    }
                    this.entityDefCollection.fullCollection.sort().each(function(val) {
                        if (Globals.entityTypeConfList) {
                            if (_.isEmptyArray(Globals.entityTypeConfList)) {
                                str += '<option>' + _.escape(val.get("name")) + '</option>';
                            } else {
                                if (_.contains(Globals.entityTypeConfList, val.get("name"))) {
                                    str += '<option>' + _.escape(val.get("name")) + '</option>';
                                }
                            }
                        }
                    });
                    this.ui.entityList.html(str);
                }
            },
            capitalize: function(string) {
                return string.charAt(0).toUpperCase() + string.slice(1);
            },
            requiredAllToggle: function(checked) {
                if (checked) {
                    this.ui.entityInputData.find('div.true').show();
                    this.ui.entityInputData.find('fieldset div.true').show();
                    this.ui.entityInputData.find('fieldset').show();
                    this.required = false;
                } else {
                    this.ui.entityInputData.find('fieldset').each(function() {
                        if (!$(this).find('div').hasClass('false')) {
                            $(this).hide();
                        }
                    });
                    this.ui.entityInputData.find('div.true').hide();
                    this.ui.entityInputData.find('fieldset div.true').hide();
                    this.required = true;
                }

            },
            onEntityChange: function(e, value) {
                this.modal.$el.find('button.ok').prop("disabled", false);
                var that = this,
                    typeName = value && value.get('entity') ? value.get('entity').typeName : null;
                this.showLoader();
                this.ui.entityInputData.empty();
                if (typeName) {
                    this.collection.url = UrlLinks.entitiesDefApiUrl(typeName);
                } else if (e) {
                    this.collection.url = UrlLinks.entitiesDefApiUrl(e.target.value);
                    this.collection.modelAttrName = 'attributeDefs';
                }
                this.collection.fetch({
                    success: function(model, data) {
                        that.subAttributeData(data)
                    },
                    complete: function() {
                        that.initilizeElements();
                    },
                    silent: true
                });
            },
            subAttributeData: function(data) {
                var that = this,
                    attributeInput = "",
                    alloptional = false;
                _.each(data.attributeDefs, function(value) {
                    if (value.isOptional == true) {
                        alloptional = true;
                    }
                    attributeInput += that.getContainer(value);
                });
                if (attributeInput !== "") {
                    entityTitle = that.getFieldSet(data, alloptional, attributeInput);
                    that.ui.entityInputData.prepend(entityTitle);
                }
                if (data.superTypes && data.superTypes.length > 0) {
                    for (var j = 0; j < data.superTypes.length; j++) {
                        var superTypeAttr = data.superTypes[j];
                        that.fetchTagSubData(superTypeAttr);
                    }
                }

                if (this.required) {
                    this.ui.entityInputData.find('fieldset div.true').hide()
                    this.ui.entityInputData.find('div.true').hide();
                }
                if (!('placeholder' in HTMLInputElement.prototype)) {
                    this.ui.entityInputData.find("input,select,textarea").placeholder();
                }
            },
            fetchTagSubData: function(entityName) {
                var that = this;
                this.collection.url = UrlLinks.entitiesDefApiUrl(entityName);
                this.collection.modelAttrName = 'attributeDefs';
                this.asyncFetchCounter++;
                this.collection.fetch({
                    success: function(model, data) {
                        that.subAttributeData(data);
                    },
                    complete: function() {
                        that.decrementCounter('asyncFetchCounter');
                        that.initilizeElements();
                    },
                    silent: true
                });
            },
            initilizeElements: function() {
                var that = this;
                if (this.asyncFetchCounter === 0) {
                    this.$('input[data-type="date"]').each(function() {
                        if (!$(this).data('datepicker')) {
                            $(this).datetimepicker({
                                format: 'DD MMMM YYYY'
                            });
                        }
                    });
                    this.initializeValidation();

                    if (this.ui.entityInputData.find('fieldset').length > 0 && this.ui.entityInputData.find('select.true,input.true').length === 0) {
                        this.requiredAllToggle(this.ui.entityInputData.find('select.true,input.true').length === 0);
                        this.ui.toggleRequired.prop('checked', true);
                    } else {
                        this.ui.entityInputData.find('fieldset').each(function() {
                            if (!$(this).find('div').hasClass('false')) {
                                $(this).hide();
                            }
                        });
                    }
                    this.$('select[data-type="boolean"]').each(function(value, key) {
                        var dataKey = $(key).data('key');
                        if (that.entityData) {
                            var setValue = that.entityData.get("entity").attributes[dataKey];
                            this.value = setValue;
                        }
                    });
                    if (this.ui.entityInputData.find('select').length) {
                        this.ui.entityInputData.find('select').each(function() {
                            that.addJsonSearchData();
                        });
                    } else {
                        this.hideLoader();
                    }
                }
            },
            initializeValidation: function() {
                // IE9 allow input type number
                var regex = /^[0-9]*((?=[^.]|$))?$/, // allow only numbers [0-9] 
                    removeText = function(e, value) {
                        if (!regex.test(value)) {
                            var txtfld = e.currentTarget;
                            var newtxt = txtfld.value.slice(0, txtfld.value.length - 1);
                            txtfld.value = newtxt;
                        }
                    }
                this.$('input[data-type="int"],input[data-type="long"]').on('keydown', function(e) {
                    // allow only numbers [0-9] 
                    if (!regex.test(e.currentTarget.value)) {
                        return false;
                    }
                });
                this.$('input[data-type="int"],input[data-type="long"]').on('paste', function(e) {
                    return false;
                });

                this.$('input[data-type="long"],input[data-type="int"]').on('keyup click', function(e) {
                    removeText(e, e.currentTarget.value);
                });
            },
            getContainer: function(value) {
                var entityLabel = this.capitalize(value.name);
                return '<div class="row row-margin-bottom ' + value.isOptional + '"><span class="col-md-3">' +
                    '<label class="' + value.isOptional + '">' + entityLabel + (value.isOptional == true ? '' : ' <span class="requiredInput">*</span>') + '</label></span>' +
                    '<span class="col-md-9 position-relative">' + (this.getElement(value)) +
                    '<span class="spanEntityType" title="Data Type : ' + value.typeName + '">' + '(' + Utils.escapeHtml(value.typeName) + ')' + '</span></input></span></div>';
            },
            getFieldSet: function(data, alloptional, attributeInput) {
                return '<fieldset class="scheduler-border' + (alloptional ? " alloptional" : "") + '"><legend class="scheduler-border">' + data.name + '</legend>' + attributeInput + '</fieldset>';
            },
            getSelect: function(value, entityValue) {
                if (value.typeName === "boolean") {
                    return '<select class="form-control row-margin-bottom ' + (value.isOptional === true ? "false" : "true") + '" data-type="' + value.typeName + '" data-key="' + value.name + '" data-id="entityInput">' +
                        '<option disabled="disabled">--Select true or false--</option><option value="true">true</option>' +
                        '<option value="false">false</option></select>';
                } else {
                    var splitTypeName = value.typeName.split("<");
                    if (splitTypeName.length > 1) {
                        splitTypeName = splitTypeName[1].split(">")[0];
                    } else {
                        splitTypeName = value.typeName;
                    }
                    return '<select class="form-control row-margin-bottom entityInputBox ' + (value.isOptional === true ? "false" : "true") + '" data-type="' + value.typeName +
                        '" data-key="' + value.name + '"data-id="entitySelectData" data-queryData="' + splitTypeName + '">' + (this.guid ? entityValue : "") + '</select>';
                }

            },
            getTextArea: function(value, entityValue) {
                return '<textarea class="form-control entityInputBox ' + (value.isOptional === true ? "false" : "true") + '"' +
                    ' data-type="' + value.typeName + '"' +
                    ' data-key="' + value.name + '"' +
                    ' placeholder="' + value.name + '"' +
                    ' data-id="entityInput">' + entityValue + '</textarea>';
            },
            getInput: function(value, entityValue) {
                return '<input class="form-control entityInputBox ' + (value.isOptional === true ? "false" : "true") + '"' +
                    ' data-type="' + value.typeName + '"' +
                    ' value="' + entityValue + '"' +
                    ' data-key="' + value.name + '"' +
                    ' placeholder="' + value.name + '"' +
                    ' data-id="entityInput">';
            },
            getElement: function(value) {
                var typeName = value.typeName,
                    entityValue = "";
                if (this.guid) {
                    var dataValue = this.entityData.get("entity").attributes[value.name];
                    if (_.isObject(dataValue)) {
                        entityValue = JSON.stringify(dataValue);
                    } else {
                        if (dataValue) {
                            entityValue = dataValue;
                        }
                        if (value.typeName === "date" && dataValue) {
                            entityValue = moment(dataValue).format("DD MMMM YYYY");
                        }
                    }
                }
                if (typeName && this.entityDefCollection.fullCollection.find({ name: typeName })) {
                    if (!_.contains(this.searchQueryList, typeName)) {
                        this.searchQueryList.push(typeName);
                        $.extend(this.searchCollection.queryParams, { query: typeName });
                        ++this.asyncFetchLOVCounter;
                        this.searchCollection.fetch({ reset: true });
                    }
                    return this.getSelect(value, entityValue);
                } else if (typeName === "boolean" || typeName.indexOf("array") > -1) {
                    var splitTypeName = typeName.split("<");
                    if (splitTypeName.length > 1) {
                        splitTypeName = splitTypeName[1].split(">")[0];
                        if (splitTypeName && this.entityDefCollection.fullCollection.find({ name: splitTypeName })) {
                            if (!_.contains(this.searchQueryList, splitTypeName)) {
                                this.searchQueryList.push(splitTypeName);
                                $.extend(this.searchCollection.queryParams, { query: splitTypeName });
                                ++this.asyncFetchLOVCounter;
                                this.searchCollection.fetch({ reset: true });
                            }
                        }
                    }
                    return this.getSelect(value, entityValue);
                } else if (typeName.indexOf("map") > -1) {
                    return this.getTextArea(value, entityValue);
                } else {
                    return this.getInput(value, entityValue);
                }
            },
            okButton: function() {
                var that = this;
                this.showLoader();
                this.parentEntity = this.ui.entityList.val();
                var entity = {};
                var referredEntities = {};
                var extractValue = function(value) {
                    if (_.isArray(value)) {
                        if (that.selectStoreCollection.length) {
                            var parseData = [];
                            value.map(function(val) {
                                var temp = {} // I9 support;
                                temp['labelName'] = val;
                                if (that.selectStoreCollection.findWhere(temp)) {
                                    var valueData = that.selectStoreCollection.findWhere(temp).toJSON();
                                    if (valueData) {
                                        //referredEntities[valueData.guid] = valueData;
                                        parseData.push({ guid: valueData.guid, typeName: valueData.typeName });
                                    }
                                }
                            });
                        }
                    } else {
                        if (that.selectStoreCollection.length) {
                            var temp = {} // I9 support;
                            temp['labelName'] = value;
                            if (that.selectStoreCollection.findWhere(temp)) {
                                var valueData = that.selectStoreCollection.findWhere(temp).toJSON();
                                if (valueData) {
                                    //referredEntities[valueData.guid] = valueData;
                                    var parseData = { guid: valueData.guid, typeName: valueData.typeName };
                                }
                            }
                        }
                    }
                    return parseData;
                }
                try {
                    this.ui.entityInputData.find("input,select,textarea").each(function() {
                        var value = $(this).val();
                        if ($(this).val() && $(this).val().trim) {
                            value = $(this).val().trim();
                        }
                        if (this.nodeName === "TEXTAREA") {
                            try {
                                if (value && value.length) {
                                    JSON.parse(value);
                                    $(this).removeClass('errorClass');
                                }
                            } catch (err) {
                                throw new Error(err.message);
                                $(this).addClass('errorClass');
                            }
                        }
                        // validation
                        if ($(this).hasClass("true")) {
                            if (value == "" || value == undefined) {
                                if ($(this).data('select2')) {
                                    $(this).data('select2').$container.addClass("errorClass")
                                } else {
                                    $(this).addClass('errorClass');
                                }
                                that.hideLoader();
                                throw new Error("Please fill the required fields");
                                return;
                            }
                        }
                        var dataTypeEnitity = $(this).data('type');
                        var datakeyEntity = $(this).data('key');

                        // Extract Data
                        if (dataTypeEnitity && datakeyEntity) {
                            if (that.entityDefCollection.fullCollection.find({ name: dataTypeEnitity })) {
                                entity[datakeyEntity] = extractValue(value);
                            } else if (typeof dataTypeEnitity === 'string' && datakeyEntity.indexOf("Time") > -1) {
                                entity[datakeyEntity] = Date.parse(value);
                            } else if (dataTypeEnitity.indexOf("map") > -1) {
                                try {
                                    if (value && value.length) {
                                        parseData = JSON.parse(value);
                                        entity[datakeyEntity] = parseData;
                                    }
                                } catch (err) {
                                    $(this).addClass('errorClass');
                                    throw new Error(datakeyEntity + " : " + err.message);
                                    return;
                                }
                            } else if (dataTypeEnitity.indexOf("array") > -1 && dataTypeEnitity.indexOf("string") === -1) {
                                entity[datakeyEntity] = extractValue(value);
                            } else {
                                if (_.isString(value)) {
                                    if (value.length) {
                                        entity[datakeyEntity] = value;
                                    }
                                } else {
                                    entity[datakeyEntity] = value;
                                }

                            }
                        }
                    });
                    var entityJson = {
                        "entity": {
                            "typeName": (this.guid ? this.entityData.get("entity").typeName : this.parentEntity),
                            "attributes": entity,
                            "guid": (this.guid ? this.guid : -1)
                        },
                        "referredEntities": referredEntities
                    };
                    this.entityModel.createOreditEntity({
                        data: JSON.stringify(entityJson),
                        type: "POST",
                        success: function(model, response) {
                            that.modal.close();
                            Utils.notifySuccess({
                                content: "entity " + Messages[that.guid ? 'editSuccessMessage' : 'addSuccessMessage']
                            });
                            if (that.guid && that.callback) {
                                that.callback();
                            } else {
                                if (model.entitiesMutated && (model.entitiesMutated.CREATE || model.entitiesMutated.UPDATE)) {
                                    that.setUrl('#!/detailPage/' + (model.entitiesMutated.CREATE ? model.entitiesMutated.CREATE[0].guid : model.entitiesMutated.UPDATE[0].guid), true);
                                }
                            }
                        },
                        complete: function() {
                            that.hideLoader();
                        }
                    });

                } catch (e) {
                    Utils.notifyError({
                        content: e.message
                    });
                    that.hideLoader();
                }
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
            showLoader: function() {
                this.$('.entityLoader').show();
                this.$('.entityInputData').hide();
            },
            hideLoader: function() {
                this.$('.entityLoader').hide();
                this.$('.entityInputData').show();
            },
            addJsonSearchData: function() {
                if (this.asyncFetchLOVCounter === 0) {
                    var that = this,
                        queryText,
                        str = '';

                    // Add oprions in select
                    if (this.selectStoreCollection.length) {
                        var appendOption = function(optionValue) {
                            var obj = optionValue.toJSON(),
                                labelName = (_.escape(obj.displayText) || _.escape(obj.attributes && obj.attributes.name ? obj.attributes.name : null) || obj.guid);
                            optionValue.set('labelName', labelName);
                            if (labelName) {
                                var str = '<option>' + _.escape(labelName) + '</option>';
                            }
                            this.$('select[data-queryData="' + obj.queryText + '"]').append(str);
                        }
                        _.each(this.selectStoreCollection.models, function(value) {
                            var obj = value.toJSON();
                            if (obj.status) {
                                if (!Enums.entityStateReadOnly[obj.status]) {
                                    appendOption(value);
                                }
                            } else {
                                appendOption(value);
                            }
                        });
                    }

                    $('select[data-id="entitySelectData"]').each(function(value, key) {
                        var $this = $(this),
                            keyData = $(this).data("key"),
                            typeData = $(this).data("type"),
                            placeholderName = "Select a " + typeData + " from the dropdown list";

                        $this.attr("multiple", ($this.data('type').indexOf("array") === -1 ? false : true));

                        if (that.guid) {
                            var dataValue = that.entityData.get("entity").attributes[keyData];
                            var selectedValue = [];
                            // if (that.selectStoreCollection.length) {
                            //     var selectedValue = [];
                            // }
                            var setValue = function(selectValue) {
                                var obj = selectValue.toJSON();
                                if (dataValue !== null && _.isArray(dataValue)) {
                                    _.each(dataValue, function(obj) {
                                        if (obj.guid === selectValue.attributes.guid) {
                                            selectedValue.push(selectValue.attributes.labelName);
                                        }
                                    });
                                } else if (dataValue !== null) {
                                    if (dataValue.guid === selectValue.attributes.guid) {
                                        selectedValue.push(selectValue.attributes.labelName);
                                    }
                                }
                            }
                            that.selectStoreCollection.each(function(storedValue) {
                                var obj = storedValue.toJSON();
                                if (obj.status) {
                                    if (!Enums.entityStateReadOnly[obj.status]) {
                                        setValue(storedValue);
                                    }
                                } else {
                                    setValue(storedValue);
                                }
                            });

                            // Array of string.
                            if (selectedValue.length === 0 && dataValue && dataValue.length) {
                                var str = "";
                                _.each(dataValue, function(obj) {
                                    if (_.isString(obj)) {
                                        selectedValue.push(obj);
                                        str += '<option>' + _.escape(obj) + '</option>';
                                    }
                                });
                                $this.html(str);
                            }
                            if (selectedValue) {
                                $this.val(selectedValue);
                            }
                        } else {
                            $this.val([]);
                        }
                        $this.select2({
                            placeholder: placeholderName,
                            allowClear: true,
                            tags: ($this.data('querydata') == "string" ? true : false)
                        });
                    });
                    this.hideLoader();
                }

            }
        });
    return CreateEntityLayoutView;
});
