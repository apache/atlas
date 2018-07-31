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
    'moment',
    'utils/UrlLinks',
    'collection/VSearchList',
    'utils/Enums',
    'utils/Globals',
    'daterangepicker'
], function(require, Backbone, CreateEntityLayoutViewTmpl, Utils, VTagList, VEntityList, VEntity, Modal, Messages, moment, UrlLinks, VSearchList, Enums, Globals) {

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
                _.extend(this, _.pick(options, 'guid', 'callback', 'showLoader', 'entityDefCollection', 'typeHeaders'));
                var that = this,
                    entityTitle, okLabel;
                this.selectStoreCollection = new Backbone.Collection();
                this.collection = new VEntityList();
                this.entityModel = new VEntity();
                if (this.guid) {
                    this.collection.modelAttrName = "createEntity"
                }
                this.asyncReferEntityCounter = 0;
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
                    width: '50%'
                }).open();
                this.modal.$el.find('button.ok').attr("disabled", true);
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
                    this.entityCollectionList();
                }, this);
                this.listenTo(this.collection, 'error', function() {
                    this.hideLoader();
                }, this);
            },
            onRender: function() {
                this.bindEvents();
                if (!this.guid) {
                    this.bindRequiredField();
                }
                this.showLoader();
                this.fetchCollections();
            },
            bindRequiredField: function() {
                var that = this;
                this.ui.entityInputData.on("keyup change", "textarea", function(e) {
                    var value = this.value;
                    if (!value.length && $(this).hasClass('false')) {
                        $(this).removeClass('errorClass');
                        that.modal.$el.find('button.ok').prop("disabled", false);
                    } else {
                        try {
                            if (value && value.length) {
                                JSON.parse(value);
                                $(this).removeClass('errorClass');
                                that.modal.$el.find('button.ok').prop("disabled", false);
                            }
                        } catch (err) {
                            $(this).addClass('errorClass');
                            that.modal.$el.find('button.ok').prop("disabled", true);
                        }
                    }
                });

                this.ui.entityInputData.on('keyup change', 'input.true,select.true', function(e) {
                    if (this.value !== "") {
                        if ($(this).data('select2')) {
                            $(this).data('select2').$container.find('.select2-selection').removeClass("errorClass");
                            if (that.ui.entityInputData.find('.errorClass').length === 0) {
                                that.modal.$el.find('button.ok').prop("disabled", false);
                            }
                        } else {
                            $(this).removeClass('errorClass');
                            if (that.ui.entityInputData.find('.errorClass').length === 0) {
                                that.modal.$el.find('button.ok').prop("disabled", false);
                            }
                        }
                    } else {
                        if ($(this).data('select2')) {
                            $(this).data('select2').$container.find('.select2-selection').addClass("errorClass");
                            that.modal.$el.find('button.ok').prop("disabled", true);
                        } else {
                            $(this).addClass('errorClass');
                            that.modal.$el.find('button.ok').prop("disabled", true);
                        }
                    }
                });
            },
            bindNonRequiredField: function() {
                var that = this;
                this.ui.entityInputData.off('keyup change', 'input.false,select.false').on('keyup change', 'input.false,select.false', function(e) {
                    if (that.modal.$el.find('button.ok').prop('disabled') && that.ui.entityInputData.find('.errorClass').length === 0) {
                        that.modal.$el.find('button.ok').prop("disabled", false);
                    }
                });
            },
            decrementCounter: function(counter) {
                if (this[counter] > 0) {
                    --this[counter];
                }
            },
            fetchCollections: function() {
                if (this.guid) {
                    this.collection.url = UrlLinks.entitiesApiUrl({ guid: this.guid });
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
                        name += Utils.getName(val.get("entity"));
                        that.entityData = val;
                    });
                    this.ui.assetName.html(name);
                    var referredEntities = this.entityData.get('referredEntities');
                    var attributes = this.entityData.get('entity').attributes;
                    _.map(_.keys(attributes), function(key) {
                        if (_.isObject(attributes[key])) {
                            var attrObj = attributes[key];
                            if (_.isObject(attrObj) && !_.isArray(attrObj)) {
                                attrObj = [attrObj];
                            }
                            _.each(attrObj, function(obj) {
                                if (obj.guid && !referredEntities[obj.guid]) {
                                    ++that.asyncReferEntityCounter;
                                    that.collection.url = UrlLinks.entitiesApiUrl({ guid: obj.guid });
                                    that.collection.fetch({
                                        success: function(data, response) {
                                            referredEntities[obj.guid] = response.entity;
                                        },
                                        complete: function() {
                                            that.decrementCounter('asyncReferEntityCounter');
                                            if (that.asyncReferEntityCounter === 0) {
                                                that.onEntityChange(null, that.entityData);
                                            }
                                        },
                                        silent: true
                                    });
                                }
                            });
                        }
                    });
                    if (this.asyncReferEntityCounter === 0) {
                        this.onEntityChange(null, this.entityData);
                    }
                } else {
                    var str = '<option disabled="disabled" selected>--Select entity-type--</option>';
                    this.entityDefCollection.fullCollection.each(function(val) {
                        var name = Utils.getName(val.toJSON());
                        if (Globals.entityTypeConfList) {
                            if (_.isEmptyArray(Globals.entityTypeConfList)) {
                                str += '<option>' + name + '</option>';
                            } else {
                                if (_.contains(Globals.entityTypeConfList, val.get("name"))) {
                                    str += '<option>' + name + '</option>';
                                }
                            }
                        }
                    });
                    this.ui.entityList.html(str);
                    this.ui.entityList.select2({});
                    this.hideLoader();
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
                var that = this,
                    typeName = value && value.get('entity') ? value.get('entity').typeName : null;
                if (!this.guid) {
                    this.showLoader();
                }
                this.ui.entityInputData.empty();
                if (typeName) {
                    this.collection.url = UrlLinks.entitiesDefApiUrl(typeName);
                } else if (e) {
                    this.collection.url = UrlLinks.entitiesDefApiUrl(e.target.value);
                    this.collection.modelAttrName = 'attributeDefs';
                }
                this.collection.fetch({
                    success: function(model, data) {
                        that.supuertypeFlag = 0;
                        that.subAttributeData(data)
                    },
                    complete: function() {
                        //that.initilizeElements();
                    },
                    silent: true
                });
            },
            subAttributeData: function(data) {
                var that = this,
                    attributeInput = "",
                    alloptional = false,
                    attributeDefs = Utils.getNestedSuperTypeObj({ data: data, collection: this.entityDefCollection });
                _.each(_.sortBy(_.keys(attributeDefs)), function(key) {
                    if (attributeDefs[key].length) {
                        attributeInput = "";
                        _.each(_.sortBy(attributeDefs[key], 'name'), function(value) {
                            if (value.isOptional == true) {
                                alloptional = true;
                            }
                            attributeInput += that.getContainer(value);
                        });
                        if (attributeInput !== "") {
                            entityTitle = that.getFieldSet(key, alloptional, attributeInput);
                            that.ui.entityInputData.append(entityTitle);
                        }
                    }
                });
                if (this.required) {
                    this.ui.entityInputData.find('fieldset div.true').hide()
                    this.ui.entityInputData.find('div.true').hide();
                }
                if (!('placeholder' in HTMLInputElement.prototype)) {
                    this.ui.entityInputData.find("input,select,textarea").placeholder();
                }
                that.initilizeElements();
            },
            initilizeElements: function() {
                var that = this;
                this.$('input[data-type="date"]').each(function() {
                    if (!$(this).data('daterangepicker')) {
                        var dateObj = { "singleDatePicker": true, "showDropdowns": true };
                        if (that.guid) {
                            dateObj["startDate"] = this.value
                        }
                        $(this).daterangepicker(dateObj);
                    }
                });
                this.initializeValidation();
                if (this.ui.entityInputData.find('fieldset').length > 0 && this.ui.entityInputData.find('select.true,input.true').length === 0) {
                    this.requiredAllToggle(this.ui.entityInputData.find('select.true,input.true').length === 0);
                    if (!this.guid) {
                        // For create entity bind keyup for non-required field when all elements are optional
                        this.bindNonRequiredField();
                    }
                    this.ui.toggleRequired.prop('checked', true);
                } else {
                    this.ui.entityInputData.find('fieldset').each(function() {
                        // if checkbox is alredy selected then dont hide
                        if (!$(this).find('div').hasClass('false') && !that.ui.toggleRequired.is(":checked")) {
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
                this.addJsonSearchData();
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

                this.$('input[data-type="date"]').on('hide.daterangepicker keydown', function(event) {
                    if (event.type) {
                        if (event.type == 'hide') {
                            this.blur();
                        } else if (event.type == 'keydown') {
                            return false;
                        }
                    }
                });
            },
            getContainer: function(value) {
                var entityLabel = this.capitalize(value.name);
                return '<div class="row form-group ' + value.isOptional + '"><span class="col-sm-3">' +
                    '<label><span class="' + (value.isOptional ? 'true' : 'false required') + '">' + entityLabel + '</span><span class="center-block ellipsis text-gray" title="Data Type : ' + value.typeName + '">' + '(' + Utils.escapeHtml(value.typeName) + ')' + '</span></label></span>' +
                    '<span class="col-sm-9">' + (this.getElement(value)) +
                    '</input></span></div>';
            },
            getFieldSet: function(name, alloptional, attributeInput) {
                return '<fieldset class="form-group fieldset-child-pd ' + (alloptional ? "alloptional" : "") + '"><legend class="legend-sm">' + name + '</legend>' + attributeInput + '</fieldset>';
            },
            getSelect: function(value, entityValue) {
                if (value.typeName === "boolean") {
                    return '<select class="form-control row-margin-bottom ' + (value.isOptional === true ? "false" : "true") + '" data-type="' + value.typeName + '" data-key="' + value.name + '" data-id="entityInput">' +
                        '<option value="">--Select true or false--</option><option value="true">true</option>' +
                        '<option value="false">false</option></select>';
                } else {
                    var splitTypeName = value.typeName.split("<");
                    if (splitTypeName.length > 1) {
                        splitTypeName = splitTypeName[1].split(">")[0];
                    } else {
                        splitTypeName = value.typeName;
                    }
                    return '<select class="form-control row-margin-bottom entityInputBox ' + (value.isOptional === true ? "false" : "true") + '" data-type="' + value.typeName +
                        '" data-key="' + value.name + '" data-id="entitySelectData" data-queryData="' + splitTypeName + '">' + (this.guid ? entityValue : "") + '</select>';
                }

            },
            getTextArea: function(value, entityValue, structType) {
                var setValue = entityValue
                try {
                    if (structType && entityValue && entityValue.length) {
                        var parseValue = JSON.parse(entityValue);
                        if (_.isObject(parseValue) && !_.isArray(parseValue) && parseValue.attributes) {
                            setValue = JSON.stringify(parseValue.attributes);
                        }
                    }
                } catch (err) {}

                return '<textarea class="form-control entityInputBox ' + (value.isOptional === true ? "false" : "true") + '"' +
                    ' data-type="' + value.typeName + '"' +
                    ' data-key="' + value.name + '"' +
                    ' placeholder="' + value.name + '"' +
                    ' data-id="entityInput">' + setValue + '</textarea>';

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
                        if (value.typeName === "date") {
                            if (dataValue) {
                                entityValue = moment(dataValue).format("MM/DD/YYYY");
                            } else {
                                entityValue = moment().format("MM/DD/YYYY");
                            }
                        }
                    }
                }
                if ((typeName && this.entityDefCollection.fullCollection.find({ name: typeName })) || typeName === "boolean" || typeName.indexOf("array") > -1) {
                    return this.getSelect(value, entityValue);
                } else if (typeName.indexOf("map") > -1) {
                    return this.getTextArea(value, entityValue);
                } else {
                    var typeNameCategory = this.typeHeaders.fullCollection.findWhere({ name: typeName });
                    if (typeNameCategory && typeNameCategory.get('category') === 'STRUCT') {
                        return this.getTextArea(value, entityValue, true);
                    } else {
                        return this.getInput(value, entityValue);
                    }
                }
            },
            okButton: function() {
                var that = this;
                this.showLoader();
                this.parentEntity = this.ui.entityList.val();
                var entity = {};
                var referredEntities = {};
                var extractValue = function(value, typeName) {
                    if (!value) {
                        return value;
                    }
                    if (_.isArray(value)) {
                        var parseData = [];
                        _.map(value, function(val) {
                            parseData.push({ 'guid': val, 'typeName': typeName });
                        });
                    } else {
                        var parseData = { 'guid': value, 'typeName': typeName };
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
                                    $(this).data('select2').$container.find('.select2-selection').addClass("errorClass")
                                } else {
                                    $(this).addClass('errorClass');
                                }
                                that.hideLoader();
                                throw new Error("Please fill the required fields");
                                return;
                            }
                        }
                        var dataTypeEnitity = $(this).data('type'),
                            datakeyEntity = $(this).data('key'),
                            typeName = $(this).data('querydata'),
                            typeNameCategory = that.typeHeaders.fullCollection.findWhere({ name: dataTypeEnitity });

                        // Extract Data
                        if (dataTypeEnitity && datakeyEntity) {
                            if (that.entityDefCollection.fullCollection.find({ name: dataTypeEnitity })) {
                                entity[datakeyEntity] = extractValue(value, typeName);
                            } else if (dataTypeEnitity === 'date' || dataTypeEnitity === 'time') {
                                entity[datakeyEntity] = Date.parse(value);
                            } else if (dataTypeEnitity.indexOf("map") > -1 || (typeNameCategory && typeNameCategory.get('category') === 'STRUCT')) {
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
                                entity[datakeyEntity] = extractValue(value, typeName);
                            } else {
                                if (_.isString(value)) {
                                    if (value.length) {
                                        entity[datakeyEntity] = value;
                                    } else {
                                        entity[datakeyEntity] = null;
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
                                if (model.mutatedEntities && model.mutatedEntities.CREATE && _.isArray(model.mutatedEntities.CREATE) && model.mutatedEntities.CREATE[0] && model.mutatedEntities.CREATE[0].guid) {
                                    Utils.setUrl({
                                        url: '#!/detailPage/' + (model.mutatedEntities.CREATE[0].guid),
                                        mergeBrowserUrl: false,
                                        trigger: true
                                    });
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
            showLoader: function() {
                this.$('.entityLoader').show();
                this.$('.entityInputData').hide();
            },
            hideLoader: function() {
                this.$('.entityLoader').hide();
                this.$('.entityInputData').show();
                // To enable scroll after selecting value from select2.
                this.ui.entityList.select2('open');
                this.ui.entityList.select2('close');
            },
            addJsonSearchData: function() {
                var that = this;
                this.$('select[data-id="entitySelectData"]').each(function(value, key) {
                    var $this = $(this),
                        keyData = $(this).data("key"),
                        typeData = $(this).data("type"),
                        queryData = $(this).data("querydata"),
                        skip = $(this).data('skip'),
                        placeholderName = "Select a " + typeData + " from the dropdown list";

                    $this.attr("multiple", ($this.data('type').indexOf("array") === -1 ? false : true));

                    // Select Value.
                    if (that.guid) {
                        var dataValue = that.entityData.get("entity").attributes[keyData],
                            entities = that.entityData.get("entity").attributes,
                            referredEntities = that.entityData.get("referredEntities"),
                            selectedValue = [],
                            select2Options = [];

                        if (dataValue) {
                            if (_.isObject(dataValue) && !_.isArray(dataValue)) {
                                dataValue = [dataValue];
                            }
                            _.each(dataValue, function(obj) {
                                if (_.isObject(obj) && obj.guid && referredEntities[obj.guid]) {
                                    var refEntiyFound = referredEntities[obj.guid];
                                    refEntiyFound['id'] = refEntiyFound.guid;
                                    if (!Enums.entityStateReadOnly[refEntiyFound.status]) {
                                        select2Options.push(refEntiyFound);
                                        selectedValue.push(refEntiyFound.guid);
                                    }
                                }
                            });
                        }

                        // Array of string.
                        if (selectedValue.length === 0 && dataValue && dataValue.length && ($this.data('querydata') === "string")) {
                            var str = "";
                            _.each(dataValue, function(obj) {
                                if (_.isString(obj)) {
                                    selectedValue.push(obj);
                                    str += '<option>' + _.escape(obj) + '</option>';
                                }
                            });
                            $this.html(str);
                        }

                    } else {
                        $this.val([]);
                    }
                    var select2Option = {
                        placeholder: placeholderName,
                        allowClear: true,
                        tags: ($this.data('querydata') == "string" ? true : false)
                    }
                    var getTypeAheadData = function(data, params) {
                        var dataList = data.entities,
                            foundOptions = [];
                        _.each(dataList, function(obj) {
                            if (obj) {
                                if (obj.guid) {
                                    obj['id'] = obj.guid;
                                }
                                foundOptions.push(obj);
                            }
                        });
                        return foundOptions;
                    }
                    if ($this.data('querydata') !== "string") {
                        _.extend(select2Option, {
                            ajax: {
                                url: UrlLinks.searchApiUrl('attribute'),
                                dataType: 'json',
                                delay: 250,
                                data: function(params) {
                                    return {
                                        attrValuePrefix: params.term, // search term
                                        typeName: queryData,
                                        limit: 10,
                                        offset: 0
                                    };
                                },
                                processResults: function(data, params) {
                                    return {
                                        results: getTypeAheadData(data, params)
                                    };
                                },
                                cache: true
                            },
                            templateResult: function(option) {
                                var name = Utils.getName(option, 'qualifiedName');
                                return name === "-" ? option.text : name;
                            },
                            templateSelection: function(option) {
                                var name = Utils.getName(option, 'qualifiedName');
                                return name === "-" ? option.text : name;
                            },
                            escapeMarkup: function(markup) {
                                return markup;
                            },
                            data: select2Options,
                            minimumInputLength: 1
                        });
                    }
                    $this.select2(select2Option);
                    if (selectedValue) {
                        $this.val(selectedValue).trigger("change");
                    }

                });
                if (this.guid) {
                    this.bindRequiredField();
                    this.bindNonRequiredField();
                }
                this.hideLoader();
            }
        });
    return CreateEntityLayoutView;
});