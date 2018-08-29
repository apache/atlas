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

define(['require', 'utils/Utils', 'modules/Modal', 'utils/Messages', 'utils/Enums', 'moment'], function(require, Utils, Modal, Messages, Enums, moment) {
    'use strict';

    var CommonViewFunction = {};
    CommonViewFunction.deleteTagModel = function(options) {
        var modal = new Modal({
            title: options.titleMessage,
            okText: options.buttonText,
            htmlContent: options.msg,
            cancelText: "Cancel",
            allowCancel: true,
            okCloses: true,
            showFooter: true,
        }).open();
        return modal;
    };
    CommonViewFunction.deleteTag = function(options) {
        require(['models/VTag'], function(VTag) {
            var tagModel = new VTag();
            if (options && options.guid && options.tagName) {
                if (options.showLoader) {
                    options.showLoader();
                }
                tagModel.deleteAssociation(options.guid, options.tagName, {
                    skipDefaultError: true,
                    success: function(data) {
                        var msg = "Tag " + name.name + Messages.removeSuccessMessage;
                        if (options.tagOrTerm === "term") {
                            msg = "Term " + options.tagName + Messages.removeSuccessMessage;
                        } else if (options.tagOrTerm === "tag") {
                            msg = "Tag " + options.tagName + Messages.removeSuccessMessage;
                        }
                        Utils.notifySuccess({
                            content: msg
                        });
                        if (options.callback) {
                            options.callback();
                        }
                        if (options.collection) {
                            options.collection.fetch({ reset: true });
                        }

                    },
                    cust_error: function(model, response) {
                        var message = options.tagName + Messages.deleteErrorMessage;
                        if (response && response.responseJSON) {
                            message = response.responseJSON.errorMessage;
                        }
                        if (options.hideLoader) {
                            options.hideLoader();
                        }
                        Utils.notifyError({
                            content: message
                        });
                    }
                });
            }
        });
    };
    CommonViewFunction.propertyTable = function(options) {
        var scope = options.scope,
            valueObject = options.valueObject,
            extractJSON = options.extractJSON,
            isTable = _.isUndefined(options.isTable) ? true : options.isTable,
            attributeDefs = options.attributeDefs;

        var table = "",
            fetchInputOutputValue = function(id) {
                var that = this;
                scope.entityModel.getEntityHeader(id, {
                    success: function(serverData) {
                        var value = "",
                            deleteButton = "",
                            data = serverData;
                        value = Utils.getName(data);
                        var id = "";
                        if (data.guid) {
                            if (Enums.entityStateReadOnly[data.status]) {
                                deleteButton += '<button title="Deleted" class="btn btn-action btn-md deleteBtn"><i class="fa fa-trash"></i></button>';
                            }
                            id = data.guid;
                        }
                        if (value.length > 0) {
                            scope.$('td div[data-id="' + id + '"]').html('<a href="#!/detailPage/' + id + '">' + value + '</a>');
                        } else {
                            scope.$('td div[data-id="' + id + '"]').html('<a href="#!/detailPage/' + id + '">' + _.escape(id) + '</a>');
                        }
                        if (deleteButton.length) {
                            scope.$('td div[data-id="' + id + '"]').addClass('block readOnlyLink');
                            scope.$('td div[data-id="' + id + '"]').append(deleteButton);
                        }
                    },
                    complete: function() {}
                });
            },
            extractObject = function(keyValue) {
                var valueOfArray = [];
                if (!_.isArray(keyValue) && _.isObject(keyValue)) {
                    keyValue = [keyValue];
                }
                var subLink = "";
                for (var i = 0; i < keyValue.length; i++) {
                    var inputOutputField = keyValue[i],
                        id = inputOutputField.guid || (_.isObject(inputOutputField.id) ? inputOutputField.id.id : inputOutputField.id),
                        tempLink = "",
                        status = inputOutputField.status || (_.isObject(inputOutputField.id) ? inputOutputField.id.state : inputOutputField.state),
                        readOnly = Enums.entityStateReadOnly[status];
                    if (!inputOutputField.attributes && inputOutputField.values) {
                        inputOutputField['attributes'] = inputOutputField.values;
                    }
                    if (_.isString(inputOutputField) || _.isBoolean(inputOutputField) || _.isNumber(inputOutputField)) {
                        var tempVarfor$check = inputOutputField.toString();
                        if (tempVarfor$check.indexOf("$") == -1) {
                            valueOfArray.push('<span class="json-string">' + _.escape(inputOutputField) + '</span>');
                        }
                    } else if (_.isObject(inputOutputField) && !id) {
                        var attributesList = inputOutputField;
                        if (scope.typeHeaders && inputOutputField.typeName) {
                            var typeNameCategory = scope.typeHeaders.fullCollection.findWhere({ name: inputOutputField.typeName });
                            if (attributesList.attributes && typeNameCategory && typeNameCategory.get('category') === 'STRUCT') {
                                attributesList = attributesList.attributes;
                            }
                        }

                        if (extractJSON && extractJSON.extractKey) {
                            var newAttributesList = {};
                            _.each(attributesList, function(objValue, objKey) {
                                var value = _.isObject(objValue) ? objValue : _.escape(objValue),
                                    tempVarfor$check = objKey.toString();
                                if (tempVarfor$check.indexOf("$") == -1) {
                                    if (_.isObject(extractJSON.extractKey)) {
                                        _.each(extractJSON.extractKey, function(extractKey) {
                                            if (objKey === extractKey) {
                                                newAttributesList[_.escape(objKey)] = value;
                                            }
                                        });
                                    } else if (_.isString(extractJSON.extractKey) && extractJSON.extractKey === objKey) {
                                        newAttributesList[_.escape(objKey)] = value;
                                    }
                                }
                            });
                            valueOfArray.push(Utils.JSONPrettyPrint(newAttributesList));
                        } else {
                            valueOfArray.push(Utils.JSONPrettyPrint(attributesList));
                        }

                    }
                    if (id && inputOutputField) {
                        var name = Utils.getName(inputOutputField);
                        if ((name === "-" || name === id) && !inputOutputField.attributes) {
                            var fetch = true;
                            var fetchId = (_.isObject(id) ? id.id : id);
                            fetchInputOutputValue(fetchId);
                            tempLink += '<div data-id="' + fetchId + '"><div class="value-loader"></div></div>';
                        } else {
                            tempLink += '<a href="#!/detailPage/' + id + '">' + name + '</a>'
                        }
                    }
                    if (readOnly) {
                        if (!fetch) {
                            tempLink += '<button title="Deleted" class="btn btn-action btn-md deleteBtn"><i class="fa fa-trash"></i></button>';
                            subLink += '<div class="block readOnlyLink">' + tempLink + '</div>';
                        } else {
                            fetch = false;
                            subLink += tempLink;
                        }

                    } else {
                        if (tempLink.search('href') != -1) {
                            subLink += '<div>' + tempLink + '</div>'
                        } else if (tempLink.length) {
                            subLink += tempLink
                        }
                    }
                }
                if (valueOfArray.length) {
                    subLink = valueOfArray.join(', ');
                }
                return subLink;
            }
        _.sortBy(_.keys(valueObject)).map(function(key) {
            key = _.escape(key);
            if (key == "profileData") {
                return;
            }
            var keyValue = valueObject[key];
            var defEntity = _.find(attributeDefs, { name: key });
            if (defEntity && defEntity.typeName) {
                var defEntityType = defEntity.typeName.toLocaleLowerCase();
                if (defEntityType === 'date' || defEntityType === 'time') {
                    keyValue = new Date(keyValue);
                } else if (_.isObject(keyValue)) {
                    keyValue = extractObject(keyValue);
                }
            } else {
                if (_.isObject(keyValue)) {
                    keyValue = extractObject(keyValue)
                }
            }
            var val = "";
            if (_.isObject(valueObject[key])) {
                val = keyValue
            } else if (Utils.isUrl(keyValue)) {
                val = '<a target="_blank" class="blue-link" href="' + keyValue + '">' + keyValue + '</a>';
            } else if (key === 'guid' || key === "__guid") {
                val = '<a title="' + key + '" href="#!/detailPage/' + keyValue + '">' + keyValue + '</a>';
            } else if (key.toLocaleLowerCase().indexOf("time") !== -1 || key.toLocaleLowerCase().indexOf("date") !== -1) {
                val = new Date(keyValue);
            } else {
                val = _.escape(keyValue);
            }
            if (isTable) {
                var htmlTag = '<div class="scroll-y">' + val + '</div>';
                if (_.isObject(valueObject[key])) {
                    var matchedLinkString = val.match(/href|value-loader\w*/g),
                        matchedJson = val.match(/json-value|json-string\w*/g),
                        isMatchLinkStringIsSingle = matchedLinkString && matchedLinkString.length == 1,
                        isMatchJSONStringIsSingle = matchedJson && matchedJson.length == 1,
                        expandCollapseButton = "";
                    if ((matchedJson && !isMatchJSONStringIsSingle) || (matchedLinkString && !isMatchLinkStringIsSingle)) {
                        var expandCollapseButton = '<button class="expand-collapse-button"><i class="fa"></i></button>'
                    }
                    var htmlTag = '<pre class="shrink code-block ' + (isMatchJSONStringIsSingle ? 'fixed-height' : '') + '">' + expandCollapseButton + '<code>' + val + '</code></pre>';
                }
                table += '<tr><td>' + _.escape(key) + '</td><td>' + htmlTag + '</td></tr>';
            } else {
                table += '<div>' + val + '</div>';
            }

        });
        return table;
    }
    CommonViewFunction.breadcrumbUrlMaker = function(url) {
        if (url) {
            var urlList = [];
            var splitURL = url.split("api/atlas/v1/taxonomies/");
            if (splitURL.length > 1) {
                var splitUrlWithoutTerm = splitURL[1].split("/terms/");
                if (splitUrlWithoutTerm.length == 1) {
                    splitUrlWithoutTerm = splitUrlWithoutTerm[0].split("/");
                }
            } else {
                var splitUrlWithoutTerm = splitURL[0].split("/terms/");
                if (splitUrlWithoutTerm.length == 1) {
                    splitUrlWithoutTerm = splitUrlWithoutTerm[0].split("/");
                }
            }

            var href = "";
            for (var i in splitUrlWithoutTerm) {
                if (i == 0) {
                    href = splitUrlWithoutTerm[i];
                    urlList.push({
                        value: _.escape(splitUrlWithoutTerm[i]),
                        href: href
                    });
                } else {
                    href += "/terms/" + splitUrlWithoutTerm[i];
                    urlList.push({
                        value: _.escape(splitUrlWithoutTerm[i]),
                        href: href
                    });
                };
            }
            return urlList;
        }
    }
    CommonViewFunction.breadcrumbMaker = function(options) {
        var li = "";
        if (options.urlList) {
            _.each(options.urlList, function(object) {
                li += '<li><a class="link" href="#!/taxonomy/detailCatalog/api/atlas/v1/taxonomies/' + object.href + '?load=true">' + _.escape(object.value) + '</a></li>';
            });
        }
        if (options.scope) {
            options.scope.html(li);
            options.scope.asBreadcrumbs("destroy");
            options.scope.asBreadcrumbs({
                namespace: 'breadcrumb',
                overflow: "left",
                responsive: false,
                toggleIconClass: 'fa fa-ellipsis-h',
                dropdown: function(classes) {
                    var dropdownClass = 'dropdown';
                    var dropdownMenuClass = 'dropdown-menu popover popoverTerm bottom arrowPosition';
                    if (this.options.overflow === 'right') {
                        dropdownMenuClass += ' dropdown-menu-right';
                    }

                    return '<li class="' + dropdownClass + ' ' + classes.dropdownClass + '">' +
                        '<a href="javascript:void(0);" class="' + classes.toggleClass + '" data-toggle="dropdown">' +
                        '<i class="' + classes.toggleIconClass + '"></i>' +
                        '</a>' +
                        '<ul class="' + dropdownMenuClass + ' ' + classes.dropdownMenuClass + '">' +
                        '<div class="arrow"></div>' +
                        '</ul>' +
                        '</li>';
                }
            });
        }
    }
    CommonViewFunction.termTableBreadcrumbMaker = function(obj) {
        if (!obj) {
            return "";
        }
        var traits = obj.classificationNames || _.pluck(obj.classifications, 'typeName'),
            url = "",
            deleteHtml = "",
            html = "",
            id = obj.guid,
            terms = [],
            entityName = Utils.getName(obj);
        if (traits) {
            traits.map(function(term) {
                var checkTagOrTerm = Utils.checkTagOrTerm(term);
                if (checkTagOrTerm.term) {
                    terms.push({
                        deleteHtml: '<a href="javascript:void(0)" class="pull-left" title="Remove Term"><i class="fa fa-trash" data-id="tagClick" data-type="term" data-assetname="' + entityName + '" data-name="' + term + '" data-guid="' + obj.guid + '" ></i></a>',
                        url: _.unescape(term).split(".").join("/"),
                        name: term
                    });
                }
            });
        }
        _.each(terms, function(obj, i) {
            var className = "";
            if (i >= 1) {
                className += "showHideDiv hide";
            }
            obj['valueUrl'] = CommonViewFunction.breadcrumbUrlMaker(obj.url);
            html += '<div class="' + className + '" dataterm-name="' + obj.name + '"><div class="liContent"></div>' + obj.deleteHtml + '</div>';
        })
        if (terms.length > 1) {
            html += '<div><a  href="javascript:void(0)" data-id="showMoreLessTerm" class="btn btn-action btn-sm btn-icon-pd"><span>Show More </span><i class="fa fa-angle-right"></i></a></div>'
        }
        if (!Enums.entityStateReadOnly[obj.status]) {
            if (obj.guid) {
                html += '<div><a href="javascript:void(0)" class="btn btn-action btn-sm" data-id="addTerm" data-guid="' + (obj.guid) + '"><i class="fa fa-plus"></i></a></div>'
            } else {
                html += '<div><a href="javascript:void(0)" class="btn btn-action btn-sm" data-id="addTerm"><i class="fa fa-plus"></i></a></div>'
            }
        }
        return {
            html: '<div class="termTableBreadcrumb btn-inline" dataterm-id="' + id + '">' + html + '</div>',
            object: { scopeId: id, value: terms }
        }

    }
    CommonViewFunction.tagForTable = function(obj) {
        var traits = obj.classificationNames || _.pluck(obj.classifications, 'typeName'),
            atags = "",
            addTag = "",
            popTag = "",
            count = 0,
            entityName = Utils.getName(obj);
        if (traits) {
            traits.map(function(tag) {
                var checkTagOrTerm = Utils.checkTagOrTerm(tag);
                if (checkTagOrTerm.tag) {
                    var className = "btn btn-action btn-sm btn-blue btn-icon",
                        tagString = '<a class="' + className + '" data-id="tagClick"><span title="' + tag + '">' + tag + '</span><i class="fa fa-times" data-id="delete"  data-assetname="' + entityName + '"data-name="' + tag + '" data-type="tag" data-guid="' + obj.guid + '" ></i></a>';
                    if (count >= 1) {
                        popTag += tagString;
                    } else {
                        atags += tagString;
                    }
                    ++count;
                }
            });
        }
        if (!Enums.entityStateReadOnly[obj.status]) {
            if (obj.guid) {
                addTag += '<a href="javascript:void(0)" data-id="addTag" class="btn btn-action btn-sm assignTag" data-guid="' + obj.guid + '" ><i class="fa fa-plus"></i></a>';
            } else {
                addTag += '<a href="javascript:void(0)" data-id="addTag" class="btn btn-action btn-sm assignTag"><i style="right:0" class="fa fa-plus"></i></a>';
            }
        }
        if (count > 1) {
            addTag += '<div data-id="showMoreLess" class="btn btn-action btn-sm assignTag"><i class="fa fa-ellipsis-h" aria-hidden="true"></i><div class="popup-tag">' + popTag + '</div></div>'
        }
        return '<div class="tagList btn-inline btn-fixed-width">' + atags + addTag + '</div>';
    }
    CommonViewFunction.generateQueryOfFilter = function(value) {
        var entityFilters = CommonViewFunction.attributeFilter.extractUrl({ "value": value.entityFilters, "formatDate": true }),
            tagFilters = CommonViewFunction.attributeFilter.extractUrl({ "value": value.tagFilters, "formatDate": true }),
            queryArray = [];

        function objToString(filterObj) {
            var generatedQuery = _.map(filterObj.rules, function(obj, key) {
                if (_.has(obj, 'condition')) {
                    return '&nbsp<span class="operator">' + obj.condition + '</span>&nbsp' + '(' + objToString(obj) + ')';
                } else {
                    return '<span class="key">' + _.escape(obj.id) + '</span>&nbsp<span class="operator">' + _.escape(obj.operator) + '</span>&nbsp<span class="value">' + _.escape(obj.value) + "</span>";
                }
            });
            return generatedQuery;
        }
        if (value.type) {
            var typeKeyValue = '<span class="key">Type:</span>&nbsp<span class="value">' + _.escape(value.type) + '</span>';
            if (entityFilters) {
                var conditionForEntity = entityFilters.rules.length == 1 ? '' : 'AND';
                typeKeyValue += '&nbsp<span class="operator">' + conditionForEntity + '</span>&nbsp(<span class="operator">' + entityFilters.condition + '</span>&nbsp(' + objToString(entityFilters) + '))';
            }
            queryArray.push(typeKeyValue)
        }
        if (value.tag) {
            var tagKeyValue = '<span class="key">Tag:</span>&nbsp<span class="value">' + _.escape(value.tag) + '</span>';
            if (tagFilters) {
                var conditionFortag = tagFilters.rules.length == 1 ? '' : 'AND';
                tagKeyValue += '&nbsp<span class="operator">' + conditionFortag + '</span>&nbsp(<span class="operator">' + tagFilters.condition + '</span>&nbsp(' + objToString(tagFilters) + '))';
            }
            queryArray.push(tagKeyValue);
        }
        if (value.query) {
            queryArray.push('<span class="key">Query:</span>&nbsp<span class="value">' + _.trim(_.escape(value.query)) + '</span>&nbsp');
        }
        if (queryArray.length == 1) {
            return queryArray.join();
        } else {
            return "<span>(</span>&nbsp" + queryArray.join('<span>&nbsp)</span>&nbsp<span>AND</span>&nbsp<span>(</span>&nbsp') + "&nbsp<span>)</span>";

        }
    }
    CommonViewFunction.saveTermToAsset = function(options, that) {
        require(['models/VCatalog'], function(Vcatalog) {
            var VCatalog = new Vcatalog();
            var name = options.termName;
            ++that.asyncFetchCounter;
            VCatalog.url = function() {
                return "api/atlas/v1/entities/" + options.guid + "/tags/" + name;
            };
            VCatalog.save(null, {
                success: function(data) {
                    Utils.notifySuccess({
                        content: "Term " + name + Messages.addTermToEntitySuccessMessage
                    });
                    if (options.collection) {
                        options.collection.fetch({ reset: true });
                    }
                },
                complete: function() {
                    --that.asyncFetchCounter
                    if (that.callback && that.asyncFetchCounter === 0) {
                        that.callback(); // It will call to parent of parent Callback i.e callback of searchLayoutView
                    }
                }
            });
        })
    }
    CommonViewFunction.generateObjectForSaveSearchApi = function(options) {
        var obj = {
            name: options.name,
            guid: options.guid
        };
        var value = options.value;
        if (value) {
            _.each(Enums.extractFromUrlForSearch, function(svalue, skey) {
                if (_.isObject(svalue)) {
                    _.each(svalue, function(v, k) {
                        var val = value[k];
                        if (!_.isUndefinedNull(val)) {
                            if (k == "attributes") {
                                val = val.split(',');
                            } else if (_.contains(["tagFilters", "entityFilters"], k)) {
                                val = CommonViewFunction.attributeFilter.generateAPIObj(val);
                            } else if (_.contains(["includeDE", "excludeST", "excludeSC"], k)) {
                                val = val ? false : true;
                            }
                        }
                        if (_.contains(["includeDE", "excludeST", "excludeSC"], k)) {
                            val = _.isUndefinedNull(val) ? true : val;
                        }
                        if (!obj[skey]) {
                            obj[skey] = {};
                        }
                        obj[skey][v] = val;
                    });
                } else {
                    obj[skey] = value[skey];
                }
            });
            return obj;
        }
    }
    CommonViewFunction.generateUrlFromSaveSearchObject = function(options) {
        var value = options.value,
            classificationDefCollection = options.classificationDefCollection,
            entityDefCollection = options.entityDefCollection,
            obj = {};
        if (value) {
            _.each(Enums.extractFromUrlForSearch, function(svalue, skey) {
                if (_.isObject(svalue)) {
                    _.each(svalue, function(v, k) {
                        var val = value[skey][v];
                        if (!_.isUndefinedNull(val)) {
                            if (k == "attributes") {
                                val = val.join(',');
                            } else if (k == "tagFilters") {
                                if (classificationDefCollection) {
                                    var classificationDef = classificationDefCollection.fullCollection.findWhere({ 'name': value[skey].classification })
                                    attributeDefs = Utils.getNestedSuperTypeObj({
                                        collection: classificationDefCollection,
                                        attrMerge: true,
                                        data: classificationDef.toJSON()
                                    });
                                }
                                val = CommonViewFunction.attributeFilter.generateUrl({ "value": val, "attributeDefs": attributeDefs });
                            } else if (k == "entityFilters") {
                                if (entityDefCollection) {
                                    var entityDef = entityDefCollection.fullCollection.findWhere({ 'name': value[skey].typeName }),
                                        attributeDefs = Utils.getNestedSuperTypeObj({
                                            collection: entityDefCollection,
                                            attrMerge: true,
                                            data: entityDef.toJSON()
                                        });
                                }
                                val = CommonViewFunction.attributeFilter.generateUrl({ "value": val, "attributeDefs": attributeDefs });
                            } else if (_.contains(["includeDE", "excludeST", "excludeSC"], k)) {
                                val = val ? false : true;
                            }
                        }
                        obj[k] = val;
                    });
                } else {
                    obj[skey] = value[skey];
                }
            });
            return obj;
        }
    }
    CommonViewFunction.attributeFilter = {
        generateUrl: function(options) {
            var attrQuery = [],
                attrObj = options.value,
                formatedDateToLong = options.formatedDateToLong,
                attributeDefs = options.attributeDefs,
                /* set attributeType for criterion while creating object*/
                spliter = 1;
            attrQuery = conditionalURl(attrObj, spliter);

            function conditionalURl(options, spliter) {
                if (options) {
                    return _.map(options.rules || options.criterion, function(obj, key) {
                        if (_.has(obj, 'condition')) {
                            return obj.condition + '(' + conditionalURl(obj, (spliter + 1)) + ')';
                        }
                        if (attributeDefs) {
                            var attributeDef = _.findWhere(attributeDefs, { 'name': obj.attributeName });
                            if (attributeDef) {
                                obj.attributeValue = obj.attributeValue;
                                obj['attributeType'] = attributeDef.typeName;
                            }
                        }
                        var type = (obj.type || obj.attributeType),
                            //obj.value will come as an object when selected type is Date and operator is isNull or not_null;
                            value = ((_.isString(obj.value) && _.contains(["is_null", "not_null"], obj.operator) && type === 'date') || _.isObject(obj.value) ? "" : _.trim(obj.value || obj.attributeValue)),
                            url = [(obj.id || obj.attributeName), mapApiOperatorToUI(obj.operator), (type === 'date' && formatedDateToLong && value.length ? Date.parse(value) : value)];
                        if (type) {
                            url.push(type);
                        }
                        return url.join("::");
                    }).join('|' + spliter + '|')
                } else {
                    return null;
                }
            }
            if (attrQuery.length) {
                return attrObj.condition + '(' + attrQuery + ')';
            } else {
                return null;
            }

            function mapApiOperatorToUI(oper) {
                if (oper == "eq") {
                    return "=";
                } else if (oper == "neq") {
                    return "!=";
                } else if (oper == "lt") {
                    return "<";
                } else if (oper == "lte") {
                    return "<=";
                } else if (oper == "gt") {
                    return ">";
                } else if (oper == "gte") {
                    return ">=";
                } else if (oper == "startsWith") {
                    return "begins_with";
                } else if (oper == "endsWith") {
                    return "ends_with";
                } else if (oper == "contains") {
                    return "contains";
                } else if (oper == "notNull") {
                    return "not_null";
                } else if (oper == "isNull") {
                    return "is_null";
                }
                return oper;
            }
        },
        extractUrl: function(options) {
            var attrObj = {},
                urlObj = options.value,
                formatDate = options.formatDate,
                spliter = 1,
                apiObj = options.apiObj; //if apiObj then create object for API call else for QueryBuilder.
            if (urlObj && urlObj.length) {
                attrObj = createObject(urlObj);

                function createObject(urlObj) {
                    var finalObj = {};
                    finalObj['condition'] = /^AND\(/.test(urlObj) ? "AND" : "OR";
                    urlObj = finalObj.condition === "AND" ? urlObj.substr(4).slice(0, -1) : urlObj.substr(3).slice(0, -1);
                    finalObj[apiObj ? "criterion" : "rules"] = _.map(urlObj.split('|' + spliter + '|'), function(obj, key) {
                        var isStringNested = obj.split('|' + (spliter + 1) + '|').length > 1,
                            isCondition = /^AND\(/.test(obj) || /^OR\(/.test(obj);
                        if (isStringNested && isCondition) {
                            ++spliter;
                            return createObject(obj);
                        } else if (isCondition) {
                            return createObject(obj);
                        } else {
                            var temp = obj.split("::") || obj.split('|' + spliter + '|'),
                                rule = {};
                            if (apiObj) {
                                rule = { attributeName: temp[0], operator: mapUiOperatorToAPI(temp[1]), attributeValue: _.trim(temp[2]) }
                                rule.attributeValue = rule.type === 'date' && formatDate && rule.attributeValue.length ? moment(parseInt(rule.attributeValue)).format('MM/DD/YYYY h:mm A') : rule.attributeValue;
                            } else {
                                rule = { id: temp[0], operator: temp[1], value: _.trim(temp[2]) }
                                if (temp[3]) {
                                    rule['type'] = temp[3];
                                }
                                rule.value = rule.type === 'date' && formatDate && rule.value.length ? moment(parseInt(rule.value)).format('MM/DD/YYYY h:mm A') : rule.value;
                            }
                            return rule;
                        }
                    });
                    return finalObj;
                }
            } else {
                return null;
            }
            return attrObj;

            function mapUiOperatorToAPI(oper) {
                if (oper == "=") {
                    return "eq";
                } else if (oper == "!=") {
                    return "neq";
                } else if (oper == "<") {
                    return "lt";
                } else if (oper == "<=") {
                    return "lte";
                } else if (oper == ">") {
                    return "gt";
                } else if (oper == ">=") {
                    return "gte";
                } else if (oper == "begins_with") {
                    return "startsWith";
                } else if (oper == "ends_with") {
                    return "endsWith";
                } else if (oper == "contains") {
                    return "contains";
                } else if (oper == "not_null") {
                    return "notNull";
                } else if (oper == "is_null") {
                    return "isNull";
                }
                return oper;
            }
        },
        generateAPIObj: function(url) {
            if (url && url.length) {
                return this.extractUrl({ "value": url, "apiObj": true });
            } else {
                return null;
            }
        }
    }
    CommonViewFunction.addRestCsrfCustomHeader = function(xhr, settings) {
        //    if (settings.url == null || !settings.url.startsWith('/webhdfs/')) {
        if (settings.url == null) {
            return;
        }
        var method = settings.type;
        if (CommonViewFunction.restCsrfCustomHeader != null && !CommonViewFunction.restCsrfMethodsToIgnore[method]) {
            // The value of the header is unimportant.  Only its presence matters.
            xhr.setRequestHeader(CommonViewFunction.restCsrfCustomHeader, '""');
        }
    }
    CommonViewFunction.restCsrfCustomHeader = null;
    CommonViewFunction.restCsrfMethodsToIgnore = null;
    CommonViewFunction.userDataFetch = function(options) {
        var csrfEnabled = false,
            header = null,
            methods = [];

        function getTrimmedStringArrayValue(string) {
            var str = string,
                array = [];
            if (str) {
                var splitStr = str.split(',');
                for (var i = 0; i < splitStr.length; i++) {
                    array.push(splitStr[i].trim());
                }
            }
            return array;
        }
        if (options.url) {
            $.ajax({
                url: options.url,
                success: function(response) {
                    if (response) {
                        if (response['atlas.rest-csrf.enabled']) {
                            var str = "" + response['atlas.rest-csrf.enabled'];
                            csrfEnabled = (str.toLowerCase() == 'true');
                        }
                        if (response['atlas.rest-csrf.custom-header']) {
                            header = response['atlas.rest-csrf.custom-header'].trim();
                        }
                        if (response['atlas.rest-csrf.methods-to-ignore']) {
                            methods = getTrimmedStringArrayValue(response['atlas.rest-csrf.methods-to-ignore']);
                        }
                        if (csrfEnabled) {
                            CommonViewFunction.restCsrfCustomHeader = header;
                            CommonViewFunction.restCsrfMethodsToIgnore = {};
                            methods.map(function(method) { CommonViewFunction.restCsrfMethodsToIgnore[method] = true; });
                            Backbone.$.ajaxSetup({
                                beforeSend: CommonViewFunction.addRestCsrfCustomHeader
                            });
                        }
                    }
                },
                complete: function(response) {
                    if (options.callback) {
                        options.callback(response.responseJSON);
                    }
                }
            });
        }
    }
    return CommonViewFunction;
});