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

define(['require', 'utils/Utils', 'modules/Modal', 'utils/Messages'], function(require, Utils, Modal, Messages) {
    'use strict';

    var CommonViewFunction = {};
    CommonViewFunction.deleteTagModel = function(tagName, AssignTerm) {
        var msg = "",
            titleMessage = "",
            deleteText = "";
        if (tagName && AssignTerm != "assignTerm") {
            var tagOrTerm = Utils.checkTagOrTerm(tagName);
            if (tagOrTerm.term) {
                msg = "<div class='ellipsis'>Delete: " + "<b>" + tagName + "?</b></div>" +
                    "<p class='termNote'>Assets map to this term will be unclassified</p>";
                titleMessage = Messages.deleteTerm;
                deleteText = "Delete";
            } else {
                msg = "<div class='ellipsis'>Delete: " + "<b>" + tagName + "?</b></div>";
                var titleMessage = Messages.deleteTag;
                deleteText = "Delete";
            }
        }
        if (AssignTerm == "assignTerm") {
            msg = "<div class='ellipsis'>Remove: " + "<b>" + tagName + "?</b></div>" +
                "<p class='termNote'>Assets map to this term will be unclassified</p>";
            titleMessage = Messages.RemoveTerm;
            deleteText = "Remove";
        }
        var modal = new Modal({
            title: titleMessage,
            okText: deleteText,
            htmlContent: msg,
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

                tagModel.deleteTag(options.guid, options.tagName, {
                    beforeSend: function() {},
                    success: function(data) {
                        var msg = "Tag " + name.name + Messages.deleteSuccessMessage;
                        if (data.traitName) {
                            var tagOrTerm = Utils.checkTagOrTerm(data.traitName);
                            if (tagOrTerm.term) {
                                msg = "Term " + data.traitName + Messages.deleteSuccessMessage;
                            } else {
                                msg = "Tag " + data.traitName + Messages.deleteSuccessMessage;
                            }
                        } else {
                            var tagOrTerm = Utils.checkTagOrTerm(options.tagName);
                            if (tagOrTerm.term) {
                                msg = "Term " + data.traitName + Messages.deleteSuccessMessage;
                            } else {
                                msg = "Tag " + data.traitName + Messages.deleteSuccessMessage;
                            }
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
                    error: function(error, data, status) {
                        var message = options.tagName + Messages.deleteErrorMessage;
                        if (data.error) {
                            message = data.error;
                        }
                        Utils.notifyError({
                            content: message
                        });
                    },
                    complete: function() {}
                });
            }
        });
    };
    CommonViewFunction.propertyTable = function(valueObject, scope) {
        var table = "",
            fetchInputOutputValue = function(id) {
                var that = this;
                scope.model.getEntity(id, {
                    beforeSend: function() {},
                    success: function(data) {
                        var value = "";
                        if (data.definition.values.name) {
                            value = data.definition.values.name;
                        }
                        var id = "";
                        if (data.definition.id) {
                            if (_.isObject(data.definition.id) && data.definition.id.id) {
                                id = data.definition.id.id;
                            } else {
                                id = data.definition.id;
                            }
                        }
                        if (value.length > 1) {
                            scope.$('td div[data-id="' + id + '"]').html('<a href="#!/detailPage/' + id + '">' + value + '</a>');
                        } else {
                            scope.$('td div[data-id="' + id + '"]').html('<a href="#!/detailPage/' + id + '">' + id + '</a>');
                        }

                    },
                    error: function(error, data, status) {},
                    complete: function() {}
                });
            }
        _.keys(valueObject).map(function(key) {
            var keyValue = valueObject[key];
            if (_.isArray(keyValue)) {
                var subLink = "";
                for (var i = 0; i < keyValue.length; i++) {
                    var inputOutputField = keyValue[i],
                        id = undefined;
                    if (_.isObject(inputOutputField.id)) {
                        id = inputOutputField.id.id;
                    } else {
                        id = inputOutputField.id;
                    }
                    if (id) {
                        if (inputOutputField.values) {
                            if (inputOutputField.values.name) {
                                subLink += '<div><a href="#!/detailPage/' + id + '">' + inputOutputField.values.name + '</a><div>'
                            } else {
                                subLink += '<a href="#!/detailPage/' + id + '">' + id + '</a>'
                            }
                        } else {
                            fetchInputOutputValue(id);
                            subLink += '<div data-id="' + id + '"></div>';
                        }

                    } else {
                        subLink += '<div></div>';
                    }
                }
                table += '<tr><td>' + key + '</td><td>' + subLink + '</td></tr>';
            } else if (_.isObject(keyValue)) {
                var id = undefined;
                if (_.isObject(keyValue.id)) {
                    id = keyValue.id.id;
                } else {
                    id = keyValue.id;
                }
                if (id) {
                    if (keyValue.values) {
                        if (keyValue.values.name) {
                            table += '<tr><td>' + key + '</td><td><div><a href="#!/detailPage/' + id + '">' + keyValue.values.name + '</a><div></td></tr>';
                        } else {
                            table += '<tr><td>' + key + '</td><td><div><a href="#!/detailPage/' + id + '">' + id + '</a><div></td></tr>';
                        }
                    } else {
                        fetchInputOutputValue(id);
                        table += '<tr><td>' + key + '</td><td><div data-id="' + id + '"></div></td></tr>';
                    }
                } else {
                    var stringArr = [];
                    _.each(keyValue, function(val, key) {
                        var value = "";
                        if (_.isObject(val)) {
                            value = JSON.stringify(val);
                        } else {
                            value = val;
                        }
                        var attrName = "<span>" + key + " : " + value + "</span>";
                        stringArr.push(attrName);
                    });
                    var jointValues = stringArr.join(", ");
                    if (jointValues.length) {
                        table += '<tr><td>' + key + '</td><td><div>' + jointValues + '</div></td></tr>';
                    } else {
                        table += '<tr><td>' + key + '</td><td></td></tr>';
                    }
                }
            } else {
                if (key == "createTime" || key == "lastAccessTime" || key == "retention") {
                    table += '<tr><td>' + key + '</td><td>' + new Date(valueObject[key]) + '</td></tr>';
                } else {
                    table += '<tr><td>' + key + '</td><td>' + valueObject[key] + '</td></tr>';
                }

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
                        value: splitUrlWithoutTerm[i],
                        href: href
                    });
                } else {
                    href += "/terms/" + splitUrlWithoutTerm[i];
                    urlList.push({
                        value: splitUrlWithoutTerm[i],
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
                li += '<li><a href="javascript:void(0)" class="link" data-href="/api/atlas/v1/taxonomies/' + object.href + '">' + object.value + '</a></li>';
            });
        }
        if (options.scope) {
            options.scope.html(li);
            options.scope.asBreadcrumbs("destroy");
            options.scope.asBreadcrumbs({
                namespace: 'breadcrumb',
                overflow: "left",
                dropicon: "fa fa-ellipsis-h",
                responsive: false,
                dropdown: function() {
                    return '<div class=\"dropdown\">' +
                        '<a href=\"javascript:void(0);\" class=\"' + this.namespace + '-toggle\" data-toggle=\"dropdown\"><i class=\"' + this.dropicon + '\"</i></a>' +
                        '<ul class=\"' + this.namespace + '-menu dropdown-menu popover bottom arrowPosition \" ><div class="arrow"></div></ul>' +
                        '</div>';
                },
                dropdownContent: function(a) {
                    return '<li><a class="link" href="javascript:void(0)" data-href="' + a.find('a').data('href') + '" class="dropdown-item">' + a.text() + "</a></li>";
                }
            });
        }
        options.scope.find('li a.link').click(function() {
            Utils.setUrl({
                url: "#!/taxonomy/detailCatalog" + $(this).data('href') + "?load=true",
                mergeBrowserUrl: false,
                trigger: true,
                updateTabState: function() {
                    return { taxonomyUrl: this.url, stateChanged: false };
                }
            });
        });
    }
    CommonViewFunction.termTableBreadcrumbMaker = function(model) {
        var traits = model.get('$traits$'),
            url = "",
            deleteHtml = "",
            html = "",
            id = model.get('$id$').id,
            terms = [];
        _.keys(traits).map(function(key) {
            var tagName = Utils.checkTagOrTerm(traits[key].$typeName$);
            if (tagName.term) {
                terms.push({
                    deleteHtml: '<a class="pull-left" title="Delete Term"><i class="fa fa-trash" data-id="tagClick" data-name="' + traits[key].$typeName$ + '" data-guid="' + model.get('$id$').id + '" ></i></a>',
                    url: traits[key].$typeName$.split(".").join("/"),
                    name: tagName.fullName
                });
            }
        });
        _.each(terms, function(obj, i) {
            var className = "";
            if (i >= 1) {
                className += "showHideDiv hide";
            }
            obj['valueUrl'] = CommonViewFunction.breadcrumbUrlMaker(obj.url);
            html += '<div class="' + className + '" dataterm-name="' + obj.name + '"><div class="liContent"></div>' + obj.deleteHtml + '</div>';
        })
        if (terms.length > 1) {
            html += '<div><a  href="javascript:void(0)" data-id="showMoreLessTerm" class="inputTag inputTagGreen"><span>Show More </span><i class="fa fa-angle-right"></i></a></div>'
        }
        if (model.get('$id$')) {
            html += '<div><a href="javascript:void(0)" class="inputAssignTag" data-id="addTerm" data-guid="' + model.get('$id$').id + '"><i class="fa fa-folder-o">' + " " + 'Assign Term</i></a></div>'
        } else {
            html += '<div><a href="javascript:void(0)" class="inputAssignTag" data-id="addTerm"><i class="fa fa-folder-o">' + " " + 'Assign Term</i></a></div>'
        }
        return {
            html: '<div class="termTableBreadcrumb" dataterm-id="' + id + '">' + html + '</div>',
            object: { scopeId: id, value: terms }
        }

    }
    CommonViewFunction.tagForTable = function(model) {
        var traits = model.get('$traits$'),
            atags = "",
            addTag = "",
            count = 0;
        _.keys(model.get('$traits$')).map(function(key) {
            var tagName = Utils.checkTagOrTerm(traits[key].$typeName$),
                className = "inputTag";
            if (!tagName.term) {
                if (count >= 1) {
                    className += " hide";
                }
                ++count;
                atags += '<a class="' + className + '" data-id="tagClick">' + traits[key].$typeName$ + '<i class="fa fa-times" data-id="delete" data-name="' + tagName.name + '" data-guid="' + model.get('$id$').id + '" ></i></a>';
            }
        });

        if (model.get('$id$')) {
            addTag += '<a href="javascript:void(0)" data-id="addTag" class="inputTag" data-guid="' + model.get('$id$').id + '" ><i style="right:0" class="fa fa-plus"></i></a>';
        } else {
            addTag += '<a href="javascript:void(0)" data-id="addTag" class="inputTag"><i style="right:0" class="fa fa-plus"></i></a>';
        }
        if (count > 1) {
            addTag += '<a  href="javascript:void(0)" data-id="showMoreLess" class="inputTag inputTagGreen"><span>Show More </span><i class="fa fa-angle-right"></i></a>'
        }
        return '<div class="tagList">' + atags + addTag + '</div>';

    }
    CommonViewFunction.saveTermToAsset = function(options) {
        require(['models/VCatalog'], function(Vcatalog) {
            var VCatalog = new Vcatalog();
            var name = options.termName;
            VCatalog.url = function() {
                return "api/atlas/v1/entities/" + options.guid + "/tags/" + name;
            };
            VCatalog.save(null, {
                beforeSend: function() {},
                success: function(data) {
                    Utils.notifySuccess({
                        content: "Term " + name + Messages.addTermToEntitySuccessMessage
                    });
                    if (options.callback) {
                        options.callback();
                    }
                    if (options.collection) {
                        options.collection.fetch({ reset: true });
                    }
                },
                error: function(error, data, status) {
                    if (data && data.responseText) {
                        var data = JSON.parse(data.responseText);
                        Utils.notifyError({
                            content: data.message
                        });
                        if (options.callback) {
                            options.callback();
                        }
                    }
                },
                complete: function() {}
            });
        })
    }
    CommonViewFunction.userDataFetch = function(options) {
        if (options.url) {
            $.ajax({
                url: options.url,
                success: function(response) {
                    if (options.callback) {
                        options.callback(response);
                    }
                }
            });
        }
    }
    return CommonViewFunction;
});
