/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * 'License'); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

angular.module('dgc.tags').factory('TagClasses', ['lodash', function ClassFactory(_) {

    function Tag(props) {
        return _.merge({
            superTypes: [],
            typeName: null,
            attributeDefinitions: []
        }, props);
    }

    var classes = {
        ENUM: 'org.apache.atlas.typesystem.types.EnumType',
        TRAIT: 'org.apache.atlas.typesystem.types.TraitType',
        STRUCT: 'org.apache.atlas.typesystem.types.StructType',
        CLASS: 'org.apache.atlas.typesystem.types.ClassType'
    };

    var instanceRespons = {
        ENUM: 'enumTypes',
        STRUCT: 'structTypes',
        TRAIT: 'traitTypes',
        SUPER: 'superTypes'
    };

    function Class(classId, className) {
        this.tags = [];
        this.id = classId;
        this.name = className;

        this.addTag = function AddTag(props) {
            var tag = new Tag(_.merge({
                hierarchicalMetaTypeName: className
            }, props));

            this.tags.push(tag);
            return this;
        };

        this.clearTags = function RemoveTags() {
            this.tags = [];
            return this;
        };

        this.toJson = function CreateJson() {
            var classTypeKey = (this.id.toLowerCase() + 'Types'),
                output = {};

            _.forEach(classes, function addTypesArray(className, classKey) {
                output[classKey.toLowerCase() + 'Types'] = [];
            });

            output[classTypeKey] = this.tags;
            return output;
        };

        this.instanceInfo = function() {
            return instanceRespons[classId];
        };
    }

    return _.chain(classes)
        .map(function CreateClass(className, classId) {
            return new Class(classId, className);
        })
        .indexBy('id')
        .value();
}]);
