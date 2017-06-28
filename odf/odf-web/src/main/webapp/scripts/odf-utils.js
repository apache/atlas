/**
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
var $ = require("jquery");
var React = require("react");
var ODFGlobals = require("./odf-globals.js");

var Utils= {

	arraysEqual : function(arr1, arr2){
		var a = arr1;
		var b = arr2;
		if(arr1 == null){
			if(arr2 == null){
				return true;
			}
			return false;
		}else{
			if(arr2 == null){
				return false;
			}
		}

		if(a.length != b.length){
			return false;
		}

		var equal = true;
		$.each(a, function(key, val){
			if(a[key] && !b[key]){
				equal = false;
				return;
			}
			if(val && typeof val == "object"){
				equal = this.arraysEqual(val, b[key]);
				return;
			}else{
				if(val != b[key]){
					equal = false;
					return;
				}
			}
		}.bind(this));
		return equal;
	},

	AnnotationStoreHelper : {
		loadAnnotationsForRequest : function(analysisRequestId, successCallback, errorCallback) {
		    var url = ODFGlobals.annotationsUrl + "?analysisRequestId=" + analysisRequestId;
            return $.ajax({
               url: url,
               type: 'GET',
               success: function(data) {
                   if(successCallback){
                       successCallback(data);
                   }
               },
               error: function(xhr, status, err) {
                   if(errorCallback){
                       errorCallback(err);
                   }
               }
            });
		}
	},

	AtlasHelper : {

		loadAtlasAssets : function(assets, successCallback, errorCallback){
			var reqs = [];
			$.each(assets, function(key, val){
				reqs.push(this.loadAtlasAsset(val, successCallback, errorCallback));
			}.bind(this));
			return reqs;
		},

		loadMostRecentAnnotations : function(asset, successCallback, errorCallback) {
		    var url = ODFGlobals.annotationsUrl + "/newestAnnotations/" + encodeURIComponent(JSON.stringify({repositoryId: asset.repositoryId, id: asset.id}));
            return $.ajax({
               url: url,
               type: 'GET',
               success: function(data) {
                   if(successCallback){
                       successCallback(data);
                   }
               },
               error: function(xhr, status, err) {
                   if(errorCallback){
                       errorCallback(err);
                   }
               }
            });
		},

		loadRelationalDataSet: function(dataSet, successCallback, errorCallback) {
			var url = ODFGlobals.metadataUrl + "/asset/" + encodeURIComponent(JSON.stringify({repositoryId: dataSet.reference.repositoryId, id: dataSet.reference.id})) + "/columns";
			return $.ajax({
				url: url,
				type: 'GET',
				error: function(xhr, status, err) {
					if(errorCallback){
						errorCallback(err);
					}
				}
			}).then( function(cols){
				if(!cols){
					successCallback([]);
					return [];
				}
				var requests = [];
				var colRefs = [];
				$.each(cols, function(key, val){
					var req = Utils.AtlasHelper.getColAnnotations(val);
					requests.push(req);
					colRefs.push(val.reference);
				}.bind(this));
				dataSet.columns = colRefs;
				$.when.apply(undefined, requests).done(function(){
					var data = [];
					if(requests.length > 1){
						$.each(arguments, function(key, val){
							data.push(val);
						});
					}else if(arguments[0]){
						data.push(arguments[0]);
					}
					successCallback(data);
				});
				return requests;
			})
		},

		getColAnnotations: function(asset, successCallback, errorCallback) {
			var refid = asset.reference.id;
		   var annotationsUrl = ODFGlobals.annotationsUrl + "?assetReference=" + encodeURIComponent(refid);
		   return $.ajax({
			   url: annotationsUrl,
			   type: 'GET',
			   success: function(annotationData) {
				   asset.annotations = annotationData.annotations;
				   if (successCallback) {
					   successCallback(asset);
				   }
			   },
			   error: function(xhr, status, err) {
				   if(errorCallback){
					   errorCallback(err);
				   }
			   }
		   }).then(function(annotationData) {
			   asset.annotations = annotationData.annotations;
			   return asset;
		   });
		},

		loadAtlasAsset : function(asset, successCallback, errorCallback){
			var url = ODFGlobals.metadataUrl + "/asset/" + encodeURIComponent(JSON.stringify({repositoryId: asset.repositoryId, id: asset.id}));
			return $.ajax({
		       url: url,
		       type: 'GET',
		       error: function(xhr, status, err) {
		    	   if(errorCallback){
		    		   errorCallback(err);
		    	   }
		       }
			}).then( function(data) {
	    		   var refid = data.reference.id;
	    		   var annotationsUrl = ODFGlobals.annotationsUrl + "?assetReference=" + encodeURIComponent(refid);
	    		   return $.ajax({
	    			  url: annotationsUrl,
	    			  type: 'GET',
	    			  success: function(annotationData) {
	    				  data.annotations = annotationData.annotations;
	    				  if (successCallback) {
	    					  successCallback(data);
	    				  }
	    			  },
	    			  error: function(xhr, status, err) {
	    				  if(errorCallback){
	    					  errorCallback(err);
	    				  }
	    			  }
	    		   }).then(function(annotationData) {
	     			   data.annotations = annotationData.annotations;
	    			   return data;
	    		   });
			});
		},

		searchAtlasMetadata : function(query, successCallback, errorCallback) {
			var url = ODFGlobals.metadataUrl + "/search?" + $.param({query: query});
			var req = $.ajax({
				url: url,
				dataType: 'json',
				type: 'GET',
				success: function(data) {
					successCallback(data);
				},
				error: function(xhr, status, err) {
					console.error(url, status, err.toString());
					var msg = "Error while loading recent analysis requests: " + err.toString();
					errorCallback(msg);
				}
			});
			return req;
		}
	},

	MetadataStore : {

		getProperties(successCallback, alertCallback) {
			if (alertCallback) {
				alertCallback({type: ""});
			}
			return $.ajax({
				url: ODFGlobals.metadataUrl,
				dataType: 'json',
				type: 'GET',
				success: successCallback,
				error: function(xhr, status, err) {
					if (alertCallback) {
						var msg = "Error while reading metadata store properties: " + err.toString();
						alertCallback({type: "danger", message: msg});
					}
				}
			});
		}
	},

	ConfigurationStore : {

	  // readUserDefinedProperties(successCallback, alertCallback) {
	   readConfig(successCallback, alertCallback) {
		   if (alertCallback) {
		     alertCallback({type: ""});
		   }
	     // clear alert

	     return $.ajax({
	       url: ODFGlobals.apiPrefix + "settings",
	       dataType: 'json',
	       type: 'GET',
	       success: successCallback,
	       error: function(xhr, status, err) {
	         if (alertCallback) {
	            var msg = "Error while reading user defined properties: " + err.toString();
	            alertCallback({type: "danger", message: msg});
	         }
	       }
	      });
	   },

	   updateConfig(config, successCallback, alertCallback) {
			if (alertCallback) {
				 alertCallback({type: ""});
			}

		    return $.ajax({
			       url: ODFGlobals.apiPrefix + "settings",
			       contentType: "application/json",
			       dataType: 'json',
			       type: 'PUT',
			       data: JSON.stringify(config),
			       success: successCallback,
			       error: function(xhr, status, err) {
			         if (alertCallback) {
			            var msg = "Error while reading user defined properties: " + err.toString();
			            alertCallback({type: "danger", message: msg});
			         }
			       }
		     });
	   }
	},

	ServicesStore : {

	  // readUserDefinedProperties(successCallback, alertCallback) {
	   getServices(successCallback, alertCallback) {
		   if (alertCallback) {
		     alertCallback({type: ""});
		   }
	     // clear alert

	     return $.ajax({
	       url: ODFGlobals.apiPrefix + "services",
	       dataType: 'json',
	       type: 'GET',
	       success: successCallback,
	       error: function(xhr, status, err) {
	         if (alertCallback) {
	            var msg = "Error while getting list of ODF services: " + err.toString();
	            alertCallback({type: "danger", message: msg});
	         }
	       }
	      });
	   }
	},

	URLHelper : {

		getBaseHash : function(){
			var baseHash = "#" + document.location.hash.split("#")[1];
			var split = baseHash.split("/");
			if(split.length>0){
				return split[0];
			}
			return "";
		},

		setUrlHash : function(newAddition){
			if(!newAddition){
				newAddition = "";
			}
			if(newAddition != "" && typeof newAddition === "object"){
				newAddition = JSON.stringify(newAddition);
			}
			var hash = document.location.hash;
			var baseHash = this.getBaseHash();
			if(!hash.startsWith(baseHash)){
				return;
			}
			document.location.hash = baseHash + "/" + encodeURIComponent(newAddition);
		}
	}
};

module.exports = Utils;
