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
var ReactBootstrap = require("react-bootstrap");

var Label = ReactBootstrap.Label;
var ListGroup = ReactBootstrap.ListGroup;
var ListGroupItem = ReactBootstrap.ListGroupItem;
var Glyphicon = ReactBootstrap.Glyphicon;

/*
 * for every data type a UI specification can be created.
 * A UI specification is an array of objects.
 *
 * Normally, the key of a property will be used to find a matching ui spec.
 * This can be overwritten by defining the uiSpec attribute on a property object.
 *
 * Each object requires a key to identify the property and a label that will be displayed
 * In order to manipulate the value or how it is displayed, a property object can pass a function to the func attribute.
 * This function will be called with the property value and the object as parameters.
 *
 * Properties with an array as their value will automatically be displayed in a grid.
 * A UI specification that is used for a grid can have a property object with the attribute sort:true, causing the tablet to be sorted alphabetically on this property
*/

var UISpec = {

		DefaultDocument : {
			attributes: [
			            {key: "name", label: "Name"},
			            {key: "description", label: "Description"},
			            {key: "type", label: "Type"}],
		    icon: <Glyphicon glyph="question-sign" />
		},

		DefaultDocuments : {
			attributes: [
			            {key: "name", label: "Name"},
			            {key: "description", label: "Description"},
			            {key: "type", label: "Type"}],
		    icon: <Glyphicon glyph="question-sign" />
		},

		Document : {
			attributes: [{key: "reference.id", label: "ID"},
		            {key: "name", label: "Name"},
		            {key: "type", label: "Type"}],
		    icon: <Glyphicon glyph="file" />
		},

		Documents : {
			attributes: [{key: "name", label: "Name"},
		            {key: "description", label: "Description"},
		            {key: "columns", label: "Columns"} ,
		            {key: "annotations", label: "Annotations",
		            	 func: function(val){
		            		 if(!val){
		            			 return 0;
		            		 }
		            		 return val.length;
		            	 }
		             }],
		   icon: <Glyphicon glyph="file" />
		},

		DataFile : {
			attributes: [{key: "name", label: "Name"},
			          {key: "description", label: "Description"},
			          {key: "columns", label: "Columns"} ,
			          {key: "annotations", label: "Annotations"}],
			icon: <Glyphicon glyph="list-alt" />
		},

		DataFiles: {
			attributes: [{key: "name", label: "Name"},
		             {key: "columns", label: "Columns"} ,
		             {key: "annotations", label: "Annotations",
		            	 func: function(val){
		            		 if(!val){
		            			 return 0;
		            		 }
		            		 return val.length;
		            	 }
		             }],
		    icon: <Glyphicon glyph="list-alt" />
		},

		Table : {
			attributes: [{key: "schema", label: "Schema"},
		         {key: "name", label: "Name"},
		         {key:"description", label: "Description"},
		         {key:"columns", label: "Columns"} ,
		         {key: "annotations", label: "Annotations"}],
     	    icon: <Glyphicon glyph="th" />
		},

		Tables : {
			attributes: [ // {key: "schema", label: "Schema"},
	          {key: "name", label: "Name"},
		          {key: "columns", label: "Columns"} ,
		          {key: "annotations", label: "Annotations",
		        	  func: function(val){
		        		  if(!val){
		        			  return 0;
		        			  }
		        		  return val.length;
		        		}
		          }],
     	    icon: <Glyphicon glyph="th" />
		},

		column : {
			attributes: [{key: "name", label: "Name"},
		          {key: "dataType", label: "Datatype"},
		          {key: "annotations", label: "Annotations"}],

		    icon: <Glyphicon glyph="th-list" />
		},

		columns : {
			attributes: [{key: "name", label: "Name", sort: true},
		           {key: "dataType", label: "Datatype"},
		           {key: "annotations", label: "Annotations",
			        	  func: function(val){
			        		  if(!val){
			        			  return 0;
			        		  }
			        		  return val.length;
			        		}
			          }],
		    icon: <Glyphicon glyph="th-list" />
		},

		//InferredDataClass	AnalysisRun	AnnotationType	JavaClass	Annotations	AnnotatedObject	Reference	JsonProperties
		annotation : {
			attributes: [{key: "annotationType", label: "Annotation type"},

		              // see Infosphere DQ service: ColumnAnalysisTableAnnotation
                      {key: "dataClassDistribution", label: "Data Class Distribution",
		                  func: function(val) {
		                      if (val) {
		                          return <span>{JSON.stringify(val)}</span>;
		                      }
		                  }
                      },
                      // see Infosphere DQ service: ColumnAnalysisColumnAnnotation
		              {key: "inferredDataClass", label: "Data Class",
		                func: function(val) {
		                     if (val) {
                                if(val.className){
                                    var confidence = "";
                                    if (val.confidenceThreshold) {
                                        confidence = " ("+val.confidenceThreshold+")";
                                    }
                                    return <span>{val.className}{confidence}</span>;
                                }
                                return <span>{JSON.stringify(val)}</span>;
		                     }
                          }
		              },
                      {key: "qualityScore", label: "Data Quality Score"},

		              // see alchemy taxonomy service: TaxonomyDiscoveryService.TaxonomyAnnotation
		              {key: "label", label: "Category"},
                      {key: "score", label: "Score"},

		              {key: "analysisRun", label:"Analysis"},
		              {key: "jsonProperties", label: "Properties"}
		              ],
  		    icon: <Glyphicon glyph="tag" />
		},

		annotations : {
			attributes: [{key: "annotationType", label: "Annotation type"},
			             {key: "analysisRun", label:"Analysis"}],
		  	icon: <Glyphicon glyph="tag" />
		},

	    request : {
	    	attributes:[
	               {key: "request.id", label: "Request ID"},
	               {key: "state", label: "Status",
	            	   func: function(val){
	            		   var btnCss = {};
	                       var statusLabel = <Label bsStyle="warning">Unknown</Label>;
	                       if (val == "ACTIVE") {
	                          statusLabel = <Label bsStyle="info">Active</Label>;
	                       } else if (val== "QUEUED") {
	                          statusLabel = <Label bsStyle="info">Queued</Label>;
	                       } else if (val== "CANCELLED") {
	                          statusLabel = <Label bsStyle="warning">Cancelled</Label>;
	                       } else if (val== "FINISHED") {
	                          statusLabel = <Label bsStyle="success">Finished</Label>;
	                       } else if (val== "ERROR") {
	                          statusLabel = <Label bsStyle="danger">Error</Label>;
	                       }
	                       return statusLabel;
	            	   }},
            	   {key: "request.dataSets", label: "Data sets", uiSpec: "DefaultDocuments"},
            	   {key: "totalTimeOnQueues", label: "Total time on queues", func: function(val){
            		   if(val){
            			   	var x = val / 1000;
            			    var seconds = Math.floor(x % 60);
            			    x /= 60;
            			    var minutes = Math.floor(x % 60);
            			    x /= 60;
            			    var hours = Math.floor(x % 24);

            			    return hours + "h " + minutes + "m " + seconds + " s";
            		   }
            		   return "";
            	   }},
            	   {key: "totalTimeProcessing", label: "Total time processing", func: function(val){
            		   if(val){
            			   	var x = val / 1000;
            			    var seconds = Math.floor(x % 60);
            			    x /= 60;
            			    var minutes = Math.floor(x % 60);
            			    x /= 60;
            			    var hours = Math.floor(x % 24);

            			    return hours + "h " + minutes + "m " + seconds + " s";
            		   }
            		   return "";
            	   }},
            	   {key: "totalTimeStoringAnnotations", label: "Total time storing results", func: function(val){
            		   if(val){
            			   	var x = val / 1000;
            			    var seconds = Math.floor(x % 60);
            			    x /= 60;
            			    var minutes = Math.floor(x % 60);
            			    x /= 60;
            			    var hours = Math.floor(x % 24);

            			    return hours + "h " + minutes + "m " + seconds + " s";
            		   }
            		   return "";
            	   }},
	               {key: "serviceRequests", label: "Service Sequence", func: function(val, obj){
	            	   var serviceNames = [];
	            	   var services = [];
	            	   for (var i=0; i<val.length; i++) {
	                       var dsreq = val[i];
	                       var dsName = dsreq.discoveryServiceName;
	                       if(serviceNames.indexOf(dsName) == -1){
	                    	   serviceNames.push(dsName);
	                    	   services.push(<span key={dsName}>{dsName}<br/></span>);
	                       }
	                   }

	                   return <em>{services}</em>;
	               	}
	               },
	               {key: "details", label: "Status Details"}
	               ],
	   		  	icon: <Glyphicon glyph="play-circle" />
	    },

	    requests : {
	    	attributes: [
		               {key: "request.id", label: "Request ID"},
		               {key: "status", label: "Status",
		            	   func: function(val){
		                       var statusLabel = <Label bsStyle="warning">Unknown</Label>;
		                       if (val == "INITIALIZED") {
		                          statusLabel = <Label bsStyle="info">Initialized</Label>;
		                       } else if (val== "IN_DISCOVERY_SERVICE_QUEUE") {
		                          statusLabel = <Label bsStyle="info">Queued</Label>;
		                       } else if (val== "DISCOVERY_SERVICE_RUNNING") {
		                           statusLabel = <Label bsStyle="info">Running</Label>;
		                       } else if (val== "CANCELLED") {
		                          statusLabel = <Label bsStyle="warning">Cancelled</Label>;
		                       } else if (val== "FINISHED") {
		                          statusLabel = <Label bsStyle="success">Finished</Label>;
		                       } else if (val== "ERROR") {
		                          statusLabel = <Label bsStyle="danger">Error</Label>;
		                       }
		                       return statusLabel;
		            	   }},
		               {key: "lastModified", label: "Last modified", func: function(val){
		            	   return new Date(val).toLocaleString();
		               }},
		               {key: "discoveryServiceRequests", label: "Service sequence", func: function(val, obj){
		            	   var serviceNames = [];
		            	   var services = [];
		            	   for (var i=0; i<val.length; i++) {
		                       var dsreq = val[i];
		                       var dsName = dsreq.discoveryServiceName;
		                       if(serviceNames.indexOf(dsName) == -1){
		                    	   serviceNames.push(dsName);
		                    	   services.push(<span key={dsName}>{dsName}<br/></span>);
		                       }
		                   }

		                   return <ListGroup>{services}</ListGroup>;
		               }},
		               {key: "statusDetails", label: "Status Details"}
		               ],
	   	icon: <Glyphicon glyph="play-circle" />
	    }
};

module.exports = UISpec;
