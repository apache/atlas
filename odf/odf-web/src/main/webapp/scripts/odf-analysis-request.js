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
var bootstrap = require("bootstrap");

var React = require("react");
var ReactDOM = require("react-dom");
var LinkedStateMixin = require('react-addons-linked-state-mixin');
var ReactBootstrap = require("react-bootstrap");
var AJAXCleanupMixin = require("./odf-mixins.js");
var configurationStore = require("./odf-configuration-store.js");
var metadataStore = require("./odf-utils.js").MetadataStore;
var ODFGlobals = require("./odf-globals.js");

var Button = ReactBootstrap.Button;
var Row = ReactBootstrap.Row;
var Col = ReactBootstrap.Col;
var Table = ReactBootstrap.Table;
var Modal = ReactBootstrap.Modal;
var Input = ReactBootstrap.Input;
var Alert = ReactBootstrap.Alert;
var Panel = ReactBootstrap.Panel;
var Label = ReactBootstrap.Label;
var Input = ReactBootstrap.Input;
var Image = ReactBootstrap.Image;

var OdfAnalysisRequest = {
	NewAnalysisRequestButton : React.createClass({

		getInitialState : function(){
			return {showAnalysisRequestDialog : false};
		},

		open : function(){
			this.setState({showAnalysisRequestDialog: true});
		},

		onClose : function(){
			this.setState({showAnalysisRequestDialog: false});
			if(this.props.onClose){
				this.props.onClose();
			}
		},

		render : function() {
			return (
					<span>
						<Button bsStyle={this.props.bsStyle} onClick={this.open}>Start analysis (service sequence)</Button>
						<OdfAnalysisRequest.NewAnalysisRequestDialog show={this.state.showAnalysisRequestDialog} dataSetId={this.props.dataSetId} alertCallback={this.props.alertCallback} onClose={this.onClose}/>
					</span>
			);
		}

	}),

	NewAnalysisRequestDialog : React.createClass({

	  mixins : [AJAXCleanupMixin],

	  getInitialState : function() {
	    return ({config: null, discoveryServices: [], errorMessage: null, discoveryServiceSequence: []});
	  },

	  close : function() {
		  this.clearDialogState();
		  if(this.props.onClose){
			  this.props.onClose();
		  }
	  },

	  submitRequest : function() {
		this.setState({requestInProgress : true});
	    var dataSet = this.refs.inputDataSet.getValue();
	    var discoveryServiceIDs = $.map(this.state.discoveryServiceSequence,
	       function(dsreg) {
	          return dsreg.id;
	       }
	    );

	    var repositoryId = this.state.repositoryId;
	    var metadataObjectRef = {
	      repositoryId: repositoryId,
	      id: dataSet
	    };
	    var analysisRequest = {
	      dataSets: [metadataObjectRef],
	      discoveryServiceSequence: discoveryServiceIDs
	    };

	    // now post request
	    // clear alert
	    if(this.props.alertCallback){
	    	this.props.alertCallback({type: "", message: ""});
	    }
	    var req = $.ajax({
	      url: ODFGlobals.analysisUrl,
	      contentType: "application/json",
	      dataType: 'json',
	      type: 'POST',
	      data: JSON.stringify(analysisRequest),
	      success: function(analysisResponse) {
	        if(!this.isMounted()){
	        	return;
	        }
	    	if (analysisResponse.invalidRequest) {
	          this.setState({errorMessage: analysisResponse.details, requestInProgress: false});
	        } else {
	          var msg = "Analysis request was started. ID: " + analysisResponse.id;
	          if(this.props.alertCallback){
	      	    this.props.alertCallback({type: "success", message: msg});
	          }
	      	  this.close();
	        }
	      }.bind(this),
	      error: function(xhr, status, err) {
	        var msg = "Error while reading ODF services: " + err.toString();
	        this.setState({errorMessage: msg, requestInProgress: false});
	      }.bind(this)
	    });

	    this.storeAbort(req.abort);
	  },

	  componentDidMount : function() {
		  this.loadDiscoveryServices();
	  },

	  loadDiscoveryServices : function() {
	    var req = configurationStore.readConfig(
	      function(config) {
	    	if(!this.isMounted()){
	        	return;
	        }
	        this.setState({config: config});
	        // clear alert
	        if(this.props.alertCallback){
	        	this.props.alertCallback({type: "", message: ""});
	        }
	        var req2 = $.ajax({
	          url: ODFGlobals.servicesUrl,
	          dataType: 'json',
	          type: 'GET',
	          success: function(data) {
	        	if(!this.isMounted()){
	  	        	return;
	  	        }
	            this.setState({discoveryServices: data});
	          }.bind(this),
	          error: function(xhr, status, err) {
	            var msg = "Error while reading ODF services: " + err.toString();
	            if(this.props.alertCallback){
	        	    this.props.alertCallback({type: "danger", message: msg});
	            }
	         }.bind(this)
	        });
	        this.storeAbort(req2.abort);
	      }.bind(this),
	      this.props.alertCallback
	    );

	    this.storeAbort(req.abort);
	  },

	  getDiscoveryServiceFromId : function(id) {
	      var servicesWithSameId = this.state.discoveryServices.filter(
	         function(dsreg) {
	             return dsreg.id == id;
	         }
	      );
	      if (servicesWithSameId.length > 0) {
	        return servicesWithSameId[0];
	      }
	      return null;
	  },

	  processDiscoveryServiceSelection : function() {
	      var selection = this.refs.inputAvailableDiscoveryServices.getValue();
	      var dsreg = this.getDiscoveryServiceFromId(selection);
	      if (dsreg) {
	        var newSequence = this.state.discoveryServiceSequence.slice();
	        newSequence.push(dsreg);
	        this.setState({discoveryServiceSequence: newSequence});
	      }
	  },

	  clearDialogState : function() {
	      this.setState({discoveryServiceSequence: [], requestInProgress : false, });
	  },

	  render : function() {
	     var alert = null;
	     if (this.state.errorMessage) {
	        alert = <Alert bsStyle="danger">{this.state.errorMessage}</Alert>;
	     }
	     var servicesOptions = $.map(
	            this.state.discoveryServices,
	            function(dsreg) {
	              return (<option key={dsreg.id} value={dsreg.id}>{dsreg.name}</option>);
	            }.bind(this)
	        );

	     var discoveryServiceSequenceComponents = $.map(this.state.discoveryServiceSequence,
	         function(dsreg) {
	            return <li key={dsreg.id}>{dsreg.name} ({dsreg.id})</li>
	         }
	     );

	     var waitingContainer = <div style={{position:"absolute", width:"100%", height:"100%", left:"50%", top: "30%"}}><Image src="img/lg_proc.gif" rounded /></div>;
	     if(!this.state.requestInProgress){
	    	 waitingContainer = null;
	     }

	     return (
	       <Modal show={this.props.show} onHide={this.close}>
	         <Modal.Header closeButton>
	            <Modal.Title>Start analysis (specify service sequence)</Modal.Title>
	         </Modal.Header>
	         <Modal.Body>
	         	{waitingContainer}
	            {alert}
	            <Input type="text" ref="inputDataSet" label="Data Set" value={this.props.dataSetId} readOnly={this.props.dataSetId}></Input>
	            <hr/>
	            Select a service from the "Available Services"
	            dropdown to append it to the sequence. Repeat selection to run multiple services for the data set.
	            <Input type="select" onChange={this.processDiscoveryServiceSelection} ref="inputAvailableDiscoveryServices" label="Available Services">
	              <option key="emptySelection">&lt;Select a service...&gt;</option>
	              {servicesOptions}
	            </Input>
	            <strong>Service Sequence</strong>
	            <ol>{discoveryServiceSequenceComponents}</ol>
	            <hr />
	            <Button bsStyle="warning" onClick={this.clearDialogState}>Clear Sequence</Button>
	        </Modal.Body>
	        <Modal.Footer>
	        <Button onClick={this.submitRequest} bsStyle="primary">Submit</Button>
	        <Button onClick={this.close} >Cancel</Button>
	        </Modal.Footer>
	       </Modal>
	     );
	  }

	}),

	NewCreateAnnotationsButton : React.createClass({

		getInitialState : function(){
			return {showCreateAnnotationsDialog : false};
		},

		open : function(){
			this.setState({showCreateAnnotationsDialog: true});
		},

		onClose : function(){
			this.setState({showCreateAnnotationsDialog: false});
			if(this.props.onClose){
				this.props.onClose();
			}
		},

		render : function() {
			return (
					<span>
						<Button bsStyle={this.props.bsStyle} onClick={this.open}>Start analysis (annotation types)</Button>
						<OdfAnalysisRequest.NewCreateAnnotationsDialog show={this.state.showCreateAnnotationsDialog} dataSetId={this.props.dataSetId} alertCallback={this.props.alertCallback} onClose={this.onClose}/>
					</span>
			);
		}

	}),

	NewCreateAnnotationsDialog : React.createClass({

		  mixins : [AJAXCleanupMixin],

		  getInitialState : function() {
		    return ({config: null, annotationTypes: [], errorMessage: null, analysisTypeSelection: []});
		  },

		  close : function() {
			  this.clearDialogState();
			  if(this.props.onClose){
				  this.props.onClose();
			  }
		  },

		  submitRequest : function() {
			this.setState({requestInProgress : true});
		    var dataSet = this.refs.inputDataSet.getValue();
		    var annotationTypeIDs = $.map(this.state.analysisTypeSelection,
		       function(annotationTypeId) {
		          return annotationTypeId;
		       }
		    );

		    var repositoryId = this.state.repositoryId;
		    var metadataObjectRef = {
		      repositoryId: repositoryId,
		      id: dataSet
		    };
		    var analysisRequest = {
		      dataSets: [metadataObjectRef],
		      annotationTypes: annotationTypeIDs
		    };

		    // now post request
		    // clear alert
		    if(this.props.alertCallback){
		    	this.props.alertCallback({type: "", message: ""});
		    }
		    var req = $.ajax({
		      url: ODFGlobals.analysisUrl,
		      contentType: "application/json",
		      dataType: 'json',
		      type: 'POST',
		      data: JSON.stringify(analysisRequest),
		      success: function(analysisResponse) {
		        if(!this.isMounted()){
		        	return;
		        }
		    	if (analysisResponse.invalidRequest) {
		          this.setState({errorMessage: analysisResponse.details, requestInProgress: false});
		        } else {
		          var msg = "Analysis request was started. ID: " + analysisResponse.id;
		          if(this.props.alertCallback){
		      	    this.props.alertCallback({type: "success", message: msg});
		          }
		      	  this.close();
		        }
		      }.bind(this),
		      error: function(xhr, status, err) {
		        var msg = "Error starting discovery request: " + err.toString();
		        this.setState({errorMessage: msg, requestInProgress: false});
		      }.bind(this)
		    });

		    this.storeAbort(req.abort);
		  },

		  componentDidMount : function() {
			  this.loadannotationTypes();
		  },

		  loadannotationTypes : function() {
		    var req = configurationStore.readConfig(
		      function(config) {
		    	if(!this.isMounted()){
		        	return;
		        }
		        this.setState({config: config});
		        // clear alert
		        if(this.props.alertCallback){
		        	this.props.alertCallback({type: "", message: ""});
		        }
		        var req2 = $.ajax({
		          url: ODFGlobals.servicesUrl,
		          dataType: 'json',
		          type: 'GET',
		          success: function(data) {
		        	if(!this.isMounted()){
		  	        	return;
		  	        }
		            var ids = [];
		            $.each(data, function(key, dsreg){
			            $.each(dsreg.resultingAnnotationTypes, function(key, annotationTypeId){
			            	if($.inArray(annotationTypeId,ids) == -1){
				            	ids.push(annotationTypeId);
			            	};
			            });
		            });
		            this.setState({annotationTypes: ids});
		          }.bind(this),
		          error: function(xhr, status, err) {
		            var msg = "Error while reading ODF services: " + err.toString();
		            if(this.props.alertCallback){
		        	    this.props.alertCallback({type: "danger", message: msg});
		            }
		         }.bind(this)
		        });
		        this.storeAbort(req2.abort);
		      }.bind(this),
		      this.props.alertCallback
		    );
			 metadataStore.getProperties(
					 function(data) {
					     this.setState({repositoryId: data.STORE_PROPERTY_ID});
					 }.bind(this)
			 );
		    this.storeAbort(req.abort);
		  },

		  processAnalysisTypeSelection : function() {
		      var selection = this.refs.inputAvailableAnnotationTypes.getValue();
		      if (selection) {
		        var newSelection = this.state.analysisTypeSelection.slice();
		        newSelection.push(selection);
		        this.setState({analysisTypeSelection: newSelection});
		      }
		  },

		  clearDialogState : function() {
		      this.setState({analysisTypeSelection: [], requestInProgress : false, });
		  },

		  render : function() {
		     var alert = null;
		     if (this.state.errorMessage) {
		        alert = <Alert bsStyle="danger">{this.state.errorMessage}</Alert>;
		     }
		     var analysisTypeOptions = $.map(
			            this.state.annotationTypes,
			            function(annotationTypeId) {
			              return (<option key={annotationTypeId} value={annotationTypeId}>{annotationTypeId}</option>);
			            }.bind(this)
			        );

		     var analysisTypeSelectionComponents = $.map(this.state.analysisTypeSelection,
		         function(annotationTypeId) {
		            return <li key={annotationTypeId}>{annotationTypeId}</li>
		         }
		     );

		     var waitingContainer = <div style={{position:"absolute", width:"100%", height:"100%", left:"50%", top: "30%"}}><Image src="img/lg_proc.gif" rounded /></div>;
		     if(!this.state.requestInProgress){
		    	 waitingContainer = null;
		     }

		     return (
		       <Modal show={this.props.show} onHide={this.close}>
		         <Modal.Header closeButton>
		            <Modal.Title>Start analysis (specify annotation types)</Modal.Title>
		         </Modal.Header>
		         <Modal.Body>
		         	{waitingContainer}
		            {alert}
		            <Input type="text" ref="inputDataSet" label="Data Set" value={this.props.dataSetId} readOnly={this.props.dataSetId}></Input>
		            <hr/>
		            Select an annotation type from the "Available Annotation Types"
		            dropdown to append it to the list. Repeat selection to create multiple annotation types for the data set.
		            <Input type="select" onChange={this.processAnalysisTypeSelection} ref="inputAvailableAnnotationTypes" label="Available Annotation Types">
		              <option key="emptySelection">&lt;Select an annotation type...&gt;</option>
		              {analysisTypeOptions}
		            </Input>
		            <strong>Selected Annotation Types</strong>
		            <ol>{analysisTypeSelectionComponents}</ol>
		            <hr />
		            <Button bsStyle="warning" onClick={this.clearDialogState}>Clear Selection</Button>
		        </Modal.Body>
		        <Modal.Footer>
		        <Button onClick={this.submitRequest} bsStyle="primary">Submit</Button>
		        <Button onClick={this.close} >Cancel</Button>
		        </Modal.Footer>
		       </Modal>
		     );
		  }

		})
}


module.exports = OdfAnalysisRequest;
