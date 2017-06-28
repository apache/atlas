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

//js imports
var $ = require("jquery");
var bootstrap = require("bootstrap");
var React = require("react");
var ReactDOM = require("react-dom");
var LinkedStateMixin = require("react-addons-linked-state-mixin");
var ReactBootstrap = require("react-bootstrap");

var ODFGlobals = require("./odf-globals.js");
var AJAXCleanupMixin = require("./odf-mixins.js");
var configurationStore = require("./odf-utils.js").ConfigurationStore;
var metadataStore = require("./odf-utils.js").MetadataStore;

var Button = ReactBootstrap.Button;
var Table = ReactBootstrap.Table;
var Modal = ReactBootstrap.Modal;
var Input = ReactBootstrap.Input;
var Alert = ReactBootstrap.Alert;
var Panel = ReactBootstrap.Panel;
var Label = ReactBootstrap.Label;
var Input = ReactBootstrap.Input;
var Image = ReactBootstrap.Image;
var Tabs = ReactBootstrap.Tabs;
var Tab = ReactBootstrap.Tab;

var ODFConfigPage = React.createClass({
  mixins: [LinkedStateMixin, AJAXCleanupMixin],

  getInitialState() {
      return ({odfconfig: { odf: {} }, showDeleteConfirmationDialog: false});
  },

  componentWillMount() {
    this.loadODFConfig();
  },

  componentWillUnmount() {
	  this.props.alertCallback({type: ""});
  },

  // all the properties we display under the "odf" path
  relevantODFPropList: ["instanceId", "odfUrl", "odfUser", "odfPassword", "consumeMessageHubEvents", "atlasMessagehubVcap", "runAnalysisOnImport", "runNewServicesOnRegistration"],

  loadODFConfig() {
     var req = configurationStore.readConfig(
    	       function(data) {
    	    	 // only "fish out" the properties we display and add them as
    	    	 // toplevel properties to the state.

    	    	 // if we have to make more complex updates this will no longer work
    	    	 var newStateObj = {};
    	    	 for (var i=0; i<this.relevantODFPropList.length; i++) {
    	    		 var prop = this.relevantODFPropList[i];
    	    		 if (data[prop]) {
    	    			 newStateObj[prop] = data[prop];
    	    		 }
    	    	 }
    	         this.setState( newStateObj );
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

  saveODFConfig() {
	  var newConfigObj = {};
	  for (var i=0; i<this.relevantODFPropList.length; i++) {
		 var prop = this.relevantODFPropList[i];
		 if (this.state[prop] != null) {
			 newConfigObj[prop] = this.state[prop];
		 }
	  }
	  var req = configurationStore.updateConfig(newConfigObj,
			  () => {
					  if(this.isMounted()){
						  this.props.alertCallback({type: "success", message: "Settings saved successfully."})
					  }
				  },
			  this.props.alertCallback );
	  this.storeAbort(req.abort);
  },

  createAtlasSampleData() {
    this.refs.sampleDataButton.disabled = true;
    $.ajax({
       url: ODFGlobals.metadataUrl + "/sampledata",
       type: 'GET',
       success: function(data) {
    	   if(this.isMounted()){
	    	   this.refs.sampleDataButton.disabled = false;
	    	   this.props.alertCallback({type: "success", message: "Sample data created successfully."});
    	   }
	   }.bind(this),
       error: function(xhr, status, err) {
    	   if(this.isMounted()){
    		   var msg = "Sample data creation failed failed: " + err.toString();
    		   this.props.alertCallback({type: "danger", message: msg});
    		   this.refs.sampleDataButton.disabled = false;
    	   }
	   }.bind(this)
     });
  },

  deleteAllAtlasData() {
	 this.refs.deleteAllDataButton.disabled = true;
	 $.ajax({
	       url: ODFGlobals.metadataUrl + "/resetalldata",
	       type: 'POST',
	       success: function(data) {
	    	   if(this.isMounted()){
		    	   this.refs.deleteAllDataButton.disabled = false;
		    	   this.props.alertCallback({type: "success", message: "All data removed!"});
		    	   this.closeDeleteConfirmationDialog();
	    	   }
		   }.bind(this),
	       error: function(xhr, status, err) {
	    	   if(this.isMounted()){
	    		   var msg = "Data deletion failed: " + err.toString();
	    		   this.props.alertCallback({type: "danger", message: msg});
	    		   this.refs.deleteAllDataButton.disabled = false;
		    	   this.closeDeleteConfirmationDialog();
	    	   }
		   }.bind(this)
	     });
  },

  openDeleteConfirmationDialog() {
	  this.setState( { showDeleteConfirmationDialog: true} );
  },

  closeDeleteConfirmationDialog() {
	  this.setState( { showDeleteConfirmationDialog: false} );
  },


  testAtlasConnection() {
//	  this.props.alertCallback({type: "warning", message: "Test connection not implemented yet."});
    $.ajax({
       url: ODFGlobals.metadataUrl + "/connectiontest",
       type: 'GET',
       success: function(data) {
    	   if(this.isMounted()){
	    	   this.props.alertCallback({type: "success", message: "Connection test successful."});
    	   }
	   }.bind(this),
       error: function(xhr, status, err) {
    	   if(this.isMounted()){
    		   var msg = "Connection test failed: " + err.toString();
    		   this.props.alertCallback({type: "danger", message: msg});
    	   }
	   }.bind(this)
     });
  },

  notificationValue() {
      if (this.state.runAnalysisOnImport) {
          return "create";
      }
      return "none";
  },

  notificationsChanged() {
      var newValue = this.refs.runAnalysisInput.getValue();
      var val = (newValue != "none");
      this.setState({runAnalysisOnImport: val});
  },

  render() {
    var divStyle = {
      marginLeft: "20px"
    };
    return (
      <div>
      	<form>
	      <fieldset className="form-group  label-floating">
		        <legend>General Settings</legend>
		        <br/>
		        <h4>Instance</h4>
		          <div style={divStyle}>
		            <Input type="text" label="ODF Instance ID" valueLink={this.linkState("instanceId")} disabled/>
		            <Input type="text" label="ODF URL" valueLink={this.linkState("odfUrl")}/>
		            <Input type="text" label="ODF User ID" valueLink={this.linkState("odfUser")}/>
		            <Input type="password" label="ODF Password" valueLink={this.linkState("odfPassword")}/>
		          </div>
		        <hr/>
		        <h4>Metadata store</h4>
		        <div style={divStyle}>
		          <Input type="text" label="Repository ID" valueLink={this.linkState("repositoryId")} disabled/>

		          <div style={divStyle} className="checkbox">
		           <label>
	                <input ref="consumeMessageHubEvents" type="checkbox" checkedLink={this.linkState("consumeMessageHubEvents")} />
	                <span className="checkbox-material">
	                	<span className="check"></span>
	                </span>
	                &nbsp;&nbsp;Consume events from Messagehub instead of a Kafka instance
	              </label>
	            </div>
		          <Input type="text" label="Atlas Messagehub VCAP" disabled={(this.state && !this.state["consumeMessageHubEvents"])} valueLink={this.linkState("atlasMessagehubVcap")}/>
		          <Button bsStyle="primary" onClick={this.testAtlasConnection}>Test connection</Button>
		          <Button bsStyle="success" ref="sampleDataButton" onClick={this.createAtlasSampleData}>Create Atlas sample data</Button>
		          <Button bsStyle="danger" ref="deleteAllDataButton" onClick={this.openDeleteConfirmationDialog}>Delete all Atlas data</Button>
		          <Modal show={this.state.showDeleteConfirmationDialog} onHide={this.closeDeleteConfirmationDialog}>
		            <Modal.Header closeButton>
		              <Modal.Title>Confirm deletion</Modal.Title>
		            </Modal.Header>
		            <Modal.Body>
		              <h4>Are you sure you want to delete all data from the metadata repository?</h4>
		            </Modal.Body>
		              <Modal.Footer>
		              <Button onClick={this.deleteAllAtlasData}>Delete all Data</Button>
		              <Button onClick={this.closeDeleteConfirmationDialog}>Close</Button>
		            </Modal.Footer>
		          </Modal>
		        </div>
		        <hr/>
		        <h4>Notifications</h4>
		        <div style={divStyle}>
		          <Input type="select" label="Run analysis automatically" ref="runAnalysisInput" onChange={this.notificationsChanged} value={this.notificationValue()}>
		             <option value="none">Never</option>
                     <option value="create">On create</option>
		          </Input>
		        </div>
		        <div style={divStyle} className="checkbox">
		           <label>
	                <input ref="runServicesOnRegInput" type="checkbox" checkedLink={this.linkState("runNewServicesOnRegistration")} />
	                <span className="checkbox-material">
	                	<span className="check"></span>
	                </span>
	                &nbsp;&nbsp;Automatically run all newly registered services in order to keep asset metadata up-to-date
	              </label>
	            </div>
		        <hr/>
		        <Button className="btn-raised" bsStyle="primary" onClick={this.saveODFConfig}>Save Settings</Button>
		        <Button onClick={this.loadODFConfig}>Reload</Button>
	        </fieldset>
        </form>
      </div>);
  }

});


var SparkConfigPage = React.createClass({
  mixins: [LinkedStateMixin, AJAXCleanupMixin],

  getInitialState() {
      return ({"clusterMasterUrl": ""});
  },

  componentWillMount() {
    this.loadODFConfig();
  },

  componentWillUnmount() {
	  this.props.alertCallback({type: ""});
  },

   loadODFConfig() {
     var req = configurationStore.readConfig(
    	       function(data) {
    	    	 var sparkConfig = {};
    	    	 if(data.sparkConfig != null){
    	    		 sparkConfig = data.sparkConfig;
    	    	 }
    	    	 this.setState(sparkConfig);
    	       }.bind(this),
    	       this.props.alertCallback
    	     );
     this.storeAbort(req.abort);
  },

  saveODFConfig() {
	  var sparkConfig = {clusterMasterUrl: this.state.clusterMasterUrl};
	  var req = configurationStore.updateConfig({"sparkConfig" : sparkConfig},
			  () => {
					  if(this.isMounted()){
						  this.props.alertCallback({type: "success", message: "Spark config saved successfully."})
					  }
				  },
			  this.props.alertCallback );
	  this.storeAbort(req.abort);
  },

  render() {
    var divStyle = {
      marginLeft: "20px"
    };

	var sparkSettings =  <div>
						<h4>Local spark cluster</h4>
		    			<div style={divStyle}>
						  <Input type="text" label="Cluster master url" valueLink={this.linkState("clusterMasterUrl")}/>
						</div>
					 </div>;


    return (
      <div>
      	<form>
	      <fieldset className="form-group  label-floating">
		        <legend>Spark configuration</legend>
		        	{sparkSettings}
		        <Button className="btn-raised" bsStyle="primary" onClick={this.saveODFConfig}>Save Settings</Button>
		        <Button onClick={this.loadODFConfig}>Reload</Button>
	        </fieldset>
        </form>
      </div>);
  }

});


var PropertyAddButton = React.createClass({
  getInitialState() {
     return ({showModal: false});
  },

  close() {
    this.setState({ showModal: false });
  },

  save() {
    var newPropObj = {};
    newPropObj[this.state.name] = this.state.value;
    var updateConfig = { userDefined: newPropObj };
    configurationStore.updateConfig(updateConfig,
    		() => { this.props.successCallback();
	                this.props.alertCallback({type: "success", message: "User-defined property added successfully."})
    		},
    		this.props.alertCallback
    );
  },

  saveAndClose() {
    this.save();
    this.close();
  },

  open() {
    this.setState({ showModal: true });
  },

  handleTextChange() {
    this.setState({
          name: this.refs.inputName.getValue(),
          value: this.refs.inputValue.getValue()
        });
  },

  handleClick() {
      this.open();
  },

  render: function() {
    return (<span>
    <Button bsStyle="primary" className="btn-raised" onClick={this.handleClick}>Add</Button>
      <Modal show={this.state.showModal} onHide={this.close}>
          <Modal.Header closeButton>
             <Modal.Title>Add Property</Modal.Title>
          </Modal.Header>
          <Modal.Body>
             <Input type="text" ref="inputName" label="Name" onChange={this.handleTextChange}></Input>
             <Input type="text" ref="inputValue" label="Value" onChange={this.handleTextChange}></Input>
         </Modal.Body>
         <Modal.Footer>
         <Button bsStyle="primary" onClick={this.saveAndClose}>Save</Button>
         <Button onClick={this.close}>Cancel</Button>
         </Modal.Footer>
			</Modal>
      </span>);
  }

});


var PropertyRemoveButton = React.createClass({

   handleClick() {
      var newPropObj = {};
      newPropObj[this.props.name] = null;
      var updateConfig = { userDefined: newPropObj };
      configurationStore.updateConfig(updateConfig,
    		  () => { this.props.successCallback();
		              this.props.alertCallback({type: "success", message: "User-defined property removed successfully."});
    		  },
    		  this.props.alertCallback
      );
   },

   render() {
     return (
    		 <Button onClick={this.handleClick}>Remove</Button>
    );
   }

});
var PropertyEditButton = React.createClass({

   getInitialState() {
      return ({showModal: false});
   },

   close() {
     this.setState({ showModal: false });
   },

   save() {
     var newPropObj = {};
     newPropObj[this.props.name] = this.state.value;
     var updateConfig = { userDefined: newPropObj };
     configurationStore.updateConfig(updateConfig,
    		 () => { this.props.successCallback();
    		         this.props.alertCallback({type: "success", message: "User-defined property saved successfully."})
    		 }, this.props.alertCallback
     );
   },

   saveAndClose() {
     this.save();
     this.close();
   },

   open() {
     this.setState({ showModal: true });
   },

   handleTextChange() {
     this.setState({
           value: this.refs.input.getValue()
         });
   },

   handleClick() {
       this.open();
   },

   render: function() {
     return (
       <span>
        <Button bsStyle="primary" onClick={this.handleClick}>Edit</Button>
        <Modal show={this.state.showModal} onHide={this.close}>
            <Modal.Header closeButton>
               <Modal.Title>Edit Property</Modal.Title>
            </Modal.Header>
            <Modal.Body>
               <h4>Enter new value for property ''{this.props.name}''</h4>
               <Input type="text" ref="input" onChange={this.handleTextChange} defaultValue={this.props.value}></Input>
           </Modal.Body>
           <Modal.Footer>
           <Button bsStyle="primary" onClick={this.saveAndClose}>Save</Button>
           <Button onClick={this.close}>Cancel</Button>
           </Modal.Footer>
        </Modal>
       </span>);
   }
});

var UserDefinedConfigPage = React.createClass({
   mixins : [AJAXCleanupMixin],

   getInitialState: function() {
      return {odfconfig: { userDefined: {}}};
   },

   loadUserDefConfig: function() {
     var req = configurationStore.readConfig(
       function(data) {
         this.setState( {odfconfig: data} );
       }.bind(this),
       this.props.alertCallback
     );

     this.storeAbort(req.abort);
   },

   componentDidMount: function() {
     this.loadUserDefConfig();
   },

   componentWillUnmount : function() {
	  this.props.alertCallback({type: ""});
   },

   render: function() {
     var tableContents = $.map(
           this.state.odfconfig.userDefined,
           function(value, name) {
        	   if (value) {
        		 var tdBtnFixStyle = { paddingTop : "26px"};

                 return <tr key={name}>
                          <td style={tdBtnFixStyle}>{name}</td>
                          <td style={tdBtnFixStyle}>{value}</td>
                          <td><PropertyEditButton name={name} value={value} successCallback={this.loadUserDefConfig} alertCallback={this.props.alertCallback}/>
                              <PropertyRemoveButton name={name} successCallback={this.loadUserDefConfig} alertCallback={this.props.alertCallback}/>
                          </td>
                        </tr>;
        	   }
        	   // empty element
        	   return null;
           }.bind(this));
     return (
       <div>
	       <form>
		      <fieldset className="form-group  label-floating">
		      <legend>
			       User-defined properties
		       </legend>
		       <Table responsive>
		          <thead>
		            <tr>
		              <th>Name</th>
		              <th>Value</th>
		              <th></th>
		            </tr>
		          </thead>
		          <tbody>
		             {tableContents}
		          </tbody>
		       </Table>
		       <PropertyAddButton successCallback={this.loadUserDefConfig} alertCallback={this.props.alertCallback}/>
	       </fieldset>
	       </form>
       </div>);
   }

});



module.exports = {ODFConfigPage : ODFConfigPage, SparkConfigPage : SparkConfigPage, UserDefinedConfigPage: UserDefinedConfigPage} ;
