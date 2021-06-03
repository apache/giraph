/*
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
/*
 * Abstracts the debugger controls.
 */

/*
 * Debugger is a class that encapsulates the graph editor and debugging controls.
 * @param {debuggerContainer} options - Initialize debugger with these options.
 * @param options.debuggerContainer - Selector for the container of the main debugger area. (Editor & Valpanel)
 * @param options.superstepControlsContainer - Selector for the container of the superstep controls.
 * @constructor
 */
function GiraphDebugger(options) {
    // Initialize (and reset) variables for jobs.
    this.resetVars();
    this.debuggerServerRoot = (location.protocol === 'file:' ? 'http://localhost:8000' : '');
    this.mode = GiraphDebugger.ModeEnum.EDIT;
    this.init(options);
    return this;
}

/* 
 * Denotes the mode.
 * Debug Mode - Editor is in readonly, walking through the supersteps of a giraph job.
 * Edit Mode - Create new graphs in the editor.
 */
GiraphDebugger.ModeEnum = {
    DEBUG : 'debug',
    EDIT : 'edit'
}

/*
 * Initializes the graph editor, node attr modal DOM elements.
 */
GiraphDebugger.prototype.init = function(options) {
    this.editorContainerId = 'editor-container';
    this.valpanelId = 'valpanel-container';

    // Create divs for valpanel and editor.
    var valpanelContainer = $('<div />')
        .attr('class', 'valpanel debug-control')
        .attr('id', this.valpanelId)
        .appendTo(options.debuggerContainer);

    var editorContainer = $('<div />')
        .attr('id', this.editorContainerId)
        .attr('class', 'debug-control')
        .appendTo(options.debuggerContainer);

    // Instantiate the editor object.
    this.editor = new Editor({
        'container' : '#' + this.editorContainerId,
        onOpenNode : this.openNodeAttrs.bind(this),
        onOpenEdge : this.openEdgeVals.bind(this)
    });
    

    // Add toggle view event handler.
    this.editor.onToggleView((function(editorView) {
        if (editorView === Editor.ViewEnum.TABLET) {
            this.btnToggleViewSpan.html(' Graph View');
        } else {
            this.btnToggleViewSpan.html(' Table View');
        }
    }).bind(this));

    // Instantiate the valpanel object.
    this.valpanel = new ValidationPanel({
        'container' : '#' + this.valpanelId,
        'editor' : this.editor,
        'debuggerServerRoot' : this.debuggerServerRoot,
        'resizeCallback' : (function() {
            this.editor.restartGraph();
        }).bind(this)
    });

    this.initIds();
    // Must initialize these members as they are used by subsequent methods.
    this.superstepControlsContainer = options.superstepControlsContainer;
    this.initElements(options);
}

/*
 * Deferred callbacks for capture scenario
 */
GiraphDebugger.prototype.onCaptureVertex = function(done, fail) {
    this.onCaptureVertex.done = this.valpanel.onCaptureVertex.done = done;
    this.onCaptureVertex.fail = this.valpanel.onCaptureVertex.fail = fail;
}

/*
 * Deferred callbacks for generate test graph.
 */
GiraphDebugger.prototype.onGenerateTestGraph = function(done, fail) {
    this.onGenerateTestGraph.done = done;
    this.onGenerateTestGraph.fail = fail;
}

/*
 * Deferred callbacks for capture scenario
 */
GiraphDebugger.prototype.onCaptureMaster = function(done, fail) {
    this.onCaptureMaster.done = done;
    this.onCaptureMaster.fail = fail;
}

/*
 * Reset job-related vars to the initial state. 
 */
GiraphDebugger.prototype.resetVars = function() { 
    // Node that is currently double clicked.
    this.selectedNodeId = null;
    // Initialize current superstep to -2 (Not in debug mode)
    this.currentSuperstepNumber = -2;
    // ID of the job currently being debugged.
    this.currentJobId = null;
    // Minimum value of superstepNumber
    this.minSuperstepNumber = -1;
    // Maximum value of superstepNumber - Depends on the job.
    // TODO(vikesh): Fetch from debugger server in some AJAX call. Replace constant below.
    this.maxSuperstepNumber = 15;
    // Caches the scenarios to show correct information when going backwards.
    // Cumulatively builds the state of the graph starting from the first superstep by merging
    // scenarios on top of each other.
    this.stateCache = {"-1": {}};
}

/*
 * Initialize DOM element Id constants
 */
GiraphDebugger.prototype.initIds = function() {
    this.ids = {
        // IDs of elements in node attribute modal.
        _nodeAttrModal : 'node-attr',
        _nodeAttrId : 'node-attr-id',
        _nodeAttrAttrs : 'node-attr-attrs',
        _nodeAttrGroupError : 'node-attr-group-error',
        _nodeAttrError : 'node-attr-error',
        _btnNodeAttrSave : 'btn-node-attr-save',
        _btnNodeAttrCancel : 'btn-node-attr-cancel',
        // IDs of elements in edge values modal
        _edgeValModal : 'edge-vals',
        // IDs of elements in Edit Mode controls.
        _selectSampleGraphs : 'sel-sample-graphs',
        // IDs of elements in Superstep controls.
        _btnPrevStep : 'btn-prev-step',
        _btnNextStep : 'btn-next-step',
        _btnEditMode : 'btn-edit-mode',
        _btnFetchJob : 'btn-fetch-job',
        _btnCaptureVertexScenario : 'btn-capture-scenario',
        _btnCaptureMasterScenario : 'btn-capture-scenario',
        _btnToggleView : 'btn-toggle-view'
    };
}

/*
 * Initializes the input elements inside the node attribute modal form.
 * @param nodeAttrForm - Form DOM object.
 */
GiraphDebugger.prototype.initInputElements = function(nodeAttrForm) {
   // Create form group for ID label and text box.
    var formGroup1 = $('<div />')
        .addClass('form-group')
        .appendTo(nodeAttrForm);

    // Create node ID label.
    var nodeAttrIdLabel = $('<label />')
        .attr('for', this.ids._nodeAttrId)
        .addClass('control-label col-sm-4')
        .html('Node ID:')
        .appendTo(formGroup1);

    // Create the ID input textbox.
    // Add it to a column div, which in turn is added to formgroup2.
    this.nodeAttrIdInput = $('<input>')
        .attr('type', 'text')
        .attr('id', this.ids._nodeAttrId)
        .addClass('form-control')
        .appendTo($('<div>').addClass('col-sm-8').appendTo(formGroup1));

    // Create the form group for attributes label and input.
    var formGroup2 = $('<div />')
        .addClass('form-group')
        .appendTo(nodeAttrForm);

    var nodeAttrAttributeLabel = $('<label />')
        .attr('for', this.ids._nodeAttrAttrs)
        .addClass('control-label col-sm-4')
        .html('Attributes: ')
        .appendTo(formGroup2);

    // Create the Attributes input textbox.
    // Add it to a column div, which in turn is added to formgroup2.
    this.nodeAttrAttrsInput = $('<input>')
        .attr('type', 'text')
        .attr('id', this._nodeAttrAttrs)
        .addClass('form-control')
        .appendTo($('<div>').addClass('col-sm-8').appendTo(formGroup2));

    // Create form group for buttons.
    var formGroupButtons = $('<div />')
        .addClass('form-group')
        .appendTo(nodeAttrForm);

    var buttonsContainer = $('<div />')
        .addClass('col-sm-12')
        .appendTo(formGroupButtons);

    this.btnNodeAttrSubmit = Utils.getBtnSubmitSm()
        .attr('type', 'submit')
        .attr('id', this.ids._btnNodeAttrSave)
        .appendTo(buttonsContainer);

    this.btnNodeAttrCancel = Utils.getBtnCancelSm()
        .attr('id', this.ids._btnNodeAttrCancel)
        .appendTo(buttonsContainer);

    this.nodeAttrGroupError = $('<div />')
        .addClass('form-group has-error')
        .attr('id', this.ids._nodeAttrGroupError)
        .hide()
        .appendTo(nodeAttrForm);

    var errorLabel = $('<label />')
        .addClass('control-label')
        .attr('id', this.ids._nodeAttrError)
        .html('Node ID must be unique')
        .appendTo($('<div class="col-sm-12"></div>').appendTo(this.nodeAttrGroupError));
}

/*
 * Initializes the message container and all elements within it.
 * Returns the message container DOM object.
 * @param nodeAttrForm - Form DOM object.
 */
GiraphDebugger.prototype.initMessageElements = function(nodeAttrForm) {
    var messageContainer = $('<div />')
        .appendTo(nodeAttrForm)

    var messageTabs = $('<ul />')
        .addClass('nav nav-tabs')
        .html('<li class="active"><a id="node-attr-received" class="nav-msg" href="#!">Received</a></li>' + 
            '<li><a id="node-attr-sent" class="nav-msg" href="#!">Sent</a></li>' +
            '<li><a id="node-attr-edgevals" class="nav-msg" href="#!">Edge Values</a></li>')
        .appendTo(messageContainer);

    var tableContainer = $('<div />')
        .addClass('highlight')
        .appendTo(messageContainer);

    this.flowTable = $('<table />')
        .addClass('table')
        .attr('id', 'node-attr-flow')
        .appendTo(messageContainer);
}

/*
 * Initializes Superstep controls.
 * @param superstepControlsContainer - Selector for the superstep controls container.
 */
GiraphDebugger.prototype.initSuperstepControls = function(superstepControlsContainer) {
    /*** Edit Mode controls ***/
    // Create the div with controls visible in Edit Mode
    this.editModeGroup = $('<div />')
        .appendTo(superstepControlsContainer);

    // Create the form that fetches the superstep data from debugger server.
    var formFetchJob = $('<div />')
        .attr('class', 'form-inline')
        .appendTo(this.editModeGroup);

    // Fetch job details for job id textbox.
    this.fetchJobIdInput = $('<input>')
        .attr('type', 'text')
        .attr('class', 'form-control ')
        .attr('placeholder', 'Job ID')
        .appendTo(formFetchJob);

    this.btnFetchJob = $('<button />')
        .attr('id', this.ids._btnFetchJob)
        .attr('type', 'button')
        .attr('class', 'btn btn-danger form-control')
        .html('Fetch')
        .appendTo(formFetchJob);

    // Create the control for creating sample graphs.
    var formSampleGraphs = $('<div />')
        .attr('class', 'form-inline')
        .appendTo(this.editModeGroup);

    this.selectSampleGraphs = $('<select />')
        .attr('class', 'form-control')
        .attr('id', this.ids._selectSampleGraphs)
        .appendTo(formSampleGraphs);

    // Add the graph names to the select drop down.
    $.each(Utils.sampleGraphs, (function (key, value) {
        $(this.selectSampleGraphs).append($('<option />').attr('value', key).html(key));
    }).bind(this));

    this.sampleGraphsInput = $('<input />')
        .attr('class', 'form-control')
        .attr('placeholder', '# of vertices')
        .appendTo(formSampleGraphs);

    this.btnSampleGraph = $('<button />')
        .attr('class', 'btn btn-primary form-control')
        .html('Generate')
        .appendTo(formSampleGraphs);

    /*** DEBUG MODE controls ***/
    this.debugModeGroup = $('<div />')
        .appendTo(superstepControlsContainer)
        .hide();

    // Initialize the actual controls.
    var formControls = $('<div />')
        .attr('id', 'controls')
        .attr('class', 'form-inline')
        .appendTo(this.debugModeGroup);

    this.btnPrevStep = $('<button />')
        .attr('class', 'btn btn-default bt-step form-control')
        .attr('id', this.ids._btnPrevStep)
        .attr('disabled', 'true')
        .append(
            $('<span />')
            .attr('class', 'glyphicon glyphicon-chevron-left')
            .html(' Previous')
        )
        .appendTo(formControls);

    var superstepLabel = $('<h2><span id="superstep">-1</span>' +
        '<small> Superstep</small></h2>')
        .appendTo(formControls);

    // Set this.superstepLabel to the actual label that will be updated.
    this.superstepLabel = $('#superstep');

    this.btnNextStep = $('<button />')
        .attr('class', 'btn btn-default btn-step form-control')
        .attr('id', this.ids._btnNextStep)
        .append(
            $('<span />')
            .attr('class', 'glyphicon glyphicon-chevron-right')
            .html(' Next')
        )
        .appendTo(formControls);

    // Return to the edit mode - Exiting the debug mode.
    this.btnEditMode = $('<button />')
        .attr('class', 'btn btn-default btn-step form-control')
        .attr('id', this.ids._btnEditMode)
        .append(
            $('<span />')
            .attr('class', 'glyphicon glyphicon-pencil')
            .html(' Edit Mode')
        )
        .appendTo(formControls);

   // Change the text value of this span when toggling views.
   this.btnToggleViewSpan = $('<span />')
                .attr('class', 'glyphicon glyphicon-cog')
                .html(' Table View');

   // Toggle the editor between the table and graph view.
   this.btnToggleView = $('<button />')
        .attr('class', 'btn btn-default btn-step form-control')
        .attr('id', this.ids._btnToggleView)
        .append(this.btnToggleViewSpan)
        .appendTo(formControls);

    // Capture Scenario group
    var captureScenarioForm = $('<div />')
        .attr('class', 'form-inline')
        .appendTo(this.debugModeGroup);
    
    // Input text box to input the vertexId
    this.captureVertexIdInput = $('<input>')
        .attr('type', 'text')
        .attr('class', 'form-control ')
        .attr('placeholder', 'Vertex ID')
        .appendTo(captureScenarioForm);

    // Capture Vertex Scenario Scenario button.
    this.btnCaptureVertexScenario = $('<button>')
        .attr('type', 'button')
        .attr('id', this.ids._btnCaptureVertexScenario)
        .attr('class', 'btn btn-primary form-control')
        .html('Capture Vertex')
        .appendTo(captureScenarioForm);

    // Capture Master
    this.btnCaptureMasterScenario = $('<button>')
        .attr('type', 'button')
        .attr('id', this.ids._btnCaptureMasterScenario)
        .attr('class', 'btn btn-danger form-control')
        .html('Capture Master')
        .appendTo(captureScenarioForm);

    // Initialize handlers for events
    this.initSuperstepControlEvents();
}

/*
 * Initializes the handlers of the elements on superstep controls.
 */
GiraphDebugger.prototype.initSuperstepControlEvents = function() {
    // On clicking Fetch button, send a request to the debugger server
    // Fetch the scenario for this job for superstep -1
    $(this.btnFetchJob).click((function(event) {
        this.currentJobId = $(this.fetchJobIdInput).val();
        this.currentSuperstepNumber = 0;
        this.changeSuperstep(this.currentJobId, this.currentSuperstepNumber);
        this.toggleMode();
    }).bind(this));
    // On clicking the edit mode button, hide the superstep controls and show fetch form.
    $(this.btnEditMode).click((function(event) {
        this.toggleMode();
    }).bind(this));

    // Handle the next and previous buttons on the superstep controls.
    $(this.btnNextStep).click((function(event) {
        this.currentSuperstepNumber += 1;
        this.changeSuperstep(this.currentJobId, this.currentSuperstepNumber);
    }).bind(this));

    $(this.btnPrevStep).click((function(event) {
        this.currentSuperstepNumber -= 1;
        this.changeSuperstep(this.currentJobId, this.currentSuperstepNumber);
    }).bind(this));

    // Handle the capture scenario button the superstep controls.
    $(this.btnCaptureVertexScenario).click((function(event){
        // Get the deferred object.
        var vertexId = $(this.captureVertexIdInput).val();
        Utils.fetchVertexTest(this.debuggerServerRoot, this.currentJobId, 
            this.currentSuperstepNumber, vertexId, 'reg')
        .done((function(response) {
            this.onCaptureVertex.done(response);
        }).bind(this))
        .fail((function(response) {
            this.onCaptureVertex.fail(response.responseText);
        }).bind(this))
    }).bind(this));
    // Handle the master capture scenario button the superstep controls.
    $(this.btnCaptureMasterScenario).click((function(event){
        Utils.fetchMasterTest(this.debuggerServerRoot, this.currentJobId, this.currentSuperstepNumber)
        .done((function(response) {
            this.onCaptureMaster.done(response);
        }).bind(this))
        .fail((function(response) {
            this.onCaptureMaster.fail(response.responseText);
        }).bind(this))
    }).bind(this));

    // Handle the toggle view button.
    $(this.btnToggleView).click((function(event) {
        this.editor.toggleView();
    }).bind(this));

    // Handle the generate sample graph button.
    $(this.btnSampleGraph).click((function(event) { 
        var numVertices = $(this.sampleGraphsInput).val();
        var graphTypeKey = $(this.selectSampleGraphs).val();
        this.editor.buildGraphFromSimpleAdjList(Utils.sampleGraphs[graphTypeKey](numVertices));

        Utils.fetchTestGraph(this.debuggerServerRoot, Utils.getAdjListStrForTestGraph(this.editor.getAdjList()))
        .done((function(response) {
            this.onGenerateTestGraph.done(response);
        }).bind(this))
        .fail((function(response) {
            this.onGenerateTestGraph.fail(response.responseText);
        }).bind(this));
    }).bind(this));
}

/*
 * Fetches the data for this superstep, updates the superstep label, graph editor
 * and disables/enables the prev/next buttons.
 * @param {int} superstepNumber : Superstep to fetch the data for.
 */
GiraphDebugger.prototype.changeSuperstep = function(jobId, superstepNumber) {
    console.log("Changing Superstep to : " + superstepNumber);
    $(this.superstepLabel).html(superstepNumber);
    // Update data of the valpanel
    this.valpanel.setData(jobId, superstepNumber);

    // Fetch the max number of supersteps again. (Online case)
    $.ajax({
            url : this.debuggerServerRoot + "/supersteps",
            data : {'jobId' : this.currentJobId}
    })
    
    .done((function(response) {
        this.maxSuperstepNumber = Math.max.apply(Math, response);
    }).bind(this))
    .fail(function(response) {
    });

    // If scenario is already cached, don't fetch again.
    if (superstepNumber in this.stateCache) {
        this.modifyEditorOnScenario(this.stateCache[superstepNumber]);
    } else {
        // Show preloader while AJAX request is in progress.
        this.editor.showPreloader();
        // Fetch from the debugger server.
        $.ajax({
            url : this.debuggerServerRoot + '/scenario',
            dataType : 'json',
            data: { 'jobId' : jobId, 'superstepId' : superstepNumber }
        })
        .retry({
            times : 5, 
            timeout : 2000,
            retryCallback : function(remainingTimes) {
                // Failed intermediately. Will be retried. 
                noty({text : 'Failed to fetch job. Retrying ' + remainingTimes + ' more times...', type : 'warning', timeout : 1000});
            }
        })
        .done((function(data) {
            console.log(data);
            // Add data to the state cache. 
            // This method will only be called if this superstepNumber was not
            // in the cache already. This method just overwrites without check.
            // If this is the first time the graph is being generated, (count = 1)
            // start from scratch - build from adjList.
            if (Utils.count(this.stateCache) === 1) {
                this.stateCache[superstepNumber] = $.extend({}, data);
                this.editor.buildGraphFromAdjList(data);
            } else {
                // Merge this data onto superstepNumber - 1's data 
                this.stateCache[superstepNumber] = this.mergeStates(this.stateCache[superstepNumber - 1], data);
                this.modifyEditorOnScenario(this.stateCache[superstepNumber]);
            }
        }).bind(this))
        .fail(function(error) {
            noty({text : 'Failed to fetch job. Please check your network and debugger server.', type : 'error'});
        })
        .always((function() {
            // Hide Editor's preloader.
            this.editor.hidePreloader();
        }).bind(this));
    }
    // Superstep changed. Enable/Disable the prev/next buttons.
    $(this.btnNextStep).attr('disabled', superstepNumber === this.maxSuperstepNumber);
    $(this.btnPrevStep).attr('disabled', superstepNumber === this.minSuperstepNumber);
}


/*
 * Modifies the editor for a given scenario.
 */
GiraphDebugger.prototype.modifyEditorOnScenario = function(scenario) {
    console.log(scenario); 
    // Add new nodes/links received in this scenario to graph.
    this.editor.addToGraph(scenario);
    // Disable the nodes that were not traced as part of this scenario.
    for (var i = 0; i < this.editor.nodes.length; i++) {
        var nodeId = this.editor.nodes[i].id;
        if ((nodeId in scenario) && scenario[nodeId].debugged != false) {
            this.editor.nodes[i].enabled = true;
        } else {
            this.editor.nodes[i].enabled = false;
        }
    }
    // Update graph data with this scenario.
    this.editor.updateGraphData(scenario);
}

/*
 * Creates the document elements, like Node Attributes modal.
 */
GiraphDebugger.prototype.initElements = function() {
    // Div for the node attribute modal.
    this.nodeAttrModal = $('<div />')
        .attr('class', 'modal')
        .attr('id', this.ids._nodeAttrModal)
        .hide()

   // Div for edge values modal.
   this.edgeValModal = $('<div />')
       .attr('class', 'modal')
       .attr('id', this.ids._edgeValModal)
       .hide()

   this.edgeValForm = $('<form />')
       .addClass('form-horizontal')
       .appendTo(this.edgeValModal);
       
    // Create a form and append to nodeAttr
    var nodeAttrForm = $('<form />')
        .addClass('form-horizontal')
        .appendTo(this.nodeAttrModal);

    this.initInputElements(nodeAttrForm);
    this.initMessageElements(nodeAttrForm);
    this.initSuperstepControls(this.superstepControlsContainer);

    // Initialize the node attr modal dialong.
    $(this.nodeAttrModal).dialog({
        modal : true,
        autoOpen : false,
        width : 300,
        resizable : false,
        closeOnEscape : true,
        hide : {effect : 'fade', duration : 100},
        close : (function() {
            this.selectedNodeId = null;
        }).bind(this)
    });

    // Initialize the edge values modal dialog.
    $(this.edgeValModal).dialog({
        modal : true,
        autoOpen : false,
        width : 250,
        resizable : false,
        title : 'Edge',
        closeOnEscape : true,
        hide : {effect : 'fade', duration : 100},
        close : (function() {
            this.selectedLink = null;
        }).bind(this)
    });

    // Attach events.
    // Click event of the Sent/Received tab buttons
    $('.nav-msg').click((function(event) {
        // Render the table
        var clickedId = event.target.id;
        this.toggleMessageTabs(clickedId);
        if (clickedId === 'node-attr-sent') {
            var messageData = this.editor.getMessagesSentByNode(this.selectedNodeId);
            this.showMessages(messageData);
        } else if(clickedId === 'node-attr-received') {
            var messageData = this.editor.getMessagesReceivedByNode(this.selectedNodeId);
            this.showMessages(messageData);
        } else {
            this.showEdgeValues(this.selectedNodeId, this.selectedEdgeValues);
        }
    }).bind(this));
    // Attach mouseenter event for valpanel - Preview (Expand to the right)
    $(this.valpanel.container).mouseenter((function(event) {
        if (this.valpanel.state === ValidationPanel.StateEnum.COMPACT) {
            this.valpanel.preview();
        }
    }).bind(this));
    // Attach mouseleave event for valpanel - Compact (Compact to the left)
    $(this.valpanel.container).mouseleave((function(event) {
        // The user must click the close button to compact from the expanded mode.
        if (this.valpanel.state != ValidationPanel.StateEnum.EXPAND) {
            this.valpanel.compact();
        }
    }).bind(this));
}

/* 
 * Handler for opening edge values. 
 * Opens the edge value modal to allow editing/viewing edge values.
 */
GiraphDebugger.prototype.openEdgeVals = function(data) {
    // Set the currently opened link.
    this.selectedLink = data.link;
    $(this.edgeValModal).dialog('option', 'position', [data.event.clientX, data.event.clientY]);
    $(this.edgeValForm).empty();
    // Data for the form.
    var table = $('<table />').addClass('table').appendTo(this.edgeValForm);
    var edges = this.editor.getEdges(data.link);
    // Convert edges array to a map to be able to cache onChange results.
    edgeValuesCache = {};
    $.each(edges, function(i, edge) {
        edgeValuesCache[edge.source.id] = edge;
    });

    $.each(edgeValuesCache, (function(sourceId, edge) {
        var tr = document.createElement('tr');
        var edgeElement = edge.edgeValue ? edge.edgeValue : 'undefined';
        if (!this.editor.readonly) {
            edgeElement = $('<input type="text" />')
                .attr('value', edge.edgeValue)
                .css('width', '100%')
                .attr('placeholder', edge.edgeValue)
                .change(function(event) {
                    // Save the temporarily edited values to show them as such
                    // when this tab is opened again.
                    edgeValuesCache[sourceId].edgeValue = event.target.value;
                });
        }
        $(tr).append($('<td />').html("{0}->{1}".format(edge.source.id, edge.target.id)));
        $(tr).append($('<td />').append(edgeElement));
        table.append(tr);
    }).bind(this));

    Utils.getBtnSubmitSm()
        .attr('type', 'submit')
        .appendTo(this.edgeValForm)
        .click((function(event) {
            event.preventDefault();
            // Save the temporary cache back to the editor object.
            $.each(edgeValuesCache, (function(sourceId, edge) {
                this.editor.addEdge(sourceId, edge.target.id, edge.edgeValue);
            }).bind(this));
            $(this.edgeValModal).dialog('close');
            this.editor.restart();
        }).bind(this));

    Utils.getBtnCancelSm()
        .appendTo(this.edgeValForm)
        .click((function() {
            $(this.edgeValModal).dialog('close');
        }).bind(this));
    $(this.edgeValModal).dialog('open');
    // setTimeout is required because of a Chrome bug - jquery.focus doesn't work expectedly.
    setTimeout((function() { $(this.edgeValModal).find('form input:text').first().focus(); }).bind(this), 1);
    $('.ui-widget-overlay').click((function() { $(Utils.getSelectorForId(this.ids._edgeValModal)).dialog('close'); }).bind(this));
}

/*
 * This is a double-click handler.
 * Called from the editor when a node is double clicked.
 * Opens the node attribute modal with NodeId, Attributes, Messages and Edge Values.
 */
GiraphDebugger.prototype.openNodeAttrs = function(data) {
    // Set the currently double clicked node
    this.selectedNodeId = data.node.id;
    // Store the current edge values for this node in a temporary map.
    // This is used by the Edge Values tab.
    this.selectedEdgeValues = this.editor.getEdgeValuesForNode(this.selectedNodeId);

    $(this.nodeAttrIdInput).attr('value', data.node.id)
        .attr('placeholder', data.node.id);
    $(this.nodeAttrAttrsInput).attr('value', data.node.attrs);
    $(this.nodeAttrGroupError).hide();
    $(this.nodeAttrModal).dialog('option', 'position', [data.event.clientX, data.event.clientY]);
    $(this.nodeAttrModal).dialog('option', 'title', 'Node (ID: ' + data.node.id + ')');
    $(this.nodeAttrModal).dialog('open');
    // Set the focus on the Attributes input field by default.
    $(this.nodeAttrModal).find('form input').eq(1).focus();
    $('.ui-widget-overlay').click((function() { $(Utils.getSelectorForId(this.ids._nodeAttrModal)).dialog('close'); }).bind(this));

    $(this.btnNodeAttrCancel).click((function() {
        $(this.nodeAttrModal).dialog('close');
    }).bind(this));

    $(this.btnNodeAttrSubmit).unbind('click');
    $(this.btnNodeAttrSubmit).click((function(event) {
        event.preventDefault();
        var new_id = $(this.nodeAttrIdInput).val();
        var new_attrs = $(this.nodeAttrAttrsInput).val();
        // Check if this id is already taken.
        if (data.editor.getNodeIndex(new_id) >= 0 && new_id != data.node.id) {
            $(this.nodeAttrGroupError).show();
            return;
        }
        data.node.id = new_id;
        data.node.attrs = new_attrs;
        // Save the stored edge values. If not edited by the user, overwritten by the original values).
        $.each(this.selectedEdgeValues, (function(targetId, edge) {
            // This method is safe - If an edge exists, only overwrites the edge value.
            data.editor.addEdge(this.selectedNodeId, targetId, edge.edgeValue);
        }).bind(this));
        data.editor.restart();
        $(this.nodeAttrModal).dialog('close');
    }).bind(this));

    // Set the 'Received' tab as the active tab and show messages.
    this.toggleMessageTabs('node-attr-received');
    this.showMessages(data.editor.getMessagesReceivedByNode(this.selectedNodeId));
}

/*
 * Makes the clicked message tab active and the other inactive,
 * by setting/removing the 'active' classes on the corresponding elements.
 * @param - Suffix of the clicked element (one of 'sent'/'received')
 */
GiraphDebugger.prototype.toggleMessageTabs = function(clickedId) {
    if (this.currentlyActiveTab) {
        $(this.currentlyActiveTab).parent().removeClass('active');
    }
    this.currentlyActiveTab = $('#' + clickedId);
    $(this.currentlyActiveTab).parent().addClass('active');
}

/*
 * Populates the messages table on the node attr modal with the message data
 * @param messageData - The data of the sent/received messages from/to this node.
 */
GiraphDebugger.prototype.showMessages = function(messageData) {
    this.flowTable.html('');
    for (var nodeId in messageData) {
        var tr = document.createElement('tr');
        $(tr).html('<td>' + nodeId + '</td><td>' +
            messageData[nodeId] + '</td>');
        this.flowTable.append(tr);
    }
}

/*
 * Populates the edge value table on the node attr modal with the edge vaue data.
 * Uses this.selectedEdgeValues and this.selectedNodeId - must be populated before calling this method.
 * Format this.selectedEdgeValues : { targetNodeId : edgeValue }
 */
GiraphDebugger.prototype.showEdgeValues = function() {
    this.flowTable.html('');
    $.each(this.selectedEdgeValues, (function(nodeId, edge) {
        var tr = document.createElement('tr');
        var edgeElement = edge.edgeValue;
        if (!this.editor.readonly) {
            edgeElement = $('<input type="text" />')
                .attr('value', edge.edgeValue)
                .attr('placeholder', edge.edgeValue)
                .change((function(event) {
                    // Save the temporarily edited values to show them as such
                    // when this tab is opened again.
                    this.selectedEdgeValues[nodeId].edgeValue = event.target.value;
                }).bind(this));
        }
        $(tr).append($('<td />').html(nodeId));
        $(tr).append($('<td />').append(edgeElement));
        this.flowTable.append(tr);
    }).bind(this));
}

/*
 * Merges deltaState on baseState. Merge implies ->
 * Keep all the values of baseState but overwrite if deltaState
 * has them too. If deltaState has some vertices not in baseState, add them.
 */
GiraphDebugger.prototype.mergeStates = function(baseState, deltaState) {
    var newState = $.extend(true, {}, baseState);
    // Start with marking all nodes in baseState as not debugged.
    // Only nodes debugged in deltaState will be marked as debugged.
    for (nodeId in baseState) {
        newState[nodeId].debugged = false;    
    }
    for (nodeId in deltaState) {
        // Add this node's properties from deltaState
        newState[nodeId] = $.extend({}, deltaState[nodeId]);
        // If nodeId was in deltaState, mark as debugged.
        newState[nodeId].debugged = true;
    }
    return newState;
}

/*
 * Toggles between the debug and edit modes.
 */
GiraphDebugger.prototype.toggleMode = function() {
    if (this.mode === GiraphDebugger.ModeEnum.DEBUG) {
        this.mode = GiraphDebugger.ModeEnum.EDIT;
        if (this.editor.view != Editor.ViewEnum.GRAPH) {
            this.editor.toggleView();
        }
        // Start with a sample graph as usual.
        this.editor.setReadonly(false);
        this.editor.buildSample();
        // Show Fetch Job and hide controls
        $(this.debugModeGroup).hide();
        $(this.editModeGroup).show();
        // Reset vars
        this.resetVars();
    } else {
        this.mode = GiraphDebugger.ModeEnum.DEBUG;
        // Set the editor in readonly mode.
        this.editor.setReadonly(true);
        // Show Form controls and hide fetch job.
        $(this.debugModeGroup).show();
        $(this.editModeGroup).hide();
    }
}
