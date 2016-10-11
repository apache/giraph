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
/*
 * Graph Editor is based on Directed Graph Editor by rkirsling http://bl.ocks.org/rkirsling/5001347.
 */

/*
 * Editor is a class that encapsulates the graph editing window
 * @param {container, [undirected]} options - Initialize editor with these options.
 * {options.container} - HTML element that contains the editor svg.
 * {options.undirected} - Indicate whether the graph is directed/undirected.
 * @constructor
 */
function Editor(options) {
    this.container = 'body';
    this.undirected = false;
    // Readonly editor does not let users
    // add new nodes/links.
    this.readonly = false;
    // Data for the graph nodes and edges.
    this.defaultColor = '#FFFDDB'
    // Useful options. Not required by the editor class itself.
    this.errorColor = '#FF9494';
    // Maximum number of nodes for which the graph view would be constructed and maintained.
    this.graphViewNodeLimit = 2000;
    // Graph members
    this.nodes = [];
    this.links = [];
    this.messages = [];
    this.currentZoom = { translate : [0,0], scale : 1 };
    // Table members
    // Current scenario (adjList) object as received from the server.
    this.currentScenario = {};
    // aggregators is a collecton of key-value pairs displayed in the top-right corner.
    this.aggregators = {};
    // set graph as the default view
    this.view = Editor.ViewEnum.GRAPH;
    // linkDistance controls the distance between two nodes in the graph.
    this.linkDistance = 150;
    if (options) {
        this.container = options['container'] ? options['container'] : this.container;
        this.undirected = options['undirected'] === true;
        if (options.onOpenNode) {
            this.onOpenNode = options.onOpenNode;
        }
        if (options.onOpenEdge) {
            this.onOpenEdge = options.onOpenEdge;
        }
    }
    this.setSize();
    this.lastKeyDown = -1;
    this.init();
    this.buildSample();
    return this;
}

/*
 * Represents the two views of the editor - tabular and graph
 */
Editor.ViewEnum = {
    TABLET : 'tablet',
    GRAPH : 'graph'
}

Editor.prototype.onToggleView = function(toggleViewHandler) {
    this.onToggleView.done = toggleViewHandler;
}

/*
 * Build a sample graph with three nodes and two edges.
 */
Editor.prototype.buildSample = function() {
    this.empty();
    // Start with a sample graph.
    for(var i = 0; i < 3; i++) {
        this.addNode();
    }
    this.addEdge('1', '2');
    this.addEdge('2', '3');
    this.restartGraph();
}

/*
 * Empties the graph by deleting all nodes and links.
 */
Editor.prototype.empty = function() {
    // NOTE : Don't use this.nodes = [] to empty the array
    // This creates a new reference and messes up this.force.nodes
    this.nodes.length = 0;
    this.links.length = 0;
    this.messages.length = 0;
    this.numNodes = 0;
    this.restartGraph();
}

/*
 * Initializes the SVG elements, force layout and event bindings.
 */
Editor.prototype.init = function() {
    // Initializes the SVG elements.
    this.initElements();
    // Binds events and initializes variables used to track selected nodes/links.
    this.initEvents();
    // Line displayed when dragging an edge off a node
    this.drag_line = this.svg.append('svg:path')
                                 .attr('class', 'link dragline hidden')
                                 .attr('d', 'M0,0L0,0');
    // Handles to link and node element groups.
    var pathContainer = this.svg.append('svg:g')
    this.path = pathContainer.selectAll('path');
    this.pathLabels = pathContainer.selectAll('text');
    this.circle = this.svg.append('svg:g').selectAll('g');
    // Initializes the force layout.
    this.initForce();
    this.restartGraph();
}

/* 
 * Wrapper for restarting both graph and table. Automatically switches to table
 * view if the number of nodes is too large.
 */
Editor.prototype.restart = function() {
    // If numNodes > graphViewLimit, empty the graph and switch
    // to table view.
    if (this.numNodes > this.graphViewNodeLimit) {
        this.empty();
        if (this.view != Editor.ViewEnum.TABLET) {
            this.toggleView();
        }
    }
    this.restartGraph();
    this.restartTable();
}

/*
 * Updates the graph. Called internally on various events.
 * May be called from the client after updating graph properties.
 */
Editor.prototype.restartGraph = function() {
    this.setSize();
    this.restartNodes();
    this.restartLinks();
    this.resizeForce();
    this.restartAggregators();

    // Set the background to light gray if editor is readonly.
    d3.select('.editor').style('background-color', this.readonly ? '#f9f9f9' : 'white');
    this.svgRect.attr('fill', this.readonly ? '#f9f9f9' : 'white')
                    .attr('width', this.width)
                    .attr('height', this.height);

    // Set the graph in motion
    this.force.start();
}

/*
 * Handles mousedown event.
 * Insert a new node if Shift key is not pressed. Otherwise, drag the graph.
 */
Editor.prototype.mousedown = function() {
    if (this.readonly === true) {
        return;
    }
    this.svg.classed('active', true);
    if (d3.event.shiftKey || this.mousedown_node || this.mousedown_link) {
        return;
    }
    // Insert new node at point.
    var point = d3.mouse(d3.event.target),
        node =  this.addNode();
    node.x = point[0];
    node.y = point[1];
    this.restartGraph();
}

/*
 * Returns all the messages sent by node with the given id.
 * Output format: {receiverId: message}
 * @param {string} id
 */
Editor.prototype.getMessagesSentByNode = function(id) {
    var messagesSent = {};
    for (var i = 0; i < this.messages.length; i++) {
        var messageObj = this.messages[i];
        if (messageObj.outgoing === true && messageObj.sender.id === id) {
            messagesSent[messageObj.receiver.id] = messageObj.message;
        }
    }
    return messagesSent;
}

/*
 * Returns all the edge values for this node's neighbor in a JSON object.
 * Note that if an edge value is not present, still returns that neighborId with null/undefined value.
 * Output format: {neighborId: edgeValue}
 * @param {string} id
 */
Editor.prototype.getEdgeValuesForNode = function(id) {
    var edgeValues = {};
    var outgoingEdges = this.getEdgesWithSourceId(id);
    $.each(outgoingEdges, function(i, edge) {
        edgeValues[edge.target.id] = edge;
    });
    return edgeValues;
}

/*
 * Returns all the messages received by node with the given id.
 * Output format: {senderId: message}
 * @param {string} id
 */
Editor.prototype.getMessagesReceivedByNode = function(id) {
    var messagesReceived = {};

    for (var i = 0; i < this.messages.length; i++) {
        var messageObj = this.messages[i];
        if (messageObj.incoming === true && messageObj.receiver.id === id) {
            // Note: This is required because incoming messages do not have a sender as of now.
            var senderId = '<i data-id="' + i + '"></i>';
            messagesReceived[senderId] = messageObj.message;
        }
    }
    return messagesReceived;
}

/*
 * Returns the edge list.
 * Edge list is the representation of the graph as a list of edges.
 * An edge is represented as a vertex pair (u,v).
 */
Editor.prototype.getEdgeList = function() {
    edgeList = '';

    for (var i = 0; i < this.links.length; i++) {
        var sourceId = this.links[i].source.id;
        var targetId = this.links[i].target.id;

        // Right links are source->target.
        // Left links are target->source.
        if (this.links[i].right) {
            edgeList += sourceId + '\t' + targetId + '\n';
        } else {
            edgeList += targetId + '\t' + sourceId + '\n';
        }
    }
    return edgeList;
}

/*
 * Returns the adjacency list.
 * Adj list is the representation of the graph as a list of nodes adjacent to
 * each node.
 */
Editor.prototype.getAdjList = function() {
    adjList = {}
    $.each(this.nodes, (function(i, node) {
        var id = node.id;
        var edges = this.getEdgesWithSourceId(id);
        adjList[id] = {adj : edges, vertexValue : node.attrs}
    }).bind(this));
    return adjList;
}

/*
 * Returns the list of nodes along with their attributes.
 */
Editor.prototype.getNodeList  = function() {
    nodeList = '';
    for (var i = 0; i < this.nodes.length; i++){
        nodeList += this.nodes[i].id + '\t' + this.nodes[i].attrs;
        nodeList += (i != this.nodes.length - 1 ? '\n' : '');
    }
    return nodeList;
}

/*
 * Handle the mousemove event.
 * Updates the drag line if mouse is pressed at present.
 * Ignores otherwise.
 */
Editor.prototype.mousemove = function() {
    if (this.readonly) {
        return;
    }
    // This indicates if the mouse is pressed at present.
    if (!this.mousedown_node) {
        return;
    }
    // Update drag line.
    this.drag_line.attr('d', 'M' + this.mousedown_node.x + ',' +
        this.mousedown_node.y + 'L' + d3.mouse(this.svg[0][0])[0] + ',' +
        d3.mouse(this.svg[0][0])[1]
    );
    this.restartGraph();
}

/*
 * Handles the mouseup event.
 */
Editor.prototype.mouseup = function() {
    if (this.mousedown_node) {
        // hide drag line
        this.drag_line
            .classed('hidden', true)
            .style('marker-end', '');
    }
    this.svg.classed('active', false);
    // Clear mouse event vars
    this.resetMouseVars();
}

/*
 * Handles keydown event.
 * If Key is Shift, drags the graph using the force layout.
 * If Key is 'L' or 'R' and link is selected, orients the link likewise.
 * If Key is 'R' and node is selected, marks the node as reflexive.
 * If Key is 'Delete', deletes the selected node or edge.
 */
Editor.prototype.keydown = function() {
    if (this.lastKeyDown !== -1) {
        return;
    }
    this.lastKeyDown = d3.event.keyCode;

    // Shift key was pressed
    if (d3.event.shiftKey) {
        this.circle.call(this.force.drag);
        this.svg.classed('ctrl', true);
    }

    if (!this.selected_node && !this.selected_link || this.readonly) {
        return;
    }

    switch (d3.event.keyCode) {
        case 46: // delete
            if (this.selected_node) {
                this.nodes.splice(this.nodes.indexOf(this.selected_node), 1);
                this.spliceLinksForNode(this.selected_node);
            } else if (this.selected_link) {
                this.links.splice(this.links.indexOf(this.selected_link), 1);
            }
            this.selected_link = null;
            this.selected_node = null;
            this.restartGraph();
            break;
        case 66: // B
            if (this.selected_link) {
                // set link direction to both left and right
                this.selected_link.left = true;
                this.selected_link.right = true;
            }

            this.restartGraph();
            break;
        case 76: // L
            if (this.selected_link) {
                // set link direction to left only
                this.selected_link.left = true;
                this.selected_link.right = false;
            }

            this.restartGraph();
            break;
        case 82: // R
            if (this.selected_node) {
                // toggle node reflexivity
                this.selected_node.reflexive = !this.selected_node.reflexive;
            } else if (this.selected_link) {
                // set link direction to right only
                this.selected_link.left = false;
                this.selected_link.right = true;
            }

            this.restartGraph();
            break;
    }
}

/*
 * Handles the keyup event.
 * Resets lastKeyDown to -1.
 * Also resets the drag event binding to null if the key released was Shift.
 */
Editor.prototype.keyup = function() {
    this.lastKeyDown = -1;

    // Shift
    if (d3.event.keyCode === 16) {
        this.circle
            .on('mousedown.drag', null)
            .on('touchstart.drag', null);
        this.svg.classed('ctrl', false);
    }
}

/*
 * Builds the graph from adj list by constructing the nodes and links arrays.
 * @param {object} adjList - Adjacency list of the graph. attrs and msgs are optional.
 * Format:
 * {
 *  nodeId: {
 *            neighbors : [{ 
 *                      neighborId: "neighborId1",
 *                      edgeValue: "edgeValue1"
 *                  },
 *                  {
 *                      neighborId: "neighborId2",
 *                      edgeValue: "edgeValue2"
 *                  }],
 *            vertexValue : attrs,
 *            outgoingMessages : {
 *                    receiverId1: "message1",
 *                    receiverId2: "message2",
 *                    ...
 *                  },
 *            incomingMessages : [ "message1", "message2" ]
 *            enabled : true/false
 *          }
 * }
 */
Editor.prototype.buildGraphFromAdjList = function(adjList) {
    this.empty();

    // Scan every node in adj list to build the nodes array.
    for (var nodeId in adjList) {
        var node = this.getNodeWithId(nodeId);
        if (!node) {
            node = this.addNode(nodeId);
        }
        var adj = adjList[nodeId]['neighbors'];
        // For every node in the adj list of this node,
        // add the node to this.nodes and add the edge to this.links
        for (var i = 0; adj && i < adj.length; i++) {
            var adjId = adj[i]['neighborId'].toString();
            var edgeValue = adj[i]['edgeValue'];
            var adjNode = this.getNodeWithId(adjId);
            if (!adjNode) {
                adjNode = this.addNode(adjId);
            }
            // Add the edge.
            this.addEdge(nodeId, adjId, edgeValue);
        }
    }
    this.updateGraphData(adjList);
}

/*
 * Updates scenario properties - node attributes and messages from adj list.
 * @param {object} scenario - scenario has the same format as adjList above,
 * but with 'adj' ignored.
 * **NOTE**: This method assumes the same scenario structure,
 * only updates the node attributes and messages exchanged.
 */
Editor.prototype.updateGraphData = function(scenario) {
    // Cache the scenario object. Used by tabular view.
    this.currentScenario = scenario;
    // Clear the messages array. Unlike other fields, messages is cleared and reloaded for every scenario.
    this.messages.length = 0;

    // Scan every node in adj list to build the nodes array.
    for (var nodeId in scenario) {
        var node = this.getNodeWithId(nodeId);
        if (scenario[nodeId]['vertexValue']) {
            node.attrs = scenario[nodeId]['vertexValue'];
        }
        if (scenario[nodeId].enabled != undefined) {
            node.enabled = scenario[nodeId].enabled;
        }

        var outgoingMessages = scenario[nodeId]['outgoingMessages'];
        var incomingMessages = scenario[nodeId]['incomingMessages'];

        // Build this.messages
        if (outgoingMessages) {
            for(var receiverId in outgoingMessages) {
                this.messages.push({ 
                    sender: node,
                    receiver: this.getNodeWithId(receiverId),
                    message: outgoingMessages[receiverId],
                    outgoing : true
                });
            }
        }

        if (incomingMessages) {
            for (var i = 0; i < incomingMessages.length; i++) {
              var incomingMessage = incomingMessages[i];
              this.messages.push({ 
                  // TODO: sender is not supplied by the server as of now.
                  sender : null, 
                  receiver: node,
                  message: incomingMessage,
                  incoming : true
              });
            }
          }

        // Update aggregators
        // NOTE: Later vertices ovewrite value for a given key
        var aggregators = scenario[nodeId]['aggregators'];
        for (var key in aggregators) {
            this.aggregators[key] = aggregators[key];
        }
    }
    // Restart the graph and table to show new values.
    this.restart();
}

/*
 * Adds new nodes and links to the graph without changing the existing structure.
 * @param {object} - scenario has the same format as above.
 * **NOTE** - This method will add news nodes and links without modifying
 * the existing structure. For instance, if the passed graph object does
 * not have a link, but it already exists in the graph, it will stay.
 */
Editor.prototype.addToGraph = function(scenario) {
    for (var nodeId in scenario) {
        // If this node is not present in the graph. Add it.
        this.addNode(nodeId);
        var neighbors = scenario[nodeId]['neighbors'];
        // For each neighbor, add the edge.
        for (var i = 0 ; i < neighbors.length; i++) {
            var neighborId = neighbors[i]['neighborId'];
            var edgeValue = neighbors[i]['edgeValue'];
            // Add neighbor node if it doesn't exist.
            this.addNode(neighborId);
            // Addes edge, or ignores if already exists.
            this.addEdge(nodeId, neighborId, edgeValue);
        }
    }
}

/*
 * Shows the preloader and hides all other elements.
 */
Editor.prototype.showPreloader = function() {
    this.svg.selectAll('g').transition().style('opacity', 0);
    this.preloader.transition().style('opacity', 1);
}

/*
 * Hides the preloader and shows all other elements.
 */
Editor.prototype.hidePreloader = function() {
    this.svg.selectAll('g').transition().style('opacity', 1);
    this.preloader.transition().style('opacity', 0);
    this.restartGraph();
}

/*
 * Enables the given node. Enabled nodes are shown as opaque.
 */
Editor.prototype.enableNode = function(nodeId) {
    this.getNodeWithId(nodeId).enabled = true;
}

/*
 * Disables the given node. 
 * Disabled nodes are shown as slightly transparent with outgoing messages removed.
 */
Editor.prototype.disableNode = function(nodeId) {
    this.getNodeWithId(nodeId).enabled = false;
    // Remove the outgoing Messages for this node.
    var toSplice = this.messages.filter(function(message) {
        return (message.outgoing === true && message.sender.id === nodeId);
    });

    toSplice.map((function(message) {
        this.messages.splice(this.messages.indexOf(message), 1);
    }).bind(this));
}

/*
 * Colors the given node ids with the given color. Use this method to uncolor 
 * all the nodes (reset to default color) by calling colorNodes([], 'random', true);
 * @param {array} nodeIds - List of node ids.
 * @param {color} color - Color of these nodes.
 * @param {bool} [uncolorRest] - Optional parameter to reset the color of other nodes to default.
 */
Editor.prototype.colorNodes = function(nodeIds, color, uncolorRest) {
    // Set the color property of each node in this array. restart will reflect changes.
    for(var i = 0; i < nodeIds.length; i++) {
        var node = this.getNodeWithId(nodeIds[i]);
        if (node) {
            node.color = color;
        }
    }
    // If uncolorRest is specified
    if (uncolorRest) {
        for (var i = 0; i < this.nodes.length; i++) {
            // Not in nodeIds, uncolor it.
            if ($.inArray(this.nodes[i].id, nodeIds) === -1) {
                this.nodes[i].color = this.defaultColor;
            }
        }
    }
    this.restartGraph();
}

/* 
 * Toggles the two views of the editor by sliding up/down the tablet.
 */
Editor.prototype.toggleView = function() { 
    if (this.view === Editor.ViewEnum.GRAPH) {
        this.view = Editor.ViewEnum.TABLET;
        $(this.tablet[0]).slideDown('slow');
    } else {
        this.view = Editor.ViewEnum.GRAPH;
        $(this.tablet[0]).slideUp('slow');
    }
    // Call the handlers registered for toggleView
    this.onToggleView.done(this.view);
}

/*
 * Creates graph from a simple adj list of the format given below.
 * @param {object} simpleAdjList : A simple adjacency list.
 * Format:
 * {
 *     "vertexId1" : [ "neighborId1", "neighborId2" ...],
 *     "vertexId2" : [ "neighborId1", "neighborId2" ...],
 *     ...
 * }
 */
Editor.prototype.buildGraphFromSimpleAdjList = function(simpleAdjList) {
    var scenario = {};
    $.each(simpleAdjList, function(vertexId, neighbors) {
        scenario[vertexId] = {}
        scenario[vertexId].neighbors = [];
        $.each(neighbors, function(index, neighborId) {
            scenario[vertexId].neighbors.push({ neighborId : neighborId });
        });
    });
    this.buildGraphFromAdjList(scenario);
}
