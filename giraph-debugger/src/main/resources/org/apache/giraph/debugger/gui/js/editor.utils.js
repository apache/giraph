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
 * Sets the size of the graph editing window.
 * The graph is always centered in the container according to these dimensions.
 */
Editor.prototype.setSize = function() {
    this.width = $(this.container).width();
    this.height = $(this.container).height();
}

/*
 * Resize the force layout. The D3 force layout controls the movement of the
 * svg elements within the container.
 */
Editor.prototype.resizeForce = function() {
    this.force.size([this.width, this.height])
              .linkDistance(this.linkDistance)
              .charge(-500 - (this.linkDistance - 150)*2);
}

/*
 * Returns the detailed view of each row in the table view.
 */
Editor.prototype.getRowDetailsHtml = function(row) {
    var outerContainer = $('<div />');
    var navContainer = $('<ul />')
        .attr('class', 'nav nav-tabs')
        .attr('id', 'tablet-nav')
        .appendTo(outerContainer);
    navContainer.append($('<li class="active"><a data-toggle="tab" data-name="outgoingMessages">Outgoing Messages</a></li>'));
    navContainer.append($('<li><a data-toggle="tab" data-name="incomingMessages">Incoming Messages</a></li>'));
    navContainer.append($('<li><a data-toggle="tab" data-name="neighbors">Neighbors</a></li>'));

    var dataContainer = $('<div />')
        .attr('class', 'tablet-data-container')
        .appendTo(outerContainer);

    return {
        'outerContainer' : outerContainer,
        'dataContainer' : dataContainer,
        'navContainer' : navContainer
    };
}

Editor.prototype.initTable = function() {
    var jqueryTableContainer = $(this.tablet[0]);
    var jqueryTable = $('<table id="editor-tablet-table" class="editor-tablet-table table display">' + 
            '<thead><tr><th></th><th>Vertex ID</th><th>Vertex Value</th><th>Outgoing Msgs</th>' + 
            '<th>Incoming Msgs</th><th>Neighbors</th></tr></thead></table>');
    jqueryTableContainer.append(jqueryTable);
    // Define the table schema and initialize DataTable object.
    this.dataTable = $("#editor-tablet-table").DataTable({
        'columns' : [
            {
                'class' : 'tablet-details-control',
                'orderable' : false,
                'data' : null,
                'defaultContent' : ''
            },
            { 'data' : 'vertexId' },
            { 'data' : 'vertexValue' },
            { 'data' : 'outgoingMessages.numOutgoingMessages' },
            { 'data' : 'incomingMessages.numIncomingMessages' },
            { 'data' : 'neighbors.numNeighbors'}
        ]
    });
}

/*
 * Zooms the svg element with the given translate and scale factors.
 * Use translate = [0,0] and scale = 1 for original zoom level (unzoomed).
 */
Editor.prototype.zoomSvg = function(translate, scale) {
    this.currentZoom.translate = translate;
    this.currentZoom.scale = scale;
    this.svg.attr("transform", "translate(" + translate + ")"
        + " scale(" + scale + ")");
}

Editor.prototype.redraw = function() {
    this.zoomSvg(d3.event.translate, d3.event.scale);
}

/*
 * Initializes the SVG element, along with marker and defs.
 */
Editor.prototype.initElements = function() {
    // Create the tabular view and hide it for now.
    this.tablet = d3.select(this.container)
        .insert('div')
        .attr('class', 'editor-tablet')
        .style('display', 'none');

    this.initTable();
    // Creates the main SVG element and appends it to the container as the first child.
    // Set the SVG class to 'editor'.
    this.svgRoot = d3.select(this.container)
                     .insert('svg')
    this.zoomHolder = this.svgRoot
                         .attr('class','editor')
                         .attr('pointer-events', 'all')
                         .append('svg:g');

    this.svg = this.zoomHolder.append('svg:g');
    this.svgRect = this.svg.append('svg:rect')

    // Defines end arrow marker for graph links.
    this.svg.append('svg:defs')
                 .append('svg:marker')
                     .attr('id', 'end-arrow')
                     .attr('viewBox', '0 -5 10 10')
                     .attr('refX', 6)
                     .attr('markerWidth', 3)
                     .attr('markerHeight', 3)
                     .attr('orient', 'auto')
                     .append('svg:path')
                         .attr('d', 'M0,-5L10,0L0,5')
                         .attr('fill', '#000');

    // Defines start arrow marker for graph links.
    this.svgRoot.append('svg:defs')
                .append('svg:marker')
                    .attr('id', 'start-arrow')
                    .attr('viewBox', '0 -5 10 10')
                    .attr('refX', 4)
                    .attr('markerWidth', 3)
                    .attr('markerHeight', 3)
                    .attr('orient', 'auto')
                    .append('svg:path')
                        .attr('d', 'M10,-5L0,0L10,5')
                        .attr('fill', '#000');
    // Append the preloader
    // Dimensions of the image are 128x128
    var preloaderX = this.width / 2 - 64;
    var preloaderY = this.height / 2 - 64;
    this.preloader = this.svgRoot.append('svg:g')
                                 .attr('transform', 'translate(' + preloaderX + ',' + preloaderY + ')')
                                 .attr('opacity', 0);

    this.preloader.append('svg:image')
                      .attr('xlink:href', 'img/preloader.gif')
                      .attr('width', '128')
                      .attr('height', '128');
    this.preloader.append('svg:text')
                      .text('Loading')
                      .attr('x', '40')
                      .attr('y', '128');
    // Aggregators
    this.aggregatorsContainer = this.svg.append('svg:g');
    this.aggregatorsContainer.append('text')
                       .attr('class', 'editor-aggregators-heading')
                       .text('Aggregators');
   // d3 selector for global key-value pairs
   this.globs = this.aggregatorsContainer.append('text').selectAll('tspan');
}

/*
 * Binds the mouse and key events to the appropriate methods.
 */
Editor.prototype.initEvents = function() {
    // Mouse event vars - These variables are set (and reset) when the corresponding event occurs.
    this.selected_node = null;
    this.selected_link = null;
    this.mousedown_link = null;
    this.mousedown_node = null;
    this.mouseup_node = null;

    // Binds mouse down/up/move events on main SVG to appropriate methods.
    // Used to create new nodes, create edges and dragging the graph.
    this.svg.on('mousedown', this.mousedown.bind(this))
            .on('mousemove', this.mousemove.bind(this))
            .on('mouseup', this.mouseup.bind(this));

    // Binds Key down/up events on the window to appropriate methods.
    d3.select(window)
          .on('keydown', this.keydown.bind(this))
          .on('keyup', this.keyup.bind(this));
}

/*
 * Initializes D3 force layout to update node/link location and orientation.
 */
Editor.prototype.initForce = function() {
    this.force = d3.layout.force()
                              .nodes(this.nodes)
                              .links(this.links)
                              .size([this.width, this.height])
                              .linkDistance(this.linkDistance)
                              .charge(-500 )
                              .on('tick', this.tick.bind(this))
}

/*
 * Reset the mouse event variables to null.
 */
Editor.prototype.resetMouseVars = function() {
    this.mousedown_node = null;
    this.mouseup_node = null;
    this.mousedown_link = null;
}

/*
 * Called at a fixed time interval to update the nodes and edge positions.
 * Gives the fluid appearance to the editor.
 */
Editor.prototype.tick = function() {
    // draw directed edges with proper padding from node centers
    this.path.attr('d', function(d) {
        var sourcePadding = getPadding(d.source);
        var targetPadding = getPadding(d.target);

        var deltaX = d.target.x - d.source.x,
                deltaY = d.target.y - d.source.y,
                dist = Math.sqrt(deltaX * deltaX + deltaY * deltaY),
                normX = deltaX / dist,
                normY = deltaY / dist,
                sourcePadding = d.left ? sourcePadding[0] : sourcePadding[1],
                targetPadding = d.right ? targetPadding[0] : targetPadding[1],
                sourceX = d.source.x + (sourcePadding * normX),
                sourceY = d.source.y + (sourcePadding * normY),
                targetX = d.target.x - (targetPadding * normX),
                targetY = d.target.y - (targetPadding * normY);
        return 'M' + sourceX + ',' + sourceY + 'L' + targetX + ',' + targetY;
    });

    this.circle.attr('transform', function(d) {
        return 'translate(' + d.x + ',' + d.y + ')';
    });
}

/*
 * Returns the radius of the node.
 * Radius is not fixed since nodes with longer identifiers need a bigger circle.
 * @param {int} node - Node object whose radius is required.
 */
function getRadius(node) {
    // Radius is detemined by multiplyiing the max of length of node ID
    // and node value (first attribute) by a factor and adding a constant.
    // If node value is not present, only node id length is used.
    return 14 + Math.max(node.id.length, getAttrForDisplay(node.attrs).length) * 3;
}

/*
 * Truncates the attribute value so that it fits propertly on the editor node 
 * without exploding the circle.
 */
function getAttrForDisplay(attr) {
    if (attr && attr.length > 11) {
        return attr.slice(0, 4) + "..." + attr.slice(attr.length - 4);    
    }
    return attr ? attr : '';
}

/*
 * Returns the padding of the node.
 * Padding is used by edges as an offset from the node center.
 * Padding is not fixed since nodes with longer identifiers need bigger circle.
 * @param {int} node - Node object whose padding is required.
 */
function getPadding(node) {
    // Offset is detemined by multiplyiing the max of length of node ID
    // and node value (first attribute) by a factor and adding a constant.
    // If node value is not present, only node id length is used.
    var nodeOffset = Math.max(node.id.length, getAttrForDisplay(node.attrs).length) * 3;
    return [19 + nodeOffset, 12  + nodeOffset];
}

/*
 * Returns a new node object.
 * @param {string} id - Identifier of the node.
 */
Editor.prototype.getNewNode = function(id) {
    return {id : id, reflexive : false, attrs : null, x: Math.random(), y: Math.random(), enabled: true, color: this.defaultColor};
}

/* 
 * Returns a new edge object. 
 * @param {object} source - Object for the source node.
 * @param {object) target - Object for the target node.
 * @param {object} edgeVal - Any edge value object.
 */
Editor.prototype.getNewEdge = function(source, target, edgeValue) {
    return {source: source, target: target, edgeValue: edgeValue};
}

/*
 * Returns a new link (edge) object from the node IDs of the logical edge.
 * @param {string} sourceNodeId - The ID of the source node in the logical edge.
 * @param {string} targetNodeId - The ID of the target node in the logical edge.
 * @param {string} [edgeValue] - Value associated with the edge. Optional parameter.
 * @desc - Logical edge means, "Edge from node with ID x to node with ID y".
 * It implicitly captures the direction. However, the link objects have
 * the 'left' and 'right' properties to denote direction. Also, source strictly < target.
 * Therefore, the source and target may not match that of the logical edge, but the
 * direction will compensate for the mismatch.
 */
Editor.prototype.getNewLink = function(sourceNodeId, targetNodeId, edgeValue) {
    var source, target, direction, leftValue = null, rightValue = null;
    if (sourceNodeId < targetNodeId) {
        source = sourceNodeId;
        target = targetNodeId;
        direction = 'right';
        rightValue = edgeValue;
    } else {
        source = targetNodeId;
        target = sourceNodeId;
        direction = 'left';
        leftValue = edgeValue;
    }
    // Every link has an ID - Added to the SVG element to show edge value as textPath
    if (!this.maxLinkId) {
        this.maxLinkId = 0;
    }
    link = {source : this.getNodeWithId(source), target : this.getNodeWithId(target), 
        id : this.maxLinkId++, leftValue : leftValue, rightValue : rightValue,  left : false, right : false};
    link[direction] = true;
    return link;
}

/*
 * Returns the logical edge(s) from a link object.
 * @param {object} link - Link object.
 * This method is required because a single link object may encode
 * two edges using the left/right attributes.
 */
Editor.prototype.getEdges = function(link) {
    var edges = [];
    if (link.left || this.undirected) {
        edges.push(this.getNewEdge(link.target, link.source, link.leftValue));
    }
    if (link.right || this.undirected) {
        edges.push(this.getNewEdge(link.source, link.target, link.rightValue));
    }
    return edges;
}

/*
 * Adds a new link object to the links array or updates an existing link.
 * @param {string} sourceNodeId - Id of the source node in the logical edge.
 * @param {string} targetNodeid - Id of the target node in the logical edge.
 * @param {string} [edgeValue] - Value associated with the edge. Optional parameter.
 */
Editor.prototype.addEdge = function(sourceNodeId, targetNodeId, edgeValue) {
    // console.log('Adding edge: ' + sourceNodeId + ' -> ' + targetNodeId);
    // Get the new link object.
    var newLink = this.getNewLink(sourceNodeId, targetNodeId, edgeValue);
    // Check if a link with these source and target Ids already exists.
    var existingLink = this.links.filter(function(l) {
        return (l.source === newLink.source && l.target === newLink.target);
    })[0];

    // Add link to graph (update if exists).
    if (existingLink) {
        // Set the existingLink directions to true if either
        // newLink or existingLink denote the edge.
        existingLink.left = existingLink.left || newLink.left;
        existingLink.right = existingLink.right || newLink.right;
        if (edgeValue != undefined) {
            if (sourceNodeId < targetNodeId) {
                existingLink.rightValue = edgeValue;
            } else {
                existingLink.leftValue = edgeValue; 
            }
        }
        return existingLink;
    } else {
        this.links.push(newLink);
        return newLink;
    }
}

/*
 * Adds node with nodeId to the graph (or ignores if already exists).
 * Returns the added (or already existing) node.
 * @param [{string}] nodeId - ID of the node to add. If not provided, adds
 * a new node with a new nodeId.
 * TODO(vikesh): Incremental nodeIds are buggy. May cause conflict. Use unique identifiers.
 */
Editor.prototype.addNode = function(nodeId) {
    if (!nodeId) {
        nodeId = (this.numNodes + 1).toString();
    }
    var newNode = this.getNodeWithId(nodeId);
    if (!newNode) {
        newNode = this.getNewNode(nodeId);
        this.nodes.push(newNode);
        this.numNodes++;
    }
    return newNode;
}

/*
 * Updates existing links and adds new links.
 */
Editor.prototype.restartLinks = function() {
    // path (link) group
    this.path = this.path.data(this.links);
    this.pathLabels = this.pathLabels.data(this.links);

    // Update existing links
    this.path.classed('selected', (function(d) {
        return d === this.selected_link;
    }).bind(this))
             .style('marker-start', (function(d) {
                 return d.left && !this.undirected ? 'url(#start-arrow)' : '';
             }).bind(this))
             .style('marker-end', (function(d) {
                 return d.right && !this.undirected ? 'url(#end-arrow)' : '';
             }).bind(this));

    // Add new links.
    // For each link in the bound data but not in elements group, enter()
    // selection calls everything that follows once.
    // Note that links are stored as source, target where source < target.
    // If the link is from source -> target, it's a 'right' link.
    // If the link is from target -> source, it's a 'left' link.
    // A right link has end marker at the target side.
    // A left link has a start marker at the source side.
    this.path.enter()
                 .append('svg:path')
                     .attr('class', 'link')
                     .attr('id', function(d) { return d.id })
                     .classed('selected', (function(d) {
                         return d === this.selected_link;
                     }).bind(this))
                     .style('marker-start', (function(d) {
                         if(d.left && !this.undirected) {
                             return  'url(#start-arrow)';
                         }
                         return '';
                     }).bind(this))
                     .style('marker-end', (function(d) {
                         if(d.right && !this.undirected) {
                             return 'url(#end-arrow)';
                         }
                         return '';
                     }).bind(this))
                     .on('mousedown', (function(d) {
                         // Select link
                         this.mousedown_link = d;
                         // If edge was selected with shift key, call the openEdge handler and return.
                         if (d3.event.shiftKey) {
                            this.onOpenEdge({ event: d3.event, link: d, editor: this });
                            return; 
                         }
                         if (this.mousedown_link === this.selected_link) {
                             this.selected_link = null;
                         } else {
                             this.selected_link = this.mousedown_link;
                         }
                         this.selected_node = null;
                         this.restartGraph();
                     }).bind(this));
    // Add edge value labels for the new edges.
    // Note that two tspans are required for 
    // left and right links (represented by the same 'link' object)
    var textPaths = this.pathLabels.enter()
        .append('svg:text')
        .append('svg:textPath')
        .attr('xlink:href', function(d) { return '#' + d.id })
        .attr('startOffset', '35%');

    textPaths.append('tspan')
        .attr('dy', -6)
        .attr('data-orientation', 'right')
    textPaths.append('tspan')
        .attr('dy', 20)
        .attr('x', 5)
        .attr('data-orientation', 'left')

    // Update the tspans with the edge value
    this.pathLabels.selectAll('tspan').text(function(d) {
        return $(this).attr('data-orientation') === 'right' 
            ? ( d.right ? d.rightValue : null ) 
            : ( d.left ? d.leftValue : null );
    });
    // Remove old links.
    this.path.exit().remove();
    this.pathLabels.exit().remove();
}

/*
 * Adds new nodes to the graph and binds mouse events.
 * Assumes that the data for this.circle is already set by the caller.
 * Creates 'circle' elements for each new node in this.nodes
 */
Editor.prototype.addNodes = function() {
    // Adds new nodes.
    // The enter() call appends a 'g' element for each node in this.nodes.
    // that is not present in this.circle already.
    var g = this.circle.enter().append('svg:g');

    g.attr('class', 'node-container')
         .append('svg:circle')
         .attr('class', 'node')
         .attr('r', (function(d) {
             return getRadius(d);
         }).bind(this))
         .style('fill', this.defaultColor)
         .style('stroke', '#000000')
         .classed('reflexive', function(d) { return d.reflexive; })
         .on('mouseover', (function(d) {
             if (!this.mousedown_node || d === this.mousedown_node) {
                 return;
             }
             // Enlarge target node.
             d3.select(d3.event.target).attr('transform', 'scale(1.1)');
         }).bind(this))
         .on('mouseout', (function(d) {
             if (!this.mousedown_node || d === this.mousedown_node) {
                 return;
             }
             // Unenlarge target node.
             d3.select(d3.event.target).attr('transform', '');
         }).bind(this))
         .on('mousedown', (function(d) {
             if (d3.event.shiftKey || this.readonly) {
                 return;
             }
             // Select node.
             this.mousedown_node = d;
             if (this.mousedown_node === this.selected_node) {
                 this.selected_node = null;
             } else {
                 this.selected_node = this.mousedown_node;
             }
             this.selected_link = null;
             // Reposition drag line.
             this.drag_line
                    .style('marker-end', 'url(#end-arrow)')
                    .classed('hidden', false)
                    .attr('d', 'M' + this.mousedown_node.x + ',' + this.mousedown_node.y + 'L' + this.mousedown_node.x + ',' + this.mousedown_node.y);
             this.restartGraph();
         }).bind(this))
         .on('mouseup', (function(d) {
             if (!this.mousedown_node) {
                 return;
             }
             this.drag_line
                    .classed('hidden', true)
                    .style('marker-end', '');

             // Check for drag-to-self.
             this.mouseup_node = d;
             if (this.mouseup_node === this.mousedown_node) {
                 this.resetMouseVars();
                 return;
             }

             // Unenlarge target node to default size.
             d3.select(d3.event.target).attr('transform', '');

             // Add link to graph (update if exists).
             var newLink = this.addEdge(this.mousedown_node.id, this.mouseup_node.id);
             this.selected_link = newLink;
             this.restartGraph();
         }).bind(this))
         .on('dblclick', (function(d) {
             if (this.onOpenNode) {
                 this.onOpenNode({ event: d3.event, node: d , editor: this });
                 this.restartGraph();
             }
         }).bind(this));

    // Show node IDs
    g.append('svg:text')
        .attr('x', 0)
        .attr('y', 4)
        .attr('class', 'tid')
}

/*
 * Updates existing nodes and adds new nodes.
 */
Editor.prototype.restartNodes = function() {
    // Set the circle group's data to this.nodes.
    // Note that nodes are identified by id, not their index in the array.
    this.circle = this.circle.data(this.nodes, function(d) { return d.id; });
    // NOTE: addNodes must only be called after .data is set to the latest
    // this.nodes. This is done at the beginning of this method.
    this.addNodes();
    // Update existing nodes (reflexive & selected visual states)
    this.circle.selectAll('circle')
        .style('fill', function(d) { return d.color; })
        .classed('reflexive', function(d) { return d.reflexive; })
        .classed('selected', (function(d) { return d === this.selected_node }).bind(this))
        .attr('r', function(d) { return getRadius(d);  });
    // If node is not enabled, set its opacity to 0.2    
    this.circle.transition().style('opacity', function(d) { return d.enabled === true ? 1 : 0.2; });
    // Update node IDs
    var el = this.circle.selectAll('text').text('');
    el.append('tspan')
          .text(function(d) {
              return d.id;
          })
          .attr('x', 0)
          .attr('dy', function(d) {
              return d.attrs != null && d.attrs.trim() != '' ? '-8' : '0 ';
          })
          .attr('class', 'id');
    // Node value (if present) is added/updated here
    el.append('tspan')
          .text(function(d) {
              return getAttrForDisplay(d.attrs);
          })
          .attr('x', 0)
          .attr('dy', function(d) {
              return d.attrs != null && d.attrs.trim() != '' ? '18' : '0';
          })
          .attr('class', 'vval');
    // remove old nodes
    this.circle.exit().remove();
}

/* 
 * Restarts (refreshes, just using 'restart' for consistency) the aggregators.
 */
Editor.prototype.restartAggregators = function() {
    this.aggregatorsContainer.attr('transform', 'translate(' + (this.width - 250) + ', 25)')
    this.aggregatorsContainer.transition().style('opacity', Utils.count(this.aggregators) > 0 ? 1 : 0);
    // Remove all values
    this.globs = this.globs.data([]);
    this.globs.exit().remove();
    // Convert JSON to array of 2-length arrays for d3
    var data = $.map(this.aggregators, function(value, key) { return [[key, value]]; });
    // Set new values
    this.globs = this.globs.data(data);
    this.globs.enter().append('tspan').classed('editor-aggregators-value', true)
        .attr('dy', '2.0em')
        .attr('x', 0)
        .text(function(d) { return "{0} -> {1}".format(d[0], d[1]); });
}

/*
 * Restarts the table with the latest currentScenario.
 */
Editor.prototype.restartTable = function() { 
    // Remove all rows of the table and add again.
    this.dataTable.rows().remove();

    // Modify the scenario object to suit dataTables format
    for (var nodeId in this.currentScenario) {
        var dataRow = {};
        var scenario = this.currentScenario[nodeId];
        dataRow.vertexId = nodeId;
        dataRow.vertexValue = scenario.vertexValue ? scenario.vertexValue : '-',
        dataRow.outgoingMessages = { 
            numOutgoingMessages : Utils.count(scenario.outgoingMessages), 
            data : scenario.outgoingMessages
        },
        dataRow.incomingMessages = { 
            numIncomingMessages : Utils.count(scenario.incomingMessages), 
            data : scenario.incomingMessages
        },
        dataRow.neighbors = {
            numNeighbors : Utils.count(scenario.neighbors),
            data : scenario.neighbors
        }
        this.dataTable.row.add(dataRow).draw();
    }

    // Bind click event for rows.
    $('#editor-tablet-table td.tablet-details-control').click((function(event) {
        var tr = $(event.target).parents('tr');
        var row = this.dataTable.row(tr);
        if ( row.child.isShown()) {
            // This row is already open - close it.
            row.child.hide();
            tr.removeClass('shown');
        } else {
            // Open this row.
            var rowData = row.data();
            var rowHtml = this.getRowDetailsHtml(rowData);
            var dataContainer = rowHtml.dataContainer;
            row.child(rowHtml.outerContainer).show();
            // Now attach events to the tabs
            // NOTE: MUST attach events after the row.child call. 
            $(rowHtml.navContainer).on('click', 'li a', (function(event) {
                // Check which tab was clicked and populate data accordingly.
                var dataContainer = rowHtml.dataContainer;
                var tabName = $(event.target).data('name');
                // Clear the data container
                $(dataContainer).empty();
                if (tabName === 'outgoingMessages') {
                    var mainTable = $('<table><thead><th>Receiver ID</th><th>Outgoing Message</th></thead></table>')
                        .attr('class', 'table')
                        .appendTo(dataContainer)
                    var outgoingMessages = rowData.outgoingMessages.data;
                    for (var receiverId in outgoingMessages) {
                        $(mainTable).append("<tr><td>{0}</td><td>{1}</td></tr>".format(receiverId, outgoingMessages[receiverId]));
                    }
                    $(mainTable).DataTable();
                } else if (tabName === 'incomingMessages') {
                    var mainTable = $('<table><thead><th>Incoming Message</th></thead></table>')
                        .attr('class', 'table')
                        .appendTo(dataContainer);
                    var incomingMessages = rowData.incomingMessages.data;
                    for (var i = 0; i < incomingMessages.length; i++) {
                        $(mainTable).append("<tr><td>{0}</td></tr>".format(incomingMessages[i]));
                    }
                } else if (tabName === 'neighbors') {
                    var mainTable = $('<table><thead><th>Neighbor ID</th><th>Edge Value</th></thead></table>')
                        .attr('class', 'table')
                        .appendTo(dataContainer)
                    var neighbors = rowData.neighbors.data;
                    console.log(neighbors);
                    for (var i = 0 ; i < neighbors.length; i++) {
                        $(mainTable).append("<tr><td>{0}</td><td>{1}</td></tr>"
                            .format(neighbors[i].neighborId, neighbors[i].edgeValue ? neighbors[i].edgeValue : '-'));
                    }
                    $(mainTable).DataTable();
                }
            }).bind(this));
            // Click the first tab of the navContainer - ul>li>a
            $(rowHtml.navContainer).children(':first').children(':first').click();
            tr.addClass('shown');
        }
    }).bind(this));
}

/*
 * Returns the index of the node with the given id in the nodes array.
 * @param {string} id - The identifier of the node.
 */
Editor.prototype.getNodeIndex = function(id) {
    return this.nodes.map(function(e) { return e.id }).indexOf(id);
}

/*
 * Returns the node object with the given id, null if node is not present.
 * @param {string} id - The identifier of the node.
 */
Editor.prototype.getNodeWithId = function(id) {
    var index = this.getNodeIndex(id);
    return index >= 0 ? this.nodes[index] : null;
}

/*
 * Returns the link objeccts with the given id as the source.
 * Note that source here implies that all these links are outgoing from this node.
 * @param {string} sourceId - The identifier of the source node.
 */
Editor.prototype.getEdgesWithSourceId = function(sourceId) {
   var edges = [];
   $.each(this.links, (function(i, link) {
       $.each(this.getEdges(link), function(index, edge) {
           if (edge.source.id === sourceId) {
               edges.push(edge);
           }
       });
   }).bind(this));
   return edges;
}

/*
 * Returns true if the node with the given ID is present in the graph.
 * @param {string} id - The identifier of the node.
 */
Editor.prototype.containsNode = function(id) {
    return this.getNodeIndex(id) >= 0;
}

/*
 * Removes the links associated with a given node.
 * Used when a node is deleted.
 */
Editor.prototype.spliceLinksForNode = function(node) {
    var toSplice = this.links.filter(function(l) {
        return (l.source === node || l.target === node);
    });

    toSplice.map((function(l) {
        this.links.splice(this.links.indexOf(l), 1);
    }).bind(this));
}

/*
 * Puts the graph in readonly state. 
 */
Editor.prototype.setReadonly = function(_readonly) {
    this.readonly = _readonly;
    if (this.readonly) {
        // Support zooming in readonly mode.
        this.zoomHolder.call(d3.behavior.zoom().on('zoom', this.redraw.bind(this)))
        // Remove double click zoom since we display node attrs on double click.
        this.zoomHolder.on('dblclick.zoom', null);
    } else {
        // Remove zooming in edit mode.
        this.zoomHolder.on('.zoom', null);
        this.zoomSvg([0,0], 1);
    }
}
