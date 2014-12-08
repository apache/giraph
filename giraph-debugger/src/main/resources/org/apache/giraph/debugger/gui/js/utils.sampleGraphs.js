/*
 * Map of sample graphs and their corresponding init handlers.
 * Format is "<human readable graph type>" : function() { }
 * Attach the handler of the same name to Utils.SampleGraphs
 */
Utils.sampleGraphs = {
    "Line Graph" : function(numNodes) {
        var simpleAdjList = { 0 : {} };
        for (var i = 0; i < numNodes - 1; i++) {
            simpleAdjList[i] = [i+1];
        }
        return simpleAdjList;
    },
    "Cycle" : function(numNodes) {
        var simpleAdjList = Utils.sampleGraphs["Line Graph"](numNodes);
        if (numNodes > 1) {
            simpleAdjList[numNodes-1] = [0];
        }
        return simpleAdjList;
    },
    "Vertex Clique" : function(numNodes) {},
    "Tailed Cycle Graph" : function(numNodes) {},
    "Star Graph" : function(numNodes) {},
    "Disconnected Graphs" : function(numNodes) {}
}
