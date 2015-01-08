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
    "Clique" : function(numNodes) {
        var simpleAdjList = { 0 : {} };
        var list = [];
        for (var i = 0; i < numNodes; i++) {
            list[list.length] = i;
        }
        for (var i = 0; i < numNodes; i++) {
            simpleAdjList[i] = list;
        }
        return simpleAdjList;
     },
    "Star Graph" : function(numNodes) {
        var simpleAdjList = { 0 : {} };    
        for (var i = 1; i < numNodes; i++) {
            simpleAdjList[i] = [0];
        }
        return simpleAdjList;
    },
   
}
