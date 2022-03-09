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
