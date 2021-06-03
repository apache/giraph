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
 * Utility functions used in other JS files. 
 * Parts of this file are borrowed from others. A comment is placed on top in such cases.
 */
function Utils() {}

/*
 * Counts the number of keys of a JSON object.
 */
Utils.count = function(obj) {
   var count=0;
   for(var prop in obj) {
      if (obj.hasOwnProperty(prop)) {
         ++count;
      }
   }
   return count;
}

/*
 * Format feature for JS strings. 
 * Example - "Hello {0}, {1}".format("World", "Graph")
 * = Hello World, Graph
 */
if (!String.prototype.format) {
  String.prototype.format = function() {
    var args = arguments;
    return this.replace(/{(\d+)}/g, function(match, number) { 
      return typeof args[number] != 'undefined'
        ? args[number]
        : match
      ;
    });
  };
}

/*! 
 *jQuery Ajax Retry - v0.2.4 - 2013-08-16
 * https://github.com/johnkpaul/jquery-ajax-retry
 * Copyright (c) 2013 John Paul; Licensed MIT 
 *
 * NOTE: We are using this code to retry AJAX calls to the debugger server.
 */
(function($) {
  // enhance all ajax requests with our retry API
  $.ajaxPrefilter(function(options, originalOptions, jqXHR){
    jqXHR.retry = function(opts){
      if(opts.timeout){
        this.timeout = opts.timeout;
      }
      if (opts.statusCodes) {
        this.statusCodes = opts.statusCodes;
      }
      if (opts.retryCallback) {
        this.retryCallback = opts.retryCallback;
      }
      return this.pipe(null, pipeFailRetry(this, opts));
    };
  });

  // Generates a fail pipe function that will retry `jqXHR` `times` more times.
  function pipeFailRetry(jqXHR, opts){
    var times = opts.times;
    var timeout = opts.timeout;
    var retryCallback = opts.retryCallback;

    // takes failure data as input, returns a new deferred
    return function(input, status, msg){
      var ajaxOptions = this;
      var output = new $.Deferred();

      // whenever we do make this request, pipe its output to our deferred
      function nextRequest() {
        $.ajax(ajaxOptions)
          .retry({times : times-1, timeout : timeout, retryCallback : retryCallback})
          .pipe(output.resolve, output.reject);
      }

      if (times > 1 && (!jqXHR.statusCodes || $.inArray(input.status, jqXHR.statusCodes) > -1)) {
        if (retryCallback) {
          retryCallback(times - 1);
        }
        // time to make that next request...
        if(jqXHR.timeout !== undefined){
          setTimeout(nextRequest, jqXHR.timeout);
        } else {
          nextRequest();
        }
      } else {
        // no times left, reject our deferred with the current arguments
        output.rejectWith(this, arguments);
      }
      return output;
    };
  }
}(jQuery));

/* 
 * Select all text in a given container.
 * @param elementId : ID of the element with the text.
 */
function selectText(elementId) {
    var doc = document
        , text = doc.getElementById(elementId)
        , range, selection
    ;    
    if (doc.body.createTextRange) { //ms
        range = doc.body.createTextRange();
        range.moveToElementText(text);
        range.select();
    } else if (window.getSelection) { //all others
        selection = window.getSelection();        
        range = doc.createRange();
        range.selectNodeContents(text);
        selection.removeAllRanges();
        selection.addRange(range);
    }
}

/*
 * Hook to force a file download given the contents and the file name.
 */
Utils.downloadFile = function(contents, fileName) {
    var pom = document.createElement('a');
    pom.setAttribute('href', 'data:text/plain;charset=utf-8,' + encodeURIComponent(contents));
    pom.setAttribute('download', fileName);
    pom.click();
}

/* 
 * Utility method to fetch vertex sceario from server.
 */
Utils.fetchVertexTest = function(debuggerServerRoot, jobId, superstepId, vertexId, traceType) {
    var url = debuggerServerRoot + '/test/vertex';
    var params = { 
        jobId : jobId,
        superstepId : superstepId,
        vertexId : vertexId,
        traceType : traceType
    };
    return $.ajax({
        url : url, 
        data : params,
        dataFilter : function(data) {
            return {
                code: data,
                url : "{0}?{1}".format(url, $.param(params))
            };
        }
    });
}

/* 
 * Utility method to fetch master sceario from server.
 */
Utils.fetchMasterTest = function(debuggerServerRoot, jobId, superstepId) {
    var url = debuggerServerRoot + '/test/master';
    var params = {
            jobId: jobId,
            superstepId : superstepId
    };
    return $.ajax({
        url : url,
        data : params,
        dataFilter : function(data) {
            return {
                code: data,
                url : "{0}?{1}".format(url, $.param(params))
            };
        }
    });
}

/*
 * Utility method to fetch the test graph for an adjacency list.
 */
Utils.fetchTestGraph = function(debuggerServerRoot, adjList) {
    var url = debuggerServerRoot + '/test/graph';
    var params = { adjList : adjList };
    return $.ajax({
        url : debuggerServerRoot + '/test/graph',
        data : params, 
        dataFilter : function(data) {
            return {
                code: data,
                url : "{0}?{1}".format(url, $.param(params))
            };
        }
    });
}

/*
 * Converts the adjList object returned by editor to a string representation 
 *
 */
Utils.getAdjListStr = function(editorAdjList) {
    adjList = '';
    $.each(editorAdjList, function(vertexId, obj) {
        adjList += vertexId + '\t';
        $.each(obj.adj, function(i, edge) {
            adjList += edge.target.id + '\t';
        });
        // Remove the last tab
        adjList = adjList.slice(0, -1);
        adjList += '\n';
    });
    // Remove the last newline
    return adjList.slice(0, -1);
}

/*
 * Converts the adjList object returned by editor to a string representation
 * used by the Test Graph debugger server.
 */
Utils.getAdjListStrForTestGraph = function(editorAdjList) {
    adjList = '';
    $.each(editorAdjList, function(vertexId, obj) {
        adjList += "{0}{1} ".format(vertexId, obj.vertexValue ? ":" + obj.vertexValue : "");
        $.each(obj.adj, function(i, edge) {
            var edgeValue = edge.edgeValue;
            adjList += "{0}{1} ".format(edge.target.id, edgeValue ? ":" + edgeValue : "");
        });
        // Remove the last whitespace
        adjList = adjList.slice(0, -1);
        adjList += '\n';
    });
    // Remove the last newline
    return adjList.slice(0, -1);
}

/*
 * Creates and returns a submit button with an OK icon.
 */
Utils.getBtnSubmitSm = function() {
    return $('<button />')
        .attr('type', 'button')
        .addClass('btn btn-primary btn-sm editable-submit')
        .html('<i class="glyphicon glyphicon-ok"></i>')
}

/*
 * Creates and returns a cancel button with REMOVE icon.
 */
Utils.getBtnCancelSm = function() {
    return $('<button />')
        .attr('type', 'button')
        .addClass('btn btn-default btn-sm editable-cancel')
        .html('<i class="glyphicon glyphicon-remove"></i>')
}

/*
 * Returns the jQuery selector for element ID.
 */
Utils.getSelectorForId = function(id) {
    return '#' + id;
}
