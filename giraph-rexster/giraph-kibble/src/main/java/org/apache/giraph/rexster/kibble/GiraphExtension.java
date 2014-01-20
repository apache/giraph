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

package org.apache.giraph.rexster.kibble;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.rexster.RexsterResourceContext;
import com.tinkerpop.rexster.Tokens;
import com.tinkerpop.rexster.extension.AbstractRexsterExtension;
import com.tinkerpop.rexster.extension.ExtensionApi;
import com.tinkerpop.rexster.extension.ExtensionDefinition;
import com.tinkerpop.rexster.extension.ExtensionDescriptor;
import com.tinkerpop.rexster.extension.ExtensionMethod;
import com.tinkerpop.rexster.extension.ExtensionNaming;
import com.tinkerpop.rexster.extension.ExtensionPoint;
import com.tinkerpop.rexster.extension.ExtensionResponse;
import com.tinkerpop.rexster.extension.HttpMethod;
import com.tinkerpop.rexster.extension.RexsterContext;
import com.tinkerpop.rexster.util.ElementHelper;
import com.tinkerpop.rexster.util.RequestObjectHelper;

/**
 * This extension allows batch/transactional operations on a graph.
 */
@SuppressWarnings("rawtypes")
@ExtensionNaming(namespace = GiraphExtension.EXTENSION_NAMESPACE,
                 name = GiraphExtension.EXTENSION_NAME)
public class GiraphExtension extends AbstractRexsterExtension {
  public static final String EXTENSION_NAMESPACE = "tp";
  public static final String EXTENSION_NAME = "giraph";
  public static final String TX_KEY = "tx";
  public static final String VLABEL_KEY = "vlabel";
  public static final String DELAY_KEY = "delay";
  public static final String RETRY_KEY = "retry";
  /* element types */
  private static final int VERTEX = 0;
  private static final int EDGE = 1;
  /* max time that will be waited for retry before giving up (msec.) */
  private static final Logger logger = Logger.getLogger(GiraphExtension.class);
  private int backoffRetry = 0;
  private int backoffDelay = 0;
  private String vlabel = null;
  private String lastInVId = "";
  private Vertex lastInV = null;

  @ExtensionDefinition(extensionPoint = ExtensionPoint.GRAPH,
                       method = HttpMethod.GET,
                       path = "vertices")
  @ExtensionDescriptor(description = "get vertices.",
     api = {
       @ExtensionApi(parameterName = Tokens.REXSTER + "." + Tokens.OFFSET_START,
         description = "start offset."),
       @ExtensionApi(parameterName = Tokens.REXSTER + "." + Tokens.OFFSET_END,
         description = "end offset.")
     })
  public ExtensionResponse getVertices(
    @RexsterContext final RexsterResourceContext context,
    @RexsterContext final Graph graph) {

    long start =
      RequestObjectHelper.getStartOffset(context.getRequestObject());
    long end =
      RequestObjectHelper.getEndOffset(context.getRequestObject());

    try {
      Iterable<Vertex> vertices = graph.getVertices();
      return ExtensionResponse.ok(new IteratorVertex(vertices.iterator(),
        start, end));
    } catch (Exception mqe) {
      logger.error(mqe);
      return ExtensionResponse.error("Error retrieving vertices.",
                                     generateErrorJson());
    }
  }

  @ExtensionDefinition(extensionPoint = ExtensionPoint.GRAPH,
                       method = HttpMethod.GET,
                       path = "edges")
  @ExtensionDescriptor(description = "get edges.",
     api = {
       @ExtensionApi(parameterName = Tokens.REXSTER + "." + Tokens.OFFSET_START,
         description = "start offset."),
       @ExtensionApi(parameterName = Tokens.REXSTER + "." + Tokens.OFFSET_END,
         description = "end offset.")
     })
  public ExtensionResponse getEdges(
    @RexsterContext final RexsterResourceContext context,
    @RexsterContext final Graph graph) {

    long start =
      RequestObjectHelper.getStartOffset(context.getRequestObject());
    long end =
      RequestObjectHelper.getEndOffset(context.getRequestObject());

    try {
      Iterable<Edge> edges = graph.getEdges();
      return ExtensionResponse.ok(new IteratorEdge(edges.iterator(),
        start, end));
    } catch (Exception mqe) {
      logger.error(mqe);
      return ExtensionResponse.error("Error retrieving edges.",
                                     generateErrorJson());
    }
  }

  @ExtensionDefinition(
    extensionPoint = ExtensionPoint.GRAPH,
    method = HttpMethod.DELETE,
    autoCommitTransaction = true)
  @ExtensionDescriptor(description = "delete the graph content.")
  public ExtensionResponse delete(
      @RexsterContext RexsterResourceContext context,
      @RexsterContext Graph graph) {

    /* delete all the content of the graph (all edges and vertices) */
    Iterable<Vertex> vertices = graph.getVertices();
    Iterator<Vertex> it = vertices.iterator();
    while(it.hasNext()) {
      Vertex current = it.next();
      graph.removeVertex(current);
    }

    Map<String, Object> resultMap = new HashMap<String, Object>();
    resultMap.put(Tokens.SUCCESS, true);
    return ExtensionResponse.ok(new JSONObject(resultMap));
  }

  @ExtensionDefinition(
    extensionPoint = ExtensionPoint.GRAPH,
    method = HttpMethod.POST,
    path = "vertices",
    autoCommitTransaction = false)
  @ExtensionDescriptor(description = "add vertices to the graph.")
  public ExtensionResponse postVertices(
    @RexsterContext RexsterResourceContext context,
    @RexsterContext Graph graph) {

    ExtensionResponse response = handlePost(graph, context, VERTEX);

    if (graph instanceof TransactionalGraph) {
      TransactionalGraph tgraph = (TransactionalGraph) graph;
      tgraph.commit();
    }

    return response;
  }

  @ExtensionDefinition(
    extensionPoint = ExtensionPoint.GRAPH,
    method = HttpMethod.POST,
    path = "edges",
    autoCommitTransaction = false)
  @ExtensionDescriptor(description = "add edges to the graph.")
  public ExtensionResponse postEdges(
    @RexsterContext RexsterResourceContext context,
    @RexsterContext Graph graph) {

    return handlePost(graph, context, EDGE);
  }

  public ExtensionResponse handlePost(Graph graph,
    RexsterResourceContext context, int type) {

    JSONObject tx = context.getRequestObject();
    if (tx == null) {
      ExtensionMethod extMethod = context.getExtensionMethod();
      return ExtensionResponse.error("no transaction JSON posted", null,
               Response.Status.BAD_REQUEST.getStatusCode(), null,
               generateErrorJson(extMethod.getExtensionApiAsJson()));
    }

    try {
      vlabel = tx.getString(VLABEL_KEY);
      if (tx.has(DELAY_KEY)) {
        backoffDelay = tx.getInt(DELAY_KEY);
      }
      if (tx.has(RETRY_KEY)) {
        backoffRetry = tx.getInt(RETRY_KEY);
      }
      JSONArray array = tx.optJSONArray(TX_KEY);

      for (int i = 0; i < array.length(); i++) {
        JSONObject element = array.optJSONObject(i);
        createElement(element, graph, type);
      }

      Map<String, Object> resultMap = new HashMap<String, Object>();
      resultMap.put(Tokens.SUCCESS, true);
      resultMap.put("txProcessed", array.length());

      return ExtensionResponse.ok(new JSONObject(resultMap));

    } catch (IllegalArgumentException iae) {
      logger.error(iae);
      ExtensionMethod extMethod = context.getExtensionMethod();
      return ExtensionResponse.error(iae.getMessage(), null,
               Response.Status.BAD_REQUEST.getStatusCode(), null,
               generateErrorJson(extMethod.getExtensionApiAsJson()));
    } catch (Exception ex) {
      logger.error(ex);
      return ExtensionResponse.error("Error executing transaction: " +
               ex.getMessage(), generateErrorJson());
    }
  }

  private void createElement(JSONObject element, Graph graph, int type)
    throws Exception {

    switch (type) {
      case VERTEX:
        String id = element.optString(vlabel);
        Vertex vertex = getVertex(graph, id);
        if (vertex != null) {
          throw new Exception("Vertex with id " + id + " already exists.");
        }
        vertex = graph.addVertex(null);
        vertex.setProperty(vlabel, id);

        accumulateAttributes(vertex, element);
        break;
      case EDGE:
        String inV = getProperty(element, Tokens._IN_V, null);
        String outV = getProperty(element, Tokens._OUT_V, null);
        String label = getProperty(element, Tokens._LABEL, "none");

        if (outV == null || inV == null || outV.isEmpty() || inV.isEmpty()) {
          throw new IllegalArgumentException("an edge must specify a "
                      + Tokens._IN_V + " and " + Tokens._OUT_V);
        }

        // there is no edge but the in/out vertex params and label are present
        // so validate that the vertexes are present before creating the edge
        addEdge(graph, element, outV, inV, label);
        break;
      default:
        throw new Exception("Element type unknown.");
    }
  }

  private void addEdge(Graph graph, JSONObject element, String outV,
    String inV, String label) throws Exception {

    Edge edge;
    Exception prevException = new
      RuntimeException("Exception initialized when trying commit.");
    int retryCount = 0;

    while (retryCount <= backoffRetry) {
      // The first time the attempt is made to save the edge, no back off is
      // needed and so the thread is not put to sleep. Differently, afterwards
      // if needed the delay is exponentially (but randomly) put to sleep.
      if (retryCount > 0) {
        try {
          double delay = backoffDelay * Math.pow(2, retryCount);
          Thread.sleep((long) delay);
        } catch (InterruptedException ie) {
          /* nothing to do */
        }
      }
      retryCount += 1;

      Vertex in = getInputVertex(graph, inV);
      Vertex out = getVertex(graph, outV);
      if (out == null || in == null) {
        throw new Exception("the " + Tokens._IN_V + " or " +
                           Tokens._OUT_V + " vertices could not be found.");
      }

      edge = graph.addEdge(null, out, in, label);
      accumulateAttributes(edge, element);
      if (graph instanceof TransactionalGraph) {
        TransactionalGraph tgraph = (TransactionalGraph) graph;
        try {
          tgraph.commit();
          return;
        } catch (Exception e) {
          tgraph.rollback();
          logger.warn("Exception thrown while saving edge: " + e.toString());
          logger.warn("retry: " + retryCount);
          /* need to work out the Exceptions to handle?! */
          prevException = e;
        }
      } else {
        return;
      }
    }

    throw new RuntimeException(prevException);
  }

  private void accumulateAttributes(Element element, JSONObject obj)
    throws Exception {

    Iterator keys = obj.keys();
    while (keys.hasNext()) {
      String key = keys.next().toString();
      if (!key.startsWith(Tokens.UNDERSCORE)) {
        element.setProperty(key,
          ElementHelper.getTypedPropertyValue(obj.getString(key)));
      }
    }
  }

  /**
   * Utility function to get a property from the JSON object with the specified
   * label as a String value.
   *
   * @param element JSON object
   * @param label label of the element to retrieve
   * @param defaultValue set to a value != null if a default value is needed
   * @return value of the associated label or null
   */
  private String getProperty(JSONObject element, String label,
    String defaultValue) {

    String value = null;
    Object tmp = element.opt(label);
    if (tmp != null) {
      value = tmp.toString();
    }
    if (value == null) {
      value = defaultValue;
    }
    return value;
  }

  private Vertex getInputVertex(Graph graph, String inV) {
    Vertex in = null;
    if (inV.equals(lastInVId)) {
      in = lastInV;
    } else {
      lastInVId = inV;
      in = getVertex(graph, inV);
      lastInV = in;
    }
    return in;
  }

  private Vertex getVertex(Graph graph, String v) {
    Vertex vertex;
    Iterable<Vertex> vertexes = graph.getVertices(vlabel, v);
    Iterator<Vertex> itvertex = vertexes.iterator();
    if (!itvertex.hasNext()) {
      return null;
    }
    vertex = itvertex.next();
    if (itvertex.hasNext()) {
      return null;
    }
    return vertex;
  }
}
