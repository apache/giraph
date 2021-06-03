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
package org.apache.giraph.debugger.gui;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.apache.giraph.debugger.mock.ComputationComputeTestGenerator;
import org.apache.giraph.debugger.mock.MasterComputeTestGenerator;
import org.apache.giraph.debugger.mock.TestGraphGenerator;
import org.apache.giraph.debugger.utils.DebuggerUtils;
import org.apache.giraph.debugger.utils.DebuggerUtils.DebugTrace;
import org.apache.giraph.debugger.utils.GiraphMasterScenarioWrapper;
import org.apache.giraph.debugger.utils.GiraphVertexScenarioWrapper;
import org.apache.giraph.debugger.utils.MsgIntegrityViolationWrapper;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.google.common.collect.Lists;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

/**
 * Entry point to the HTTP Debugger Server.
 */
public class Server {

  /**
   * Logger for the class.
   */
  private static final Logger LOG = Logger.getLogger(Server.class);
  /**
   * Default port number for the server.
   */
  private static final int SERVER_PORT = Integer.parseInt(System.getProperty(
    "giraph.debugger.guiPort", "8000"));

  /**
   * Private constructor to disallow construction outside of the class.
   */
  private Server() { }

  /**
   * @param args command line arguments for the server
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    HttpServer server = HttpServer
      .create(new InetSocketAddress(SERVER_PORT), 0);
    // Attach JobHandler instance to handle /job GET call.
    server.createContext("/vertices", new GetVertices());
    server.createContext("/supersteps", new GetSupersteps());
    server.createContext("/scenario", new GetScenario());
    server.createContext("/integrity", new GetIntegrity());
    server.createContext("/test/vertex", new GetVertexTest());
    server.createContext("/test/master", new GetMasterTest());
    server.createContext("/test/graph", new GetTestGraph());
    server.createContext("/", new GetEditor());
    // Creates a default executor.
    server.setExecutor(null);
    server.start();
  }

  /**
   * Handler when accessing the landing page for the server.
   */
  static class GetEditor implements HttpHandler {

    @Override
    public void handle(HttpExchange t) {
      URI uri = t.getRequestURI();
      try {
        try {
          String path = uri.getPath();
          LOG.debug(path);
          if (path.endsWith("/")) {
            path += "index.html";
          }
          path = path.replaceFirst("^/", "");
          LOG.debug("resource path to look for = " + path);
          LOG.debug("resource URL = " + getClass().getResource(path));
          InputStream fs = getClass().getResourceAsStream(path);
          if (fs == null) {
            // Object does not exist or is not a file: reject
            // with 404 error.
            String response = "404 (Not Found)\n";
            t.sendResponseHeaders(404, response.length());
            OutputStream os = t.getResponseBody();
            os.write(response.getBytes());
            os.close();
          } else {
            // Object exists and is a file: accept with response
            // code 200.
            t.sendResponseHeaders(200, 0);
            OutputStream os = t.getResponseBody();
            final byte[] buffer = new byte[0x10000];
            int count = 0;
            while ((count = fs.read(buffer)) >= 0) {
              os.write(buffer, 0, count);
            }
            fs.close();
            os.close();
          }
        } catch (IOException e) {
          e.printStackTrace();
          t.sendResponseHeaders(404, 0);
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Returns the list of vertices debugged in a given Superstep for a given job.
   *
   * URL parameters: {jobId, superstepId}
   */
  static class GetVertices extends ServerHttpHandler {
    @Override
    public void processRequest(HttpExchange httpExchange,
      Map<String, String> paramMap) {
      String jobId = paramMap.get(ServerUtils.JOB_ID_KEY);
      String superstepId = paramMap.get(ServerUtils.SUPERSTEP_ID_KEY);
      // CHECKSTYLE: stop IllegalCatch
      try {
        // jobId and superstepId are mandatory. Validate.
        if (jobId == null || superstepId == null) {
          throw new IllegalArgumentException("Missing mandatory params.");
        }
        List<String> vertexIds = null;
        // May throw NumberFormatException. Handled below.
        long superstepNo = Long.parseLong(superstepId);
        if (superstepNo < -1) {
          throw new NumberFormatException("Superstep must be integer >= -1.");
        }
        // May throw IOException. Handled below.
        vertexIds = ServerUtils.getVerticesDebugged(jobId, superstepNo,
          DebugTrace.VERTEX_ALL);
        this.statusCode = HttpURLConnection.HTTP_OK;
        // Returns output as an array ["id1", "id2", "id3" .... ]
        this.response = new JSONArray(vertexIds).toString();
      } catch (Exception e) {
        this.handleException(e, String.format(
          "Invalid parameters. %s is a mandatory parameter.",
          ServerUtils.JOB_ID_KEY));
      }
      // CHECKSTYLE: resume IllegalCatch
    }
  }

  /**
   * Returns the number of supersteps traced for the given job.
   */
  static class GetSupersteps extends ServerHttpHandler {
    @Override
    public void processRequest(HttpExchange httpExchange,
      Map<String, String> paramMap) {
      String jobId = paramMap.get(ServerUtils.JOB_ID_KEY);
      // CHECKSTYLE: stop IllegalCatch
      try {
        // jobId and superstepId are mandatory. Validate.
        if (jobId == null) {
          throw new IllegalArgumentException("Missing mandatory params.");
        }
        List<Long> superstepIds = null;
        // May throw IOException. Handled below.
        superstepIds = ServerUtils.getSuperstepsDebugged(jobId);
        this.statusCode = HttpURLConnection.HTTP_OK;
        // Returns output as an array ["id1", "id2", "id3" .... ]
        this.response = new JSONArray(superstepIds).toString();
      } catch (Exception e) {
        this.handleException(e, String.format(
          "Invalid parameters. %s and %s are mandatory parameter.",
          ServerUtils.JOB_ID_KEY, ServerUtils.SUPERSTEP_ID_KEY));
      }
      // CHECKSTYLE: resume IllegalCatch
    }
  }

  /**
   * Returns the scenario for a given superstep of a given job.
   *
   * URL Params: {jobId, superstepId, [vertexId], [raw]}
   * vertexId: vertexId is optional. It can be a single value or a comma
   *       separated list. If it is not supplied, returns the scenario for all
   *       vertices. If 'raw' parameter is specified, returns the raw protocol
   *       buffer.
   */
  static class GetScenario extends ServerHttpHandler {
    @Override
    @SuppressWarnings("rawtypes")
    public void processRequest(HttpExchange httpExchange,
      Map<String, String> paramMap) {
      String jobId = paramMap.get(ServerUtils.JOB_ID_KEY);
      String superstepId = paramMap.get(ServerUtils.SUPERSTEP_ID_KEY);
      // Check both jobId and superstepId are present
      // CHECKSTYLE: stop IllegalCatch
      try {
        if (jobId == null || superstepId == null) {
          throw new IllegalArgumentException("Missing mandatory parameters");
        }
        Long superstepNo = Long.parseLong(paramMap
          .get(ServerUtils.SUPERSTEP_ID_KEY));
        if (superstepNo < -1) {
          this.statusCode = HttpURLConnection.HTTP_BAD_REQUEST;
          this.response = String.format("%s must be an integer >= -1.",
            ServerUtils.SUPERSTEP_ID_KEY);
          return;
        }
        List<String> vertexIds = null;
        // Get the single vertexId or the list of vertexIds (comma-separated).
        String rawVertexIds = paramMap.get(ServerUtils.VERTEX_ID_KEY);
        // No vertex Id supplied. Return scenario for all vertices.
        if (rawVertexIds == null) {
          // Read scenario for all vertices.
          // May throw IOException. Handled below.
          vertexIds = ServerUtils.getVerticesDebugged(jobId, superstepNo,
            DebugTrace.VERTEX_ALL);
        } else {
          // Split the vertices by comma.
          vertexIds = Lists.newArrayList(rawVertexIds.split(","));
        }
        // Send JSON by default.
        JSONObject scenarioObj = new JSONObject();
        for (String vertexId : vertexIds) {
          GiraphVertexScenarioWrapper giraphScenarioWrapper;
          giraphScenarioWrapper = ServerUtils.readScenarioFromTrace(jobId,
            superstepNo, vertexId.trim(), DebugTrace.VERTEX_REGULAR);
          scenarioObj.put(vertexId,
            ServerUtils.scenarioToJSON(giraphScenarioWrapper));
        }
        // Set status as OK and convert JSONObject to string.
        this.statusCode = HttpURLConnection.HTTP_OK;
        this.response = scenarioObj.toString();
      } catch (Exception e) {
        this.handleException(e, String.format(
          "Invalid parameters. %s and %s are mandatory parameter.",
          ServerUtils.JOB_ID_KEY, ServerUtils.SUPERSTEP_ID_KEY));
      }
      // CHECKSTYLE: stop IllegalCatch
    }
  }

  /**
   * Returns the JAVA code for vertex scenario.
   *
   * URL Params: {jobId, superstepId, vertexId, traceType}
   * traceType: Can be one of reg, err, msg or vv
   */
  static class GetVertexTest extends ServerHttpHandler {
    @Override
    @SuppressWarnings("rawtypes")
    public void processRequest(HttpExchange httpExchange,
      Map<String, String> paramMap) {
      String jobId = paramMap.get(ServerUtils.JOB_ID_KEY);
      String superstepId = paramMap.get(ServerUtils.SUPERSTEP_ID_KEY);
      String vertexId = paramMap.get(ServerUtils.VERTEX_ID_KEY);
      String traceType = paramMap.get(ServerUtils.VERTEX_TEST_TRACE_TYPE_KEY);
      // Check both jobId, superstepId and vertexId are present
      try {
        if (jobId == null || superstepId == null || vertexId == null ||
          traceType == null) {
          throw new IllegalArgumentException("Missing mandatory parameters");
        }
        Long superstepNo = Long.parseLong(paramMap
          .get(ServerUtils.SUPERSTEP_ID_KEY));
        if (superstepNo < -1) {
          throw new NumberFormatException();
        }
        DebugTrace debugTrace = DebuggerUtils
          .getVertexDebugTraceForPrefix(traceType);
        // Send JSON by default.
        GiraphVertexScenarioWrapper giraphScenarioWrapper = ServerUtils
          .readScenarioFromTrace(jobId, superstepNo, vertexId.trim(),
            debugTrace);
        ComputationComputeTestGenerator testGenerator =
          new ComputationComputeTestGenerator();
        String testClassName = String.format("%sTest_%s_S%s_V%s",
          giraphScenarioWrapper.getVertexScenarioClassesWrapper()
            .getClassUnderTest().getSimpleName(), jobId, superstepId, vertexId);
        // Set the content-disposition header to force a download with the
        // given filename.
        String filename = String.format("%s.java", testClassName);
        this.setResponseHeader("Content-Disposition",
          String.format("attachment; filename=\"%s\"", filename));
        this.statusCode = HttpURLConnection.HTTP_OK;
        this.responseContentType = MediaType.TEXT_PLAIN;
        this.response = testGenerator
          .generateTest(giraphScenarioWrapper,
            null /* testPackage is optional */, testClassName);
      } catch (Exception e) {
        this.handleException(e, String.format(
          "Invalid parameters. %s, %s and %s are mandatory parameter.",
          ServerUtils.JOB_ID_KEY, ServerUtils.SUPERSTEP_ID_KEY,
          ServerUtils.VERTEX_ID_KEY));
      }
    }
  }

  /**
   * Returns the JAVA code for master scenario.
   *
   * @URLParams : {jobId, superstepId}
   */
  static class GetMasterTest extends ServerHttpHandler {
    @Override
    public void processRequest(HttpExchange httpExchange,
      Map<String, String> paramMap) {
      String jobId = paramMap.get(ServerUtils.JOB_ID_KEY);
      String superstepId = paramMap.get(ServerUtils.SUPERSTEP_ID_KEY);
      // Check both jobId, superstepId and vertexId are present
      try {
        if (jobId == null || superstepId == null) {
          throw new IllegalArgumentException("Missing mandatory parameters");
        }
        Long superstepNo = Long.parseLong(paramMap
          .get(ServerUtils.SUPERSTEP_ID_KEY));
        if (superstepNo < -1) {
          this.statusCode = HttpURLConnection.HTTP_BAD_REQUEST;
          this.response = String.format("%s must be an integer >= -1.",
            ServerUtils.SUPERSTEP_ID_KEY);
          return;
        }
        // Send JSON by default.
        GiraphMasterScenarioWrapper giraphScenarioWrapper = ServerUtils
          .readMasterScenarioFromTrace(jobId, superstepNo,
            DebugTrace.MASTER_ALL);
        MasterComputeTestGenerator masterTestGenerator =
          new MasterComputeTestGenerator();
        // Set the content-disposition header to force a download with the
        // given filename.
        String testClassName = String.format("%sTest_%s_S%s",
          giraphScenarioWrapper.getMasterClassUnderTest()
            .replaceFirst(".*\\.", ""), jobId, superstepId);
        String filename = String.format("%s.java", testClassName);
        this.setResponseHeader("Content-Disposition",
          String.format("attachment; filename=\"%s\"", filename));
        this.statusCode = HttpURLConnection.HTTP_OK;
        this.responseContentType = MediaType.TEXT_PLAIN;
        this.response = masterTestGenerator.generateTest(giraphScenarioWrapper,
          null /* testPackage is optional */, testClassName);
      } catch (Exception e) {
        this.handleException(e, String.format(
          "Invalid parameters. %s and %s are mandatory parameter.",
          ServerUtils.JOB_ID_KEY, ServerUtils.SUPERSTEP_ID_KEY));
      }
    }
  }

  /**
   * Returns the integrity violations based on the requested parameter. The
   * requested parameter (type) may be one of M, E or V.
   *
   * URL Params: jobId, superstepId, violiationType It is an optional parameter
   *            and is only used when violationType = V
   */
  static class GetIntegrity extends ServerHttpHandler {
    /**
     * The server returns only a limited number of msg or vertex value
     * violations. For message violations, it may not put the limit at exactly
     * this number because it reads each violation trace which may include
     * multiple message violations and adds all the violations in the trace to
     * the response. Once the total message violations is over this number it
     * stops reading traces.
     */
    private static final int NUM_VIOLATIONS_THRESHOLD = 50;

    @Override
    @SuppressWarnings("rawtypes")
    public void processRequest(HttpExchange httpExchange,
      Map<String, String> paramMap) {
      String jobId = paramMap.get(ServerUtils.JOB_ID_KEY);
      String superstepId = paramMap.get(ServerUtils.SUPERSTEP_ID_KEY);
      String violationType = paramMap
        .get(ServerUtils.INTEGRITY_VIOLATION_TYPE_KEY);
      try {
        if (jobId == null || superstepId == null || violationType == null) {
          throw new IllegalArgumentException("Missing mandatory parameters");
        }
        Long superstepNo = Long.parseLong(paramMap
          .get(ServerUtils.SUPERSTEP_ID_KEY));
        if (superstepNo < -1) {
          throw new NumberFormatException();
        }
        // JSON object that will be finally returned.
        JSONObject integrityObj = new JSONObject();
        // Message violation
        if (violationType.equals("M")) {
          List<String> taskIds = ServerUtils.getTasksWithIntegrityViolations(
            jobId, superstepNo, DebugTrace.INTEGRITY_MESSAGE_ALL);

          int numViolations = 0;
          for (String taskId : taskIds) {
            MsgIntegrityViolationWrapper msgIntegrityViolationWrapper =
              ServerUtils.readMsgIntegrityViolationFromTrace(jobId, taskId,
                superstepNo);
            integrityObj.put(taskId,
              ServerUtils.msgIntegrityToJson(msgIntegrityViolationWrapper));
            numViolations += msgIntegrityViolationWrapper.numMsgWrappers();
            if (numViolations >= NUM_VIOLATIONS_THRESHOLD) {
              break;
            }
          }
          this.response = integrityObj.toString();
          this.statusCode = HttpURLConnection.HTTP_OK;
        } else if (violationType.equals("V")) {
          List<String> vertexIds = ServerUtils.getVerticesDebugged(jobId,
            superstepNo, DebugTrace.INTEGRITY_VERTEX);
          int numViolations = 0;
          for (String vertexId : vertexIds) {
            GiraphVertexScenarioWrapper giraphVertexScenarioWrapper =
              ServerUtils.readVertexIntegrityViolationFromTrace(jobId,
                superstepNo, vertexId);
            numViolations++;
            integrityObj.put(vertexId,
              ServerUtils.vertexIntegrityToJson(giraphVertexScenarioWrapper));
            if (numViolations >= NUM_VIOLATIONS_THRESHOLD) {
              break;
            }
          }
          this.response = integrityObj.toString();
          this.statusCode = HttpURLConnection.HTTP_OK;
        } else if (violationType.equals("E")) {
          List<String> vertexIds = null;
          // Get the single vertexId or the list of vertexIds (comma-separated).
          String rawVertexIds = paramMap.get(ServerUtils.VERTEX_ID_KEY);
          // No vertex Id supplied. Return exceptions for all vertices.
          if (rawVertexIds == null) {
            // Read exceptions for all vertices.
            vertexIds = ServerUtils.getVerticesDebugged(jobId, superstepNo,
              DebugTrace.VERTEX_EXCEPTION);
          } else {
            // Split the vertices by comma.
            vertexIds = Lists.newArrayList(rawVertexIds.split(","));
          }
          // Send JSON by default.
          JSONObject scenarioObj = new JSONObject();
          for (String vertexId : vertexIds) {
            GiraphVertexScenarioWrapper giraphScenarioWrapper;
            giraphScenarioWrapper = ServerUtils.readScenarioFromTrace(jobId,
              superstepNo, vertexId.trim(), DebugTrace.VERTEX_EXCEPTION);
            scenarioObj.put(vertexId,
              ServerUtils.scenarioToJSON(giraphScenarioWrapper));
          }
          // Set status as OK and convert JSONObject to string.
          this.statusCode = HttpURLConnection.HTTP_OK;
          this.response = scenarioObj.toString();
        }
      } catch (Exception e) {
        this.handleException(e, String.format(
          "Invalid parameters. %s, %s and %s are mandatory parameter.",
          ServerUtils.JOB_ID_KEY, ServerUtils.SUPERSTEP_ID_KEY,
          ServerUtils.INTEGRITY_VIOLATION_TYPE_KEY));
      }
    }
  }

  /**
   * Returns the TestGraph JAVA code.
   *
   * URL Param: adjList - Adjacency list of the graph
   */
  static class GetTestGraph extends ServerHttpHandler {
    @Override
    public void processRequest(HttpExchange httpExchange,
      Map<String, String> paramMap) {
      String adjList = paramMap.get(ServerUtils.ADJLIST_KEY);
      // Check both jobId and superstepId are present
      try {
        if (adjList == null) {
          throw new IllegalArgumentException("Missing mandatory parameters");
        }
        TestGraphGenerator testGraphGenerator = new TestGraphGenerator();
        String testGraph = testGraphGenerator.generate(adjList.split("\n"));
        this.setResponseHeader("Content-Disposition",
          "attachment; filename=graph.java");
        this.statusCode = HttpURLConnection.HTTP_OK;
        this.responseContentType = MediaType.TEXT_PLAIN;
        this.response = testGraph;
      } catch (Exception e) {
        this.handleException(e, String.format(
          "Invalid parameters. %s is mandatory parameter.",
          ServerUtils.ADJLIST_KEY));
      }
    }
  }
}
