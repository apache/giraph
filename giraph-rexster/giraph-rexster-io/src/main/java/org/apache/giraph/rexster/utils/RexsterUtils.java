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

package org.apache.giraph.rexster.utils;

import static org.apache.giraph.rexster.conf.GiraphRexsterConstants.GIRAPH_REXSTER_GREMLIN_E_SCRIPT;
import static org.apache.giraph.rexster.conf.GiraphRexsterConstants.GIRAPH_REXSTER_GREMLIN_V_SCRIPT;
import static org.apache.giraph.rexster.conf.GiraphRexsterConstants.GIRAPH_REXSTER_HOSTNAME;
import static org.apache.giraph.rexster.conf.GiraphRexsterConstants.GIRAPH_REXSTER_INPUT_GRAPH;
import static org.apache.giraph.rexster.conf.GiraphRexsterConstants.GIRAPH_REXSTER_OUTPUT_GRAPH;
import static org.apache.giraph.rexster.conf.GiraphRexsterConstants.GIRAPH_REXSTER_PASSWORD;
import static org.apache.giraph.rexster.conf.GiraphRexsterConstants.GIRAPH_REXSTER_PORT;
import static org.apache.giraph.rexster.conf.GiraphRexsterConstants.GIRAPH_REXSTER_USERNAME;
import static org.apache.giraph.rexster.conf.GiraphRexsterConstants.GIRAPH_REXSTER_USES_SSL;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.rexster.io.RexsterInputSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

/**
 * Utility functions for the Rexster REST interface
 */
@SuppressWarnings("rawtypes")
public abstract class RexsterUtils {
  /** start object symbol for JSON */
  public static final char KEY_VALUE_SEPARATOR = ':';
  /** start object symbol for JSON */
  public static final char START_OBJECT = '{';
  /** end object symbol for JSON */
  public static final char END_OBJECT = '}';
  /** start array symbol for JSON */
  public static final char START_ARRAY = '[';
  /** end array symbol for JSON */
  public static final char END_ARRAY = ']';
  /** array elements separator symbol for JSON */
  public static final char ARRAY_SEPARATOR = ',';
  /** Class logger. */
  private static final Logger LOG = Logger.getLogger(RexsterUtils.class);

  /**
   * The default constructor is set to be private by default so that the
   * class is not instantiated.
   */
  private RexsterUtils() { /* private constructor */ }

  /**
   * Parse all the vertices from the JSON retreived from Rexster. Inspired
   * by the implementation of the JSONObject class.
   *
   * @param  br           buffer over the HTTP response content
   * @return JSONTokener  tokener over the HTTP JSON. Null in case the results
   *                      array is empty.
   */
  public static JSONTokener parseJSONEnvelope(BufferedReader br)
    throws InterruptedException {

    JSONTokener tokener = null;

    try {
      tokener = new JSONTokener(br);
      /* check that the JSON is well-formed by starting with a '{' */
      if (tokener.nextClean() != START_OBJECT) {
        LOG.error(String.format("A JSONObject text must begin with '%c'",
                  START_OBJECT));
      }

      /* loop on the whole array */
      char c = '\0';
      String key = null;
      for (;;) {
        c = tokener.nextClean();
        switch (c) {
        case 0:
          LOG.error(String.format("A JSONObject text must end with '%c'",
                    END_OBJECT));
          break;
        case END_OBJECT:
          return tokener;
        default:
          tokener.back();
          key = tokener.nextValue().toString();
        }

        c = tokener.nextClean();

        if (c != KEY_VALUE_SEPARATOR) {
          LOG.error(String.format("Expected a %c after a key", c));
        }

        if (key != null && !key.equals("results")) {
          tokener.nextValue();
        } else {
          /* starting array */
          c = tokener.nextClean();
          if (c != START_ARRAY) {
            LOG.error("'results' is expected to be an array");
          }

          /* check if the array is emty. If so, return null to signal that
             no objects are available in the array, otherwise return the
             tokener. */
          c = tokener.nextClean();
          if (c == END_ARRAY) {
            return null;
          } else {
            tokener.back();
            return tokener;
          }
        }

        switch (tokener.nextClean()) {
        case ';':
        case ',':
          if (tokener.nextClean() == '}') {
            return tokener;
          }
          tokener.back();
          break;
        case '}':
          return tokener;
        default:
          LOG.error("Expected a ',' or '}'");
        }
      }

    } catch (JSONException e) {
      LOG.error("Unable to parse the JSON with the vertices.\n" +
                e.getMessage());
      throw new InterruptedException(e.toString());
    }
  }

  /**
   * Splitter used by both Vertex and Edge Input Format.
   *
   * @param  context     The job context
   * @param  estimation  Number of estimated objects
   * @return splits to be generated to read the input
   */
  public static List<InputSplit> getSplits(JobContext context,
    long estimation) throws IOException, InterruptedException {

    int chunks = context.getConfiguration().getInt("mapred.map.tasks", 1);
    long chunkSize = estimation / chunks;
    List<InputSplit> splits = new ArrayList<InputSplit>();

    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("Estimated objects: %d", estimation));
      LOG.debug(String.format("Number of chunks: %d", chunks));
    }

    for (int i = 0; i < chunks; ++i) {
      long start = i * chunkSize;
      long end =
        ((i + 1) == chunks) ? Long.MAX_VALUE : (i * chunkSize) + chunkSize;
      RexsterInputSplit split = new RexsterInputSplit(start, end);
      splits.add(split);

      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format("Chunk: start %d; end %d;", start, end));
        LOG.debug(String.format("Chunk: size %d;", chunkSize));
        LOG.debug(split);
      }
    }

    return splits;
  }

  /**
   * Opens an HTTP connection to the specified Rexster server.
   *
   * @param   conf            giraph configuration
   * @param   start           start index of the Rexster page split
   * @param   end             end index of the Rexster page split
   * @param   type            stream type (vertices or edges) needed for the
   *                          REST Url
   * @param   gremlinScript   gremlin script. If set to null, will be ignored.
   * @return  BufferedReader  the object used to retrieve the HTTP response
   *                          content
   */
  // CHECKSTYLE: stop IllegalCatch
  public static BufferedReader openInputStream(
    ImmutableClassesGiraphConfiguration conf,
    long start, long end, String type, String gremlinScript)
    throws InterruptedException {

    String uriScriptFormat =
      "/graphs/%s/tp/gremlin?script=%s" +
      "&rexster.offset.start=%s&rexster.offset.end=%s";
    String uriFormat =
      "/graphs/%s/tp/giraph/%s/" +
      "?rexster.offset.start=%s&rexster.offset.end=%s";
    String endpoint = GIRAPH_REXSTER_HOSTNAME.get(conf);

    try {
      boolean isSsl = GIRAPH_REXSTER_USES_SSL.get(conf);
      int port = GIRAPH_REXSTER_PORT.get(conf);
      String graph = GIRAPH_REXSTER_INPUT_GRAPH.get(conf);
      URL url;
      if (gremlinScript != null && !gremlinScript.isEmpty()) {
        url = new URL(isSsl ? "https" : "http", endpoint, port,
                      String.format(uriScriptFormat, graph, gremlinScript,
                                    start, end));
      } else {
        url = new URL(isSsl ? "https" : "http", endpoint, port,
                      String.format(uriFormat, graph, type, start, end));
      }

      LOG.info(url);

      String username = GIRAPH_REXSTER_USERNAME.get(conf);
      String password = GIRAPH_REXSTER_PASSWORD.get(conf);
      String auth = getHTTPAuthString(username, password);

      HttpURLConnection connection = createConnection(url, "GET", auth);
      connection.setRequestProperty("Content-Type",
        "application/json; charset=UTF-8");
      connection.setDoInput(true);
      connection.setDoOutput(false);
      RexsterUtils.handleResponse(connection, type);

      InputStream is  = connection.getInputStream();
      InputStreamReader isr = new InputStreamReader(is,
          Charset.forName("UTF-8"));
      return new BufferedReader(isr);
    } catch (Exception e) {
      throw new InterruptedException(e.getMessage());
    }
  }
  // CHECKSTYLE: resume IllegalCatch

  /**
   * Opens an HTTP connection to the specified Rexster server.
   *
   * @param conf giraph configuration
   * @param type either edge or vertex
   * @return  the object used to populate the HTTP response content
   */
  // CHECKSTYLE: stop IllegalCatch
  public static HttpURLConnection openOutputConnection(
    ImmutableClassesGiraphConfiguration conf, String type)
    throws InterruptedException {

    String uriFormat = "/graphs/%s/tp/giraph/%s/";
    String endpoint = GIRAPH_REXSTER_HOSTNAME.get(conf);
    boolean isSsl = GIRAPH_REXSTER_USES_SSL.get(conf);
    int port = GIRAPH_REXSTER_PORT.get(conf);
    String graph = GIRAPH_REXSTER_OUTPUT_GRAPH.get(conf);

    try {
      URL url = new URL(isSsl ? "https" : "http", endpoint, port,
                        String.format(uriFormat, graph, type));
      LOG.info(url);

      String username = GIRAPH_REXSTER_USERNAME.get(conf);
      String password = GIRAPH_REXSTER_PASSWORD.get(conf);
      String auth = getHTTPAuthString(username, password);

      HttpURLConnection connection = createConnection(url, "POST", auth);
      connection.setRequestProperty("Content-Type",
                                    "application/json; cherset=UTF-8");
      connection.setDoInput(true);
      connection.setDoOutput(true);
      return connection;
    } catch (Exception e) {
      throw new InterruptedException(e.getMessage());
    }
  }
  // CHECKSTYLE: resume IllegalCatch

  /**
   * Creates a new HTTP connection to the specified server.
   *
   * @param   url         URI to connec to
   * @param   method      method used for the HTTP request
   * @param   authValue   authetication value if available
   * @return  a new HTTP connection
   */
  private static HttpURLConnection createConnection(final URL url,
    final String method, final String authValue) throws Exception {

    final HttpURLConnection connection =
      (HttpURLConnection) url.openConnection();

    connection.setConnectTimeout(0);
    connection.setReadTimeout(0);
    connection.setRequestMethod(method);
    if (authValue != null) {
      connection.setRequestProperty("Authorization", authValue);
    }

    return connection;
  }

  /**
   * Utility to handle the output response in case of errors.
   *
   * @param conn connection to the Rexster Interface
   * @param type type of data saved (vertices or edges)
   */
  private static void handleResponse(HttpURLConnection conn, String type)
    throws IOException, InterruptedException {

    if (conn.getResponseCode() != 200) {
      InputStream is = conn.getErrorStream();
      BufferedReader rd =
        new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));

      JSONObject obj = new JSONObject(rd);
      StringBuffer sb = new StringBuffer("Error occured while saving " +
                                         type + ";");
      String aux;
      while ((aux = rd.readLine()) != null) {
        sb.append(aux);
      }
      sb.append(obj);

      /*
      try {
        LOG.info("--> " + obj);
        String message = obj.getString("message");
        sb.append(" ");
        sb.append(message);
      } catch (JSONException e) {
        LOG.error("Unable to extract the error message.");
      }
      */
      rd.close();

      throw new InterruptedException(sb.toString());
    }
  }

  /**
   * Specific Rexster utility functions for vertices
   */
  public static class Vertex {
    /**
     * Empty private constructor. This class should not be instantiated.
     */
    private Vertex() { /* private constructor */ }

    /**
     * Opens an HTTP connection to the specified Rexster server for vertices.
     *
     * @param   conf            giraph configuration
     * @param   start           start index of the Rexster page split
     * @param   end             end index of the Rexster page split
     * @return  BufferedReader  the object used to retrieve the HTTP response
     */
    public static BufferedReader openInputStream(
      ImmutableClassesGiraphConfiguration conf, long start, long end)
      throws InterruptedException {

      String gremlinScript = GIRAPH_REXSTER_GREMLIN_V_SCRIPT.get(conf);
      return RexsterUtils.openInputStream(conf, start, end, "vertices",
                                          gremlinScript);
    }

    /**
     * Opens an HTTP connection to the specified Rexster server for vertices.
     *
     * @param conf giraph configuration
     * @return  the object used to populate the HTTP response content
     */
    public static HttpURLConnection openOutputConnection(
      ImmutableClassesGiraphConfiguration conf)
      throws InterruptedException {

      return RexsterUtils.openOutputConnection(conf, "vertices");
    }

    /**
     * Utility to handle the output response in case of errors.
     *
     * @param conn connection to the Rexster Interface
     */
    public static void handleResponse(HttpURLConnection conn)
      throws IOException, InterruptedException {

      RexsterUtils.handleResponse(conn, "vertices");
    }
  }

  /**
   * Specific Rexster utility functions for edges
   */
  public static class Edge {
    /**
     * Empty private constructor. This class should not be instantiated.
     */
    private Edge() { /* private constructor */ }

    /**
     * Opens an HTTP connection to the specified Rexster server for edges.
     *
     * @param   conf            giraph configuration
     * @param   start           start index of the Rexster page split
     * @param   end             end index of the Rexster page split
     * @return  BufferedReader  the object used to retrieve the HTTP response
     */
    public static BufferedReader openInputStream(
      ImmutableClassesGiraphConfiguration conf, long start, long end)
      throws InterruptedException {

      String gremlinScript = GIRAPH_REXSTER_GREMLIN_E_SCRIPT.get(conf);
      return RexsterUtils.openInputStream(conf, start, end, "edges",
                                          gremlinScript);
    }

    /**
     * Opens an HTTP connection to the specified Rexster server for edges.
     *
     * @param conf giraph configuration
     * @return  the object used to populate the HTTP response content
     */
    public static HttpURLConnection openOutputConnection(
      ImmutableClassesGiraphConfiguration conf)
      throws InterruptedException {

      return RexsterUtils.openOutputConnection(conf, "edges");
    }

    /**
     * Utility to handle the output response in case of errors.
     *
     * @param conn connection to the Rexster Interface
     */
    public static void handleResponse(HttpURLConnection conn)
      throws IOException, InterruptedException {

      RexsterUtils.handleResponse(conn, "edges");
    }
  }

  /**
   * Provide the Authentication string used for the HTTP connection with
   * Rexster.
   *
   * @param  username   username to connect to HTTP
   * @param  password   password to connect to HTTP
   * @return String     the authentication string
   */
  private static String getHTTPAuthString(String username,
    String password) {

    if (username.isEmpty()) {
      return null;
    } else {
      return "Basic " +
             Base64.encodeBase64URLSafeString(
               (username + ":" + password).getBytes(Charset.forName("UTF-8")));
    }
  }
}
