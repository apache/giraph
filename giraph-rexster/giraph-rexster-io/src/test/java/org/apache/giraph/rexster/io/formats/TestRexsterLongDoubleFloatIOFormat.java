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

package org.apache.giraph.rexster.io.formats;

import static org.apache.giraph.rexster.conf.GiraphRexsterConstants.GIRAPH_REXSTER_GREMLIN_E_SCRIPT;
import static org.apache.giraph.rexster.conf.GiraphRexsterConstants.GIRAPH_REXSTER_GREMLIN_V_SCRIPT;
import static org.apache.giraph.rexster.conf.GiraphRexsterConstants.GIRAPH_REXSTER_HOSTNAME;
import static org.apache.giraph.rexster.conf.GiraphRexsterConstants.GIRAPH_REXSTER_INPUT_GRAPH;
import static org.apache.giraph.rexster.conf.GiraphRexsterConstants.GIRAPH_REXSTER_OUTPUT_GRAPH;
import static org.apache.giraph.rexster.conf.GiraphRexsterConstants.GIRAPH_REXSTER_PORT;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat;
import org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexOutputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.tinkerpop.rexster.Tokens;
import com.tinkerpop.rexster.protocol.EngineController;
import com.tinkerpop.rexster.server.HttpRexsterServer;
import com.tinkerpop.rexster.server.RexsterApplication;
import com.tinkerpop.rexster.server.RexsterServer;
import com.tinkerpop.rexster.server.XmlRexsterApplication;

/**
 * This test suit is intended to extensively test Rexster I/O Format
 * together with the Kibble for such a goal.
 *
 *
 * Note: this is a very simple test case: load data into rexster
 * and then read it using giraph. And reverse: load data using giraph
 * and then read it using rexster. The graph that is being loaded
 * is always the same, but the output we receive is actually different
 * for different underlying formats. Why? Probably because of some bugs.
 */
public class TestRexsterLongDoubleFloatIOFormat {
  /** temporary directory */
  protected static final String TMP_DIR = "/tmp/";
  /** input JSON extension */
  protected static final String REXSTER_CONF = "rexster.xml";
  /** string databases */
  protected static final String DATABASES[] = { "tgdb", "neodb", "orientdb" };
  /** string database (empty one) */
  protected static final String EMPTYDB = "emptydb";
  /** Rexster server instance */
  protected static RexsterServer server;

  @BeforeClass
  public static void initialSetup() throws Exception {
    //In case there were previous runs that failed
    deleteDbs();
    startRexsterServer();
    insertDbData();
  }

  @AfterClass
  static public void finalTearDown() throws Exception {
    stopRexsterServer();
    deleteDbs();
  }

  @Test
  public void testEmptyDbInput() throws Exception {
    testDbInput(EMPTYDB, true, false);
  }

  @Ignore("Fails due to maven dependecy conflicts.")
  @Test
  public void testEmptyDbInputGremlin() throws Exception {
    testDbInput(EMPTYDB, true, true);
  }

  @Ignore
  @Test
  public void testTgDbInput() throws Exception {
    testDbInput(DATABASES[0], false, false);
  }

  @Ignore("Fails due to maven dependecy conflicts.")
  @Test
  public void testTgDbInputGremlin() throws Exception {
    testDbInput(DATABASES[0], false, true);
  }

  @Test
  public void testNeoDbInput() throws Exception {
    testDbInput(DATABASES[1], false, false);
  }

  @Ignore("Fails due to maven dependecy conflicts.")
  @Test
  public void testNeoDbInputGremlin() throws Exception {
    testDbInput(DATABASES[1], false, true);
  }

  @Test
  public void testOrientDbInput() throws Exception {
    testDbInput(DATABASES[2], false, false);
  }

  @Ignore("Fails due to maven dependecy conflicts.")
  @Test
  public void testOrientDbInputGremlin() throws Exception {
    testDbInput(DATABASES[2], false, true);
  }

  @Ignore
  @Test
  public void testTgDbOutput() throws Exception {
    testDbOutput("empty" + DATABASES[0]);
  }

  @Ignore
  @Test
  public void testNeoDbOutput() throws Exception {
    testDbOutput("empty" + DATABASES[1]);
  }

  @Ignore
  @Test
  public void testOrientDbOutput() throws Exception {
    testDbOutput("empty" + DATABASES[2]);
  }

  private void testDbInput(String name, boolean isEmpty, boolean isGramlin)
    throws Exception {
    GiraphConfiguration conf = new GiraphConfiguration();
    GIRAPH_REXSTER_HOSTNAME.set(conf, "127.0.0.1");
    GIRAPH_REXSTER_PORT.set(conf, 18182);
    GIRAPH_REXSTER_INPUT_GRAPH.set(conf, name);
    if (isGramlin) {
      GIRAPH_REXSTER_GREMLIN_V_SCRIPT.set(conf, "g.V");
      GIRAPH_REXSTER_GREMLIN_E_SCRIPT.set(conf, "g.E");
    }
    conf.setComputationClass(EmptyComputation.class);
    conf.setVertexInputFormatClass(
        RexsterLongDoubleFloatVertexInputFormat.class);
    conf.setEdgeInputFormatClass(RexsterLongFloatEdgeInputFormat.class);
    conf.setVertexOutputFormatClass(
        JsonLongDoubleFloatDoubleVertexOutputFormat.class);

    Iterable<String> results = InternalVertexRunner.run(conf, new String[0],
      new String[0]);

    if (isEmpty) {
      boolean empty = false;
      if (results != null) {
        Iterator<String> it = results.iterator();
        empty = !it.hasNext();
      } else {
        empty = true;
      }
      Assert.assertTrue(empty);
      return;
    } else {
      Assert.assertNotNull(results);
    }

    URL url = this.getClass().getResource(name + "-output.json");
    File file = new File(url.toURI());
    ArrayList<Element> expected =
      convertIterator(Files.readLines(file,Charsets.UTF_8).iterator());
    ArrayList<Element> result = convertIterator(results.iterator());
    checkResult(expected, result);
  }

  private void testDbOutput(String name) throws Exception {
    GiraphConfiguration conf = new GiraphConfiguration();
    GIRAPH_REXSTER_HOSTNAME.set(conf, "127.0.0.1");
    GIRAPH_REXSTER_PORT.set(conf, 18182);
    GIRAPH_REXSTER_OUTPUT_GRAPH.set(conf, name);
    conf.setComputationClass(EmptyComputation.class);
    conf.setVertexInputFormatClass(
      JsonLongDoubleFloatDoubleVertexInputFormat.class);
    conf.setVertexOutputFormatClass(
      RexsterLongDoubleFloatVertexOutputFormat.class);
    conf.setEdgeOutputFormatClass(
      RexsterLongDoubleFloatEdgeOutputFormat.class);

    /* graph used for testing */
    String[] graph = new String[] {
      "[1,0,[[2,1],[4,3]]]",
      "[2,0,[[1,1],[3,2],[4,1]]]",
      "[3,0,[[2,2]]]",
      "[4,0,[[1,3],[5,4],[2,1]]]",
      "[5,0,[[3,4],[4,4]]]"
    };

    InternalVertexRunner.run(conf, graph);
    URL url = this.getClass().getResource(name + "-output.json");
    File file = new File(url.toURI());
    ArrayList<Element> expected =
      convertIterator(Files.readLines(file,Charsets.UTF_8).iterator());
    ArrayList<Element> result = getRexsterContent(name);
    checkResult(expected, result);
  }

  /**
   * Test compute method that sends each edge a notification of its parents.
   * The test set only has a 1-1 parent-to-child ratio for this unit test.
   */
  public static class EmptyComputation
    extends BasicComputation<LongWritable, DoubleWritable,
              FloatWritable, LongWritable> {
    @Override
    public void compute(
      Vertex<LongWritable, DoubleWritable, FloatWritable> vertex,
      Iterable<LongWritable> messages) throws IOException {

      vertex.voteToHalt();
    }
  }

  /**
   * Start the Rexster server by preparing the configuration file loaded via
   * the resources and setting other important parameters.
   */
  @SuppressWarnings("unchecked")
  private static void startRexsterServer() throws Exception {
    InputStream rexsterConf =
      TestRexsterLongDoubleFloatIOFormat.class.getResourceAsStream(
        REXSTER_CONF);
    XMLConfiguration properties = new XMLConfiguration();
    properties.load(rexsterConf);
    rexsterConf.close();

    List<HierarchicalConfiguration> graphConfigs =
      properties.configurationsAt(Tokens.REXSTER_GRAPH_PATH);
    RexsterApplication application  = new XmlRexsterApplication(graphConfigs);
    server = new HttpRexsterServer(properties);

    int scriptEngineThreshold =
      properties.getInt("script-engine-reset-threshold",
                        EngineController.RESET_NEVER);
    String scriptEngineInitFile =
      properties.getString("script-engine-init", "");

    /* allow scriptengines to be configured so that folks can drop in
       different gremlin flavors. */
    List<String> scriptEngineNames = properties.getList("script-engines");

    if (scriptEngineNames == null) {
      /* configure to default with gremlin-groovy */
      EngineController.configure(scriptEngineThreshold, scriptEngineInitFile);
    } else {
      EngineController.configure(scriptEngineThreshold, scriptEngineInitFile,
                                 new HashSet<String>(scriptEngineNames));
    }
    server.start(application);
  }

  private static void stopRexsterServer() throws Exception {
    try {
      server.stop();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static void deleteDbs() throws Exception {
    for (int i = 0; i < DATABASES.length; ++i) {
      FileUtils.deleteDirectory(new File(TMP_DIR + DATABASES[i]));
      FileUtils.deleteDirectory(new File(TMP_DIR + "empty" + DATABASES[i]));
    }
    FileUtils.deleteDirectory(new File(TMP_DIR + EMPTYDB));
  }

  private static void insertDbData() throws Exception {
    for (int i = 0; i < DATABASES.length; ++i) {
      URL obj = new URL("http://127.0.0.1:18182/graphs/" + DATABASES[i] +
        "/tp/giraph/vertices");
      HttpURLConnection conn = (HttpURLConnection) obj.openConnection();
      conn.setRequestMethod("POST");
      conn.setRequestProperty("Accept", "*/*");
      conn.setRequestProperty("Content-Type",
          "application/json; charset=UTF-8");
      conn.setDoOutput(true);
      DataOutputStream wr = new DataOutputStream(conn.getOutputStream());
      /* write the JSON to be sent */
      wr.writeBytes("{ \"vlabel\":\"_vid\", \"tx\":[ ");
      wr.writeBytes("{ \"value\":0,\"_vid\":0 },");
      wr.writeBytes("{ \"value\":0,\"_vid\":1 },");
      wr.writeBytes("{ \"value\":0,\"_vid\":2 },");
      wr.writeBytes("{ \"value\":0,\"_vid\":3 },");
      wr.writeBytes("{ \"value\":0,\"_vid\":4 }");
      wr.writeBytes(" ] }");
      int responseCode = conn.getResponseCode();
      if (responseCode != 200) {
        throw new RuntimeException("Unable to insert data in " + DATABASES[i] +
          " code: " + responseCode + "\nresponse: " + conn.getResponseMessage());
      }
      BufferedReader in = new BufferedReader(
          new InputStreamReader(conn.getInputStream()));
      String inputLine;
      StringBuffer response = new StringBuffer();
      while ((inputLine = in.readLine()) != null) {
        response.append(inputLine);
      }
      in.close();

      obj = new URL("http://127.0.0.1:18182/graphs/" + DATABASES[i] +
        "/tp/giraph/edges");
      conn = (HttpURLConnection) obj.openConnection();
      conn.setRequestMethod("POST");
      conn.setRequestProperty("Accept", "*/*");
      conn.setRequestProperty("Content-Type",
          "application/json; charset=UTF-8");
      conn.setDoOutput(true);
      wr = new DataOutputStream(conn.getOutputStream());
      /* write the JSON to be sent */
      wr.writeBytes("{ \"vlabel\":\"_vid\", \"tx\":[ ");
      wr.writeBytes("{ \"value\": 1, \"_outV\": 0, \"_inV\": 1 },");
      wr.writeBytes("{ \"value\": 3, \"_outV\": 0, \"_inV\": 3 },");
      wr.writeBytes("{ \"value\": 1, \"_outV\": 1, \"_inV\": 0 },");
      wr.writeBytes("{ \"value\": 2, \"_outV\": 1, \"_inV\": 2 },");
      wr.writeBytes("{ \"value\": 1, \"_outV\": 1, \"_inV\": 3 },");
      wr.writeBytes("{ \"value\": 5, \"_outV\": 2, \"_inV\": 1 },");
      wr.writeBytes("{ \"value\": 4, \"_outV\": 2, \"_inV\": 4 },");
      wr.writeBytes("{ \"value\": 3, \"_outV\": 3, \"_inV\": 0 },");
      wr.writeBytes("{ \"value\": 1, \"_outV\": 3, \"_inV\": 1 },");
      wr.writeBytes("{ \"value\": 4, \"_outV\": 3, \"_inV\": 4 },");
      wr.writeBytes("{ \"value\": 4, \"_outV\": 4, \"_inV\": 3 },");
      wr.writeBytes("{ \"value\": 4, \"_outV\": 4, \"_inV\": 2 }");
      wr.writeBytes(" ] }");
      wr.flush();
      wr.close();
      responseCode = conn.getResponseCode();
      if (responseCode != 200) {
        throw new RuntimeException("Unable to insert data in " + DATABASES[i] +
          " code: " + responseCode + "\nresponse: " + conn.getResponseMessage());
      }
      in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
      response = new StringBuffer();
      while ((inputLine = in.readLine()) != null) {
        response.append(inputLine);
      }
      in.close();
    }
  }

  private ArrayList<Element> convertIterator(Iterator<String> elementit)
    throws JSONException {
    ArrayList<Element> result = new ArrayList<Element>();
    while(elementit.hasNext()) {
      JSONArray vertex = new JSONArray(elementit.next());
      Element element = new Element(vertex.getLong(0), vertex.getLong(1));
      JSONArray edges = vertex.getJSONArray(2);
      for (int i = 0; i < edges.length(); ++i) {
        element.add(edges.getJSONArray(i).toString());
      }
      result.add(element);
    }
    return result;
  }

  private ArrayList<Element> getRexsterContent(String name) throws Exception {
    ArrayList<Element> result = new ArrayList<Element>();
    /* get all the vertices */
    URL url = new URL("http://127.0.0.1:18182/graphs/" + name +
      "/tp/giraph/vertices");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("Content-Type", "application/json; charset=UTF-8");

    InputStream is = conn.getInputStream();
    StringBuffer json = new StringBuffer();
    BufferedReader br = new BufferedReader(new InputStreamReader(is));
    while (br.ready()) {
      json.append(br.readLine());
    }
    br.close();
    is.close();

    JSONObject results = new JSONObject(json.toString());
    JSONArray vertices = results.getJSONArray("results");
    for (int i = 0; i < vertices.length(); ++i) {
      JSONObject vertex = vertices.getJSONObject(i);
      long id = getId(vertex, "_id");
      result.add(new Element(id, 0));
    }

    /* get all the edges */
    url = new URL("http://127.0.0.1:18182/graphs/" + name + "/tp/giraph/edges");
    conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("Content-Type", "application/json; charset=UTF-8");

    is = conn.getInputStream();
    json = new StringBuffer();
    br = new BufferedReader(new InputStreamReader(is));
    while (br.ready()) {
      json.append(br.readLine());
    }
    br.close();
    is.close();
    results = new JSONObject(json.toString());
    JSONArray edges = results.getJSONArray("results");
    for (int i = 0; i < edges.length(); ++i) {
      JSONObject edge = edges.getJSONObject(i);
      long inV = getId(edge, "_inV");
      long outV = getId(edge, "_outV");
      long value = edge.getLong("value");

      for (int j = 0; j < result.size(); ++j) {
        Element element = result.get(j);
        if (element.id == outV) {
          element.add("[" + inV + "," + value + "]");
        }
      }
    }
    return result;
  }

  private long getId(JSONObject obj, String label) throws Exception {
    long id = 0;
    try {
      id = obj.getLong(label);
    } catch(JSONException e) {
      String idString = obj.getString(label);
      String[] splits = idString.split(":");
      id = Integer.parseInt(splits[1]);
    }
    return id;
  }

  protected void checkResult(ArrayList<Element> expected,
    ArrayList<Element> result) throws Exception {
    for (int i = 0; i < expected.size(); ++i) {
      boolean found = false;
      for (int j = 0; j < result.size(); ++j) {
        if (expected.get(i).equals(result.get(j))) {
          found = true;
        }
      }
      Assert.assertTrue("expected: " + expected + " result: " + result, found);
    }
  }

  protected static class Element {
    public long id;
    public long value;
    public ArrayList<String> edges;

    public Element(long id, long value) {
      this.id = id;
      this.value = value;
      this.edges = new ArrayList<String>();
    }

    public void add(String edge) {
      edges.add(edge);
    }

    public boolean equals(Element obj) {
      if (id != obj.id || value != obj.value) {
        return false;
      }
      for (int i = 0; i < edges.size(); ++i) {
        boolean found = false;
        for (int j = 0; j < obj.edges.size(); ++j) {
          if (edges.get(i).equals(obj.edges.get(j))) {
            found = true;
          }
        }
        if (found == false) {
          return false;
        }
      }

      return true;
    }

    public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append("id: ");
      sb.append(id);
      sb.append(" value: ");
      sb.append(value);
      sb.append(" edges: ");
      for (String element : edges) {
        sb.append(element + " ");
      }
      return  sb.toString();
    }
  }
}
