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

package org.apache.giraph.rexster.io;

import static org.apache.giraph.rexster.conf.GiraphRexsterConstants.GIRAPH_REXSTER_OUTPUT_E_TXSIZE;
import static org.apache.giraph.rexster.conf.GiraphRexsterConstants.GIRAPH_REXSTER_VLABEL;
import static org.apache.giraph.rexster.conf.GiraphRexsterConstants.GIRAPH_REXSTER_HOSTNAME;
import static org.apache.giraph.rexster.conf.GiraphRexsterConstants.GIRAPH_REXSTER_BACKOFF_DELAY;
import static org.apache.giraph.rexster.conf.GiraphRexsterConstants.GIRAPH_REXSTER_BACKOFF_RETRY;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.nio.charset.Charset;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.io.EdgeOutputFormat;
import org.apache.giraph.io.EdgeWriter;
import org.apache.giraph.rexster.utils.RexsterUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Abstract class that users should subclass to use their own Rexster based
 * edge output format.
 *
 * @param <I>
 * @param <V>
 * @param <E>
 */
@SuppressWarnings("rawtypes")
public class RexsterEdgeOutputFormat<I extends WritableComparable,
  V extends Writable, E extends Writable>
  extends EdgeOutputFormat<I, V, E> {

  /** Class logger. */
  private static final Logger LOG =
      Logger.getLogger(RexsterEdgeOutputFormat.class);

  @Override
  public RexsterEdgeWriter
  createEdgeWriter(TaskAttemptContext context)
    throws IOException, InterruptedException {

    return new RexsterEdgeWriter();
  }

  @Override
  public void checkOutputSpecs(JobContext context)
    throws IOException, InterruptedException {
    GiraphConfiguration gconf =
      new GiraphConfiguration(context.getConfiguration());
    String msg = "Rexster OutputFormat usage requires both Edge and " +
                 "Vertex OutputFormat's.";

    if (!gconf.hasVertexOutputFormat()) {
      LOG.error(msg);
      throw new InterruptedException(msg);
    }

    String endpoint = GIRAPH_REXSTER_HOSTNAME.get(gconf);
    if (endpoint == null) {
      throw new InterruptedException(GIRAPH_REXSTER_HOSTNAME.getKey() +
                                     " is a mandatory parameter.");
    }
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
    throws IOException, InterruptedException {

    return new NullOutputCommitter();
  }

  /**
   * Empty output commiter for hadoop.
   */
  private static class NullOutputCommitter extends OutputCommitter {
    @Override
    public void abortTask(TaskAttemptContext taskContext) { }

    @Override
    public void cleanupJob(JobContext jobContext) { }

    @Override
    public void commitTask(TaskAttemptContext taskContext) { }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskContext) {
      return false;
    }

    @Override
    public void setupJob(JobContext jobContext) { }

    @Override
    public void setupTask(TaskAttemptContext taskContext) { }
  }

  /**
   * Abstract class to be implemented by the user based on their specific
   * vertex/edges output. Easiest to ignore the key value separator and only
   * use key instead.
   */
  protected class RexsterEdgeWriter extends EdgeWriter<I, V, E> {
    /** array key that points to the edges and vertices */
    private static final String JSON_ARRAY_KEY = "tx";
    /** Connection to the HTTP REST endpoint */
    private HttpURLConnection rexsterConn;
    /** Output stream from the HTTP connection to the REST endpoint */
    private BufferedWriter rexsterBufferedStream;
    /** attribute used to keep the state of the element array status */
    private boolean isFirstElement = true;
    /** number of vertices before starting a new connection */
    private int txsize;
    /** number of vertexes of vertices sent */
    private int txcounter = 0;
    /** label of the vertex id field */
    private String vlabel;
    /** Back-off time delay in milliseconds */
    private int backoffDelay = 0;
    /** Back-off number of attempts */
    private int backoffRetry = 0;

    @Override
    public void initialize(TaskAttemptContext context) throws IOException,
      InterruptedException {

      txsize = GIRAPH_REXSTER_OUTPUT_E_TXSIZE.get(getConf());
      vlabel = GIRAPH_REXSTER_VLABEL.get(getConf());
      backoffDelay = GIRAPH_REXSTER_BACKOFF_DELAY.get(getConf());
      backoffRetry = GIRAPH_REXSTER_BACKOFF_RETRY.get(getConf());
      startConnection();
    }

    @Override
    public void close(TaskAttemptContext context)
      throws IOException, InterruptedException {

      stopConnection();
    }

    @Override
    public void writeEdge(I srcId, V srcValue, Edge<I, E> edge)
      throws IOException, InterruptedException {

      if (txcounter == txsize) {
        txcounter = 0;
        isFirstElement = true;
        stopConnection();
        startConnection();
      }

      try {
        JSONObject jsonEdge;
        String suffix;

        /* extract the JSON object of the vertex */
        jsonEdge = getEdge(srcId, srcValue, edge);
        /* determine the suffix to add the object into the JSON array */
        if (isFirstElement) {
          isFirstElement = false;
          suffix = "";
        } else {
          suffix = ",";
        }
        rexsterBufferedStream.write(suffix + jsonEdge);
        txcounter += 1;

      } catch (JSONException e) {
        throw new InterruptedException("Error writing the edge: " +
                                       e.getMessage());
      }
    }

    /**
     * Start a new connection with the Rexster REST endpoint.
     */
    private void startConnection() throws IOException, InterruptedException {
      rexsterConn = RexsterUtils.Edge.openOutputConnection(getConf());
      rexsterBufferedStream = new BufferedWriter(
          new OutputStreamWriter(rexsterConn.getOutputStream(),
                                 Charset.forName("UTF-8")));
      /* open the JSON container: is an object containing an array of
         elements */
      rexsterBufferedStream.write("{ ");
      rexsterBufferedStream.write("\"vlabel\" : \"" + vlabel + "\",");
      rexsterBufferedStream.write("\"delay\" : \"" + backoffDelay + "\",");
      rexsterBufferedStream.write("\"retry\" : \"" + backoffRetry + "\",");
      rexsterBufferedStream.write("\"" + JSON_ARRAY_KEY + "\"");
      rexsterBufferedStream.write(" : [ ");
    }

    /**
     * Stop a new connection with the Rexster REST endpoint. By default the
     * JDK manages keep-alive so no particular code is sent in place for this
     * aim.
     */
    private void stopConnection() throws IOException, InterruptedException {
      /* close the JSON container */
      rexsterBufferedStream.write(" ] }");
      rexsterBufferedStream.flush();
      rexsterBufferedStream.close();

      /* check the response and in case of error signal the unsuccessful state
         via exception */
      RexsterUtils.Edge.handleResponse(rexsterConn);
    }

    /**
     * Each edge needs to be transformed into a JSON object to be sent to the
     * batch interface of Rexster.
     *
     * @param  srcId    source vertex ID of the edge
     * @param  srcValue source vertex value of the edge
     * @param  edge     edge to be transformed in JSON
     * @return          JSON representation of the edge
     */
    protected JSONObject getEdge(I srcId, V srcValue, Edge<I, E> edge)
      throws JSONException {

      String outId = srcId.toString();
      String inId = edge.getTargetVertexId().toString();
      String value = edge.getValue().toString();
      JSONObject jsonEdge = new JSONObject();
      jsonEdge.accumulate("_outV", outId);
      jsonEdge.accumulate("_inV",  inId);
      jsonEdge.accumulate("value", value);

      return jsonEdge;
    }
  }
}
