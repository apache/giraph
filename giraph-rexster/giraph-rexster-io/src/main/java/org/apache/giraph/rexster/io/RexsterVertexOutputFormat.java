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

import static org.apache.giraph.rexster.conf.GiraphRexsterConstants.GIRAPH_REXSTER_OUTPUT_V_TXSIZE;
import static org.apache.giraph.rexster.conf.GiraphRexsterConstants.GIRAPH_REXSTER_OUTPUT_WAIT_TIMEOUT;
import static org.apache.giraph.rexster.conf.GiraphRexsterConstants.GIRAPH_REXSTER_VLABEL;
import static org.apache.giraph.rexster.conf.GiraphRexsterConstants.GIRAPH_REXSTER_HOSTNAME;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.giraph.bsp.BspService;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.io.VertexWriter;
import org.apache.giraph.rexster.utils.RexsterUtils;
import org.apache.giraph.zk.ZooKeeperExt;
import org.apache.giraph.zk.ZooKeeperManager;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Abstract class that users should subclass to use their own Rexster based
 * vertex onput format.
 *
 * @param <I>
 * @param <V>
 * @param <E>
 */
@SuppressWarnings("rawtypes")
public class RexsterVertexOutputFormat<I extends WritableComparable,
  V extends Writable, E extends Writable>
  extends VertexOutputFormat<I, V, E> {

  /** Class logger. */
  private static final Logger LOG =
      Logger.getLogger(RexsterVertexOutputFormat.class);

  @Override
  public RexsterVertexWriter
  createVertexWriter(TaskAttemptContext context)
    throws IOException, InterruptedException {

    return new RexsterVertexWriter();
  }

  @Override
  public void checkOutputSpecs(JobContext context)
    throws IOException, InterruptedException {
    GiraphConfiguration gconf =
      new GiraphConfiguration(context.getConfiguration());
    String msg = "Rexster OutputFormat usage requires both Edge and Vertex " +
                 "OutputFormat's.";

    if (!gconf.hasEdgeOutputFormat()) {
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
  protected class RexsterVertexWriter extends VertexWriter<I, V, E>
    implements Watcher {
    /** barrier path */
    private static final String BARRIER_PATH = "/_rexsterBarrier";
    /** array key that points to the edges and vertices */
    private static final String JSON_ARRAY_KEY = "tx";
    /** Connection to the HTTP REST endpoint */
    private HttpURLConnection rexsterConn;
    /** Output stream from the HTTP connection to the REST endpoint */
    private BufferedWriter rexsterBufferedStream;
    /** attribute used to keep the state of the element array status */
    private boolean isFirstElement = true;
    /** ZooKeeper client object */
    private ZooKeeperExt zk = null;
    /** lock for management of the barrier */
    private final Object lock = new Object();
    /** number of vertices before starting a new connection */
    private int txsize;
    /** number of vertexes of vertices sent */
    private int txcounter = 0;
    /** label of the vertex id field */
    private String vlabel;
    /** vertex id */
    private I vertexId;

    @Override
    public void initialize(TaskAttemptContext context)
      throws IOException, InterruptedException {
      ImmutableClassesGiraphConfiguration conf = getConf();

      vlabel = GIRAPH_REXSTER_VLABEL.get(conf);
      txsize = GIRAPH_REXSTER_OUTPUT_V_TXSIZE.get(conf);
      startConnection();

      /* set the barrier */
      zk = new ZooKeeperExt(conf.getZookeeperList(),
          conf.getZooKeeperSessionTimeout(), conf.getZookeeperOpsMaxAttempts(),
          conf.getZookeeperOpsRetryWaitMsecs(), this, context);
    }

    @Override
    public void close(TaskAttemptContext context)
      throws IOException, InterruptedException {
      /* close connection */
      stopConnection();

      /* deal with the barrier */
      String id = context.getTaskAttemptID().toString();
      String zkBasePath = ZooKeeperManager.getBasePath(getConf()) +
        BspService.BASE_DIR + "/" +
        getConf().getJobId();
      prepareBarrier(zkBasePath);
      enterBarrier(zkBasePath, id);
      checkBarrier(zkBasePath, context);
    }

    @Override
    public void writeVertex(Vertex<I, V, E> vertex)
      throws IOException, InterruptedException {

      if (txcounter == txsize) {
        txcounter = 0;
        isFirstElement = true;
        stopConnection();
        startConnection();
      }

      try {
        /* extract the JSON object of the vertex */
        JSONObject jsonVertex = getVertex(vertex);
        jsonVertex.accumulate("_type", "vertex");
        jsonVertex.accumulate(vlabel, getVertexId().toString());
        String suffix = ",";
        if (isFirstElement) {
          isFirstElement = false;
          suffix = "";
        }
        rexsterBufferedStream.write(suffix + jsonVertex);
        txcounter += 1;

      } catch (JSONException e) {
        throw new InterruptedException("Error writing the vertex: " +
                                       e.getMessage());
      }
    }

    @Override
    public void process(WatchedEvent event) {
      EventType type = event.getType();

      if (type == EventType.NodeChildrenChanged) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("signal: number of children changed.");
        }
        synchronized (lock) {
          lock.notify();
        }
      }
    }

    /**
     * Prepare the root node if needed to create the root Rexster barrier znode
     *
     * @param  zkBasePath  base path for zookeeper
     * @throws InterruptedException
     */
    private void prepareBarrier(String zkBasePath)
      throws InterruptedException {
      try {
        zk.createExt(zkBasePath + BARRIER_PATH, null, Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT, false);
      } catch (KeeperException.NodeExistsException nee) {
        if (LOG.isInfoEnabled()) {
          LOG.info("rexster barrier znode already exists.");
        }
      } catch (KeeperException ke) {
        throw new InterruptedException("RexsterVertexOutputFormat: " +
            "error while creating the barrier: " + ke.getMessage());
      }
    }

    /**
     * Enter the Rexster barrier
     *
     * @param  zkBasePath  base path for zookeeper
     * @param  id       value id used for the znode
     * @throws InterruptedException
     */
    private void enterBarrier(String zkBasePath, String id)
      throws InterruptedException {
      try {
        zk.createExt(zkBasePath + BARRIER_PATH + "/" + id, null,
          Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, false);
      } catch (KeeperException.NodeExistsException nee) {
        if (LOG.isInfoEnabled()) {
          LOG.info("rexster barrier znode already exists.");
        }
      } catch (KeeperException ke) {
        throw new InterruptedException("RexsterVertexOutputFormat: " +
            "error while creating the barrier: " + ke.getMessage());
      }
    }

    /**
     * Check the Rexster barrier to verify whether all the vertices have been
     * saved. If so, the barrier can be left and it is possible to save the
     * edges.
     *
     * @param  zkBasePath  base path for zookeeper
     * @param  context  task attempt context
     * @throws InterruptedException
     */
    private void checkBarrier(String zkBasePath,
      TaskAttemptContext context) throws InterruptedException {
      long workersNum = getConf().getMapTasks() - 1;
      int timeout = GIRAPH_REXSTER_OUTPUT_WAIT_TIMEOUT.get(getConf());

      try {
        String barrierPath = zkBasePath + BARRIER_PATH;
        while (true) {
          List<String> list =
            zk.getChildrenExt(barrierPath, true, false, false);

          if (list.size() < workersNum) {
            synchronized (lock) {
              lock.wait(timeout);
            }
            context.progress();
          } else {
            return;
          }
        }
      } catch (KeeperException ke) {
        throw new InterruptedException("Error while checking the barrier:" +
                                       ke.getMessage());
      }
    }

    /**
     * Start a new connection with the Rexster REST endpoint.
     */
    private void startConnection() throws IOException, InterruptedException {
      rexsterConn = RexsterUtils.Vertex.openOutputConnection(getConf());
      rexsterBufferedStream = new BufferedWriter(
          new OutputStreamWriter(rexsterConn.getOutputStream(),
                                 Charset.forName("UTF-8")));
      /* open the JSON container: is an object containing an array of
         elements */
      rexsterBufferedStream.write("{ ");
      rexsterBufferedStream.write("\"vlabel\" : \"" + vlabel + "\",");
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
      RexsterUtils.Vertex.handleResponse(rexsterConn);
    }

    /**
     * Each vertex needs to be transformed into a JSON object to be sent to the
     * batch interface of Rexster. This function does NOT need to implement any
     * edge transformation since RexsterVertexWriter#getEdge is
     * intended for such a task.
     *
     * @param  vertex   vertex to be transformed into JSON
     * @return          JSON representation of the vertex
     */
    protected JSONObject getVertex(Vertex<I, V, E> vertex)
      throws JSONException {

      vertexId = vertex.getId();

      String value = vertex.getValue().toString();
      JSONObject jsonVertex = new JSONObject();
      jsonVertex.accumulate("value", value);

      return jsonVertex;
    }

    /**
     * For compatibility reasons, the id of the vertex needs to be accumulated
     * in the vertex object using the defined vlabel, hence we provide a
     * different function to get the vertex id to keep this compatibility
     * management indipendent from the user implementation.
     *
     * @return vertex id object
     */
    protected I getVertexId() {
      return vertexId;
    }
  }
}
