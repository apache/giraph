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

import static org.apache.giraph.rexster.conf.GiraphRexsterConstants.GIRAPH_REXSTER_V_ESTIMATE;
import static org.apache.giraph.rexster.conf.GiraphRexsterConstants.GIRAPH_REXSTER_HOSTNAME;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexReader;
import org.apache.giraph.rexster.utils.RexsterUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

/**
 * Abstract class that users should subclass to use their own Rexster based
 * vertex input format. This class was inspired by the Rexster Input format
 * available in Faunus authored by Stephen Mallette.
 *
 * @param <I>
 * @param <V>
 * @param <E>
 */
@SuppressWarnings("rawtypes")
public abstract class RexsterVertexInputFormat<I extends WritableComparable,
  V extends Writable, E extends Writable>
  extends VertexInputFormat<I, V, E> {

  /** Class logger. */
  private static final Logger LOG =
      Logger.getLogger(RexsterVertexInputFormat.class);

  /**
   * @param conf configuration parameters
   */
  public void checkInputSpecs(Configuration conf) {
    GiraphConfiguration gconf = new GiraphConfiguration(conf);

    if (!gconf.hasEdgeInputFormat()) {
      String err = "Rexster Input I/O requires " +
                   "both Vertex- and EdgeInputFormat.";
      LOG.error(err);
      throw new RuntimeException(err);
    }

    String endpoint = GIRAPH_REXSTER_HOSTNAME.get(conf);
    if (endpoint == null) {
      throw new RuntimeException(GIRAPH_REXSTER_HOSTNAME.getKey() +
                                 " is a mandatory parameter.");
    }
  }

  /**
   * Create a vertex reader for a given split. Guaranteed to have been
   * configured with setConf() prior to use.  The framework will also call
   * {@link VertexReader#initialize(InputSplit, TaskAttemptContext)} before
   * the split is used.
   *
   * @param split the split to be read
   * @param context the information about the task
   * @return a new record reader
   * @throws IOException
   */
  public abstract RexsterVertexReader createVertexReader(InputSplit split,
    TaskAttemptContext context) throws IOException;

  @Override
  public List<InputSplit> getSplits(JobContext context, int minSplitCountHint)
    throws IOException, InterruptedException {

    return RexsterUtils.getSplits(context,
                                  GIRAPH_REXSTER_V_ESTIMATE.get(getConf()));
  }

  /**
   * Abstract class to be implemented by the user based on their specific
   * vertex input. Easiest to ignore the key value separator and only use
   * key instead.
   */
  protected abstract class RexsterVertexReader extends VertexReader<I, V, E> {

    /** Input stream from the HTTP connection to the REST endpoint */
    private BufferedReader rexsterBufferedStream;
    /** JSON parser/tokenizer object */
    private JSONTokener tokener;
    /** start index of the Rexster paging */
    private long splitStart;
    /** end index of the Rexster paging */
    private long splitEnd;
    /** index to access the iterated vertices */
    private long itemsIterated = 0;
    /** current vertex */
    private Vertex<I, V, E> vertex;
    /** first call to the nextVertex fuction */
    private boolean isFirstVertex;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context)
      throws IOException, InterruptedException {

      RexsterInputSplit rexsterInputSplit = (RexsterInputSplit) inputSplit;
      splitEnd = rexsterInputSplit.getEnd();
      splitStart = rexsterInputSplit.getStart();
      rexsterBufferedStream = RexsterUtils.Vertex.openInputStream(getConf(),
        splitStart, splitEnd);
      tokener = RexsterUtils.parseJSONEnvelope(rexsterBufferedStream);
      isFirstVertex = true;
    }

    @Override
    public boolean nextVertex() throws IOException, InterruptedException {
      try {
        /* if the tokener was not set, no object is in fact available */
        if (this.tokener == null) {
          return false;
        }

        char c;
        if (isFirstVertex) {
          isFirstVertex = false;

          c = this.tokener.nextClean();
          if (c == RexsterUtils.END_ARRAY) {
            return false;
          }
          tokener.back();
        }

        JSONObject obj = new JSONObject(this.tokener);
        this.vertex = parseVertex(obj);

        c = this.tokener.nextClean();
        if (c == RexsterUtils.ARRAY_SEPARATOR) {
          itemsIterated += 1;
          return true;
        } else if (c == RexsterUtils.END_ARRAY) {
          return false;
        } else {
          LOG.error(String.format("Expected a '%c' at the end of the array",
                                  RexsterUtils.END_ARRAY));
          throw new InterruptedException(
              String.format("Expected a '%c' at the end of the array",
                            RexsterUtils.END_ARRAY));
        }
      } catch (JSONException e) {
        throw new InterruptedException(e.toString());
      }
    }

    @Override
    public void close() throws IOException {
      this.rexsterBufferedStream.close();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      float vestimated = GIRAPH_REXSTER_V_ESTIMATE.get(getConf());

      if (this.splitStart == this.splitEnd) {
        return 0.0f;
      } else {
        // assuming you got the estimate right this progress should be
        // pretty close;
        return Math.min(1.0f, this.itemsIterated / (float) vestimated);
      }
    }

    @Override
    public Vertex<I, V, E> getCurrentVertex()
      throws IOException, InterruptedException {

      return this.vertex;
    }

    /**
     * Parser for a single vertex JSON object
     *
     * @param   jsonVertex vertex represented as JSON object
     * @return  The vertex object represented by the JSON object
     */
    protected abstract Vertex<I, V, E> parseVertex(JSONObject jsonVertex)
      throws JSONException;
  }
}
