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

import static org.apache.giraph.rexster.conf.GiraphRexsterConstants.GIRAPH_REXSTER_E_ESTIMATE;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.io.EdgeInputFormat;
import org.apache.giraph.io.EdgeReader;
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
 * @param <I>   Vertex id
 * @param <E>   Edge data
 */
public abstract class RexsterEdgeInputFormat<I extends WritableComparable,
  E extends Writable> extends EdgeInputFormat<I, E> {

  /** Class logger. */
  private static final Logger LOG =
      Logger.getLogger(RexsterEdgeInputFormat.class);

  /**
   * @param conf configuration parameters
   */
  public void checkInputSpecs(Configuration conf) { }

  @Override
  public List<InputSplit> getSplits(JobContext context, int minSplitCountHint)
    throws IOException, InterruptedException {

    return RexsterUtils.getSplits(context,
                                  GIRAPH_REXSTER_E_ESTIMATE.get(getConf()));
  }

  @Override
  public abstract RexsterEdgeReader createEdgeReader(InputSplit split,
      TaskAttemptContext context) throws IOException;

  /**
   * Abstract class to be implemented by the user based on their specific
   * vertex input. Easiest to ignore the key value separator and only use
   * key instead.
   */
  protected abstract class RexsterEdgeReader extends EdgeReader<I, E> {

    /** Input stream from the HTTP connection to the REST endpoint */
    private BufferedReader  rexsterBufferedStream;
    /** JSON parser/tokenizer object */
    private JSONTokener     tokener;
    /** start index of the Rexster paging */
    private long            splitStart;
    /** end index of the Rexster paging */
    private long            splitEnd;
    /** number of iterated items */
    private long            itemsIterated = 0;
    /** current edge obtained from Rexster */
    private Edge<I, E>      edge;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context)
      throws IOException, InterruptedException {

      final RexsterInputSplit rexsterInputSplit =
        (RexsterInputSplit) inputSplit;

      this.splitEnd   = rexsterInputSplit.getEnd();
      this.splitStart = rexsterInputSplit.getStart();

      this.rexsterBufferedStream =
        RexsterUtils.Edge.openRexsterStream(getConf(),
                                            this.splitStart, this.splitEnd);

      this.tokener = RexsterUtils.parseJSONEnvelope(this.rexsterBufferedStream);
    }

    @Override
    public void close() throws IOException {
      this.rexsterBufferedStream.close();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      final float estimated = GIRAPH_REXSTER_E_ESTIMATE.get(getConf());

      if (this.splitStart == this.splitEnd) {
        return 0.0f;
      } else {
        /* assuming you got the estimate right this progress should be
           pretty close; */
        return Math.min(1.0f, this.itemsIterated / (float) estimated);
      }
    }

    @Override
    public Edge<I, E> getCurrentEdge()
      throws IOException, InterruptedException {

      return this.edge;
    }

    @Override
    public boolean nextEdge() throws IOException, InterruptedException {
      try {
        JSONObject  obj;
        char        c;

        /* if the tokener was not set, no objects are in fact available */
        if (this.tokener == null) {
          return false;
        }

        obj = new JSONObject(this.tokener);
        this.edge = parseEdge(obj);

        c = tokener.nextClean();
        if (c == RexsterUtils.ARRAY_SEPARATOR) {
          itemsIterated += 1;
          return true;
        } else if (c == RexsterUtils.END_ARRAY) {
          return false;
        } else {
          LOG.error(String.format("Expected a '%c' at the end of the array",
                                  RexsterUtils.END_ARRAY));
          throw new InterruptedException();
        }

      } catch (JSONException e) {
        LOG.error(e.toString());
        return false;
      }
    }

    /**
     * Parser for a single edge JSON object
     *
     * @param   jsonEdge edge represented as JSON object
     * @return  The edge object associated with the JSON object
     */
    protected abstract Edge<I, E> parseEdge(JSONObject jsonEdge)
      throws JSONException;

    @Override
    public abstract I getCurrentSourceId()
      throws IOException, InterruptedException;
  }
}
