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

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.rexster.io.RexsterEdgeInputFormat;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Rexster Edge Input Format for Long vertex ID's and Float edge values
 */
public class RexsterLongFloatEdgeInputFormat
  extends RexsterEdgeInputFormat<LongWritable, FloatWritable> {

  @Override
  public RexsterEdgeReader createEdgeReader(
      InputSplit split, TaskAttemptContext context) throws IOException {

    return new RexsterLongFloatEdgeReader();
  }

  /**
   * Rexster edge reader
   */
  protected class RexsterLongFloatEdgeReader extends RexsterEdgeReader {

    /** source vertex of the edge */
    private LongWritable sourceId;

    @Override
    public LongWritable getCurrentSourceId() throws IOException,
        InterruptedException {

      return this.sourceId;
    }

    @Override
    protected Edge<LongWritable, FloatWritable> parseEdge(JSONObject jsonEdge)
      throws JSONException {

      Edge<LongWritable, FloatWritable> edge = null;
      Long                              dest;
      Long                              value;

      value = jsonEdge.getLong("weight");
      dest = jsonEdge.getLong("_outV");
      edge = EdgeFactory.create(new LongWritable(dest),
                                new FloatWritable(value));
      this.sourceId = new LongWritable(jsonEdge.getLong("_inV"));

      return edge;
    }
  }
}
