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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Rexster Edge Input Format for Long vertex ID's and Double edge values
 */
public class RexsterLongDoubleEdgeInputFormat
  extends RexsterEdgeInputFormat<LongWritable, DoubleWritable> {

  @Override
  public RexsterEdgeReader createEdgeReader(
      InputSplit split, TaskAttemptContext context) throws IOException {

    return new RexsterLongDoubleEdgeReader();
  }

  /**
   * Rexster edge reader
   */
  protected class RexsterLongDoubleEdgeReader extends RexsterEdgeReader {

    /** source vertex of the edge */
    private LongWritable sourceId;

    @Override
    public LongWritable getCurrentSourceId()
      throws IOException, InterruptedException {

      return this.sourceId;
    }

    @Override
    protected Edge<LongWritable, DoubleWritable> parseEdge(JSONObject jsonEdge)
      throws JSONException {

      Long value = jsonEdge.getLong("value");
      Long dest;
      try {
        dest = jsonEdge.getLong("_outV");
      } catch (JSONException ex) {
        /* OrientDB compatibility; try to transform it as long */
        String idString = jsonEdge.getString("_outV");
        String[] splits = idString.split(":");
        dest = Long.parseLong(splits[1]);
      }
      Edge<LongWritable, DoubleWritable> edge =
        EdgeFactory.create(new LongWritable(dest), new DoubleWritable(value));

      Long sid;
      try {
        sid = jsonEdge.getLong("_inV");
      } catch (JSONException ex) {
        /* OrientDB compatibility; try to transform it as long */
        String sidString = jsonEdge.getString("_inV");
        String[] splits = sidString.split(":");
        sid = Long.parseLong(splits[1]);
      }
      this.sourceId = new LongWritable(sid);
      return edge;
    }
  }
}
