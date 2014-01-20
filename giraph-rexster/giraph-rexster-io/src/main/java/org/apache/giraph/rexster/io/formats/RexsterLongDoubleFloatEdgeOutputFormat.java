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
import org.apache.giraph.rexster.io.RexsterEdgeOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Rexster Edge Output Format for Long ID's, Double Vertex values and
 * Float edge values.
 */
public class RexsterLongDoubleFloatEdgeOutputFormat
  extends RexsterEdgeOutputFormat<LongWritable, DoubleWritable,
          FloatWritable> {

  @Override
  public RexsterEdgeWriter createEdgeWriter(
      TaskAttemptContext context) throws IOException,
      InterruptedException {

    return new RexsterLongDoubleFloatEdgeWriter();
  }

  /**
   * Rexster edge writer.
   */
  protected class RexsterLongDoubleFloatEdgeWriter
    extends RexsterEdgeWriter {

    @Override
    protected JSONObject getEdge(LongWritable srcId, DoubleWritable srcValue,
      Edge<LongWritable, FloatWritable> edge) throws JSONException {

      long outId = srcId.get();
      long inId = edge.getTargetVertexId().get();
      float value = edge.getValue().get();
      JSONObject jsonEdge = new JSONObject();
      jsonEdge.accumulate("_outV", outId);
      jsonEdge.accumulate("_inV",  inId);
      jsonEdge.accumulate("value", value);

      return jsonEdge;
    }
  }
}
