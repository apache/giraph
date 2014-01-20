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

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.rexster.io.RexsterVertexOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Rexster Vertex Output Format for Long ID's, Double Vertex values and
 * Float edge values.
 */
public class RexsterLongDoubleFloatVertexOutputFormat
  extends RexsterVertexOutputFormat<LongWritable, DoubleWritable,
          FloatWritable> {

  @Override
  public RexsterVertexWriter createVertexWriter(
      TaskAttemptContext context) throws IOException,
      InterruptedException {

    return new RexsterLongDoubleFloatVertexWriter();
  }

  /**
   * Rexster vertex writer.
   */
  protected class RexsterLongDoubleFloatVertexWriter
    extends RexsterVertexWriter {

    /** current vertex ID */
    private LongWritable vertexId;

    @Override
    protected JSONObject getVertex(
      Vertex<LongWritable, DoubleWritable, FloatWritable> vertex)
      throws JSONException {

      vertexId = vertex.getId();

      double value = vertex.getValue().get();
      JSONObject jsonVertex = new JSONObject();
      jsonVertex.accumulate("value", value);

      return jsonVertex;
    }

    @Override
    protected LongWritable getVertexId() {
      return vertexId;
    }
  }
}
