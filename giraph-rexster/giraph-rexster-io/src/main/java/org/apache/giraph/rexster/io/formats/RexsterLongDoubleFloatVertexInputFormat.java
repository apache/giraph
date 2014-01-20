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
import org.apache.giraph.rexster.io.RexsterVertexInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Rexster Edge Input Format for Long vertex ID's and Float edge values
 */
public class RexsterLongDoubleFloatVertexInputFormat
  extends RexsterVertexInputFormat<LongWritable, DoubleWritable,
          FloatWritable> {

  @Override
  public RexsterVertexReader createVertexReader(
    InputSplit split, TaskAttemptContext context) throws IOException {

    return new RexsterLongDoubleFloatVertexReader();
  }

  /**
   * Rexster vertex reader
   */
  protected class RexsterLongDoubleFloatVertexReader
    extends RexsterVertexReader {

    @Override
    protected Vertex<LongWritable, DoubleWritable, FloatWritable> parseVertex(
      JSONObject jsonVertex) throws JSONException {

      /* create the actual vertex */
      Vertex<LongWritable, DoubleWritable, FloatWritable> vertex =
        getConf().createVertex();

      Long id;
      try {
        id = jsonVertex.getLong("_id");
      } catch (JSONException ex) {
        /* OrientDB compatibility; try to transform it as long */
        String idString = jsonVertex.getString("_id");
        String[] splits = idString.split(":");
        id = Long.parseLong(splits[1]);
      }

      Double value;
      try {
        value = jsonVertex.getDouble("value");
      } catch (JSONException ex) {
        /* OrientDB compatibility; try to transform it as long */
        value = new Double(0);
      }

      vertex.initialize(new LongWritable(id), new DoubleWritable(value));
      return vertex;
    }
  }
}
