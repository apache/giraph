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
package org.apache.giraph.io.gora;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.io.gora.generated.GEdgeResult;
import org.apache.gora.persistency.Persistent;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Implementation of a specific writer for a generated data bean.
 */
public class GoraGEdgeEdgeOutputFormat
  extends GoraEdgeOutputFormat<LongWritable, DoubleWritable,
  FloatWritable> {

  /**
   * Default constructor
   */
  public GoraGEdgeEdgeOutputFormat() {
  }

  @Override
  public GoraEdgeWriter createEdgeWriter(
      TaskAttemptContext context) throws IOException, InterruptedException {
    return new GoraGEdgeEdgeWriter();
  }

  /**
   * Gora edge writer.
   */
  protected class GoraGEdgeEdgeWriter
    extends GoraEdgeWriter {

    @Override
    protected Persistent getGoraEdge(LongWritable srcId,
        DoubleWritable srcValue, Edge<LongWritable, FloatWritable> edge) {
      GEdgeResult tmpGEdge = new GEdgeResult();
      tmpGEdge.setEdgeId(srcId.toString());
      tmpGEdge.setEdgeWeight(edge.getValue().get());
      tmpGEdge.setVertexOutId(edge.getTargetVertexId().toString());
      getLogger().debug("GoraObject created: " + tmpGEdge.toString());
      return tmpGEdge;
    }

    @Override
    protected Object getGoraKey(LongWritable srcId,
        DoubleWritable srcValue, Edge<LongWritable, FloatWritable> edge) {
      String goraKey = String.valueOf(
          edge.getTargetVertexId().get() + edge.getValue().get());
      return goraKey;
    }

  }
}
