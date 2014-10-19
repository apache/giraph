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

import org.apache.avro.util.Utf8;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.io.gora.generated.GEdge;
import org.apache.giraph.io.gora.generated.GEdgeResult;
import org.apache.gora.persistency.Persistent;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Assert;

/**
 * Implementation of a specific writer for a generated data bean.
 */
public class GoraTestEdgeOutputFormat
  extends GoraEdgeOutputFormat<LongWritable, DoubleWritable,
  FloatWritable> {

  /**
   * Default constructor
   */
  public GoraTestEdgeOutputFormat() {
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
      Utf8 keyLabel = new Utf8(srcId.toString() + "-" +
      edge.getTargetVertexId().toString());
      tmpGEdge.setEdgeId(keyLabel.toString());
      tmpGEdge.setEdgeWeight(edge.getValue().get());
      tmpGEdge.setVertexInId(srcId.toString());
      tmpGEdge.setVertexOutId(edge.getTargetVertexId().toString());
      tmpGEdge.setLabel(keyLabel.toString());
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

    @SuppressWarnings("unchecked")
    @Override
    public void writeEdge(LongWritable srcId, DoubleWritable srcValue,
        Edge<LongWritable, FloatWritable> edge)
        throws IOException, InterruptedException {
      super.writeEdge(srcId, srcValue, edge);
      Object goraKey = getGoraKey(srcId, srcValue, edge);
      String keyLabel = String.valueOf(srcId) + "-" +
          String.valueOf(edge.getTargetVertexId());
      float weight = Float.valueOf(srcId.toString()) +
          Float.valueOf(edge.getTargetVertexId().toString());
      // Asserting
      Assert.assertEquals(createEdge(keyLabel, String.valueOf(srcId),
              String.valueOf(edge.getTargetVertexId()),keyLabel, weight),
              getDataStore().get(goraKey));
    }

    /**
     * Creates an edge using an id and a set of edges.
     * @param id Vertex id.
     * @param vertexInId Vertex source Id.
     * @param vertexOutId Vertex destination Id.
     * @param edgeLabel Edge label.
     * @param edgeWeight Edge wight.
     * @return GEdge created.
     */
    private GEdgeResult createEdge(String id, String vertexInId,
        String vertexOutId, String edgeLabel, float edgeWeight) {
      GEdgeResult newEdge = new GEdgeResult();
      newEdge.setEdgeId(id);
      newEdge.setVertexInId(vertexInId);
      newEdge.setVertexOutId(vertexOutId);
      newEdge.setLabel(edgeLabel);
      newEdge.setEdgeWeight(edgeWeight);
      return newEdge;
    }
  }
}
