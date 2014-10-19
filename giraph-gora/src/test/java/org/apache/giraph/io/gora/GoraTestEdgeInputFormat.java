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
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.gora.generated.GEdge;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Implementation of a specific reader for a generated data bean.
 */
public class GoraTestEdgeInputFormat
  extends GoraEdgeInputFormat<LongWritable, FloatWritable> {

  /**
   * Default constructor
   */
  public GoraTestEdgeInputFormat() {
  }

  /**
   * Creates specific vertex reader to be used inside Hadoop.
   * @param split split to be read.
   * @param context JobContext to be used.
   * @return GoraEdgeReader Edge reader to be used by Hadoop.
   */
  @Override
  public GoraEdgeReader createEdgeReader(
      InputSplit split, TaskAttemptContext context) throws IOException {
    putArtificialData();
    return new GoraGEdgeEdgeReader();
  }

  /**
   * Writes data into the data store in order to test it out.
   */
  @SuppressWarnings("unchecked")
  private static void putArtificialData() {
    getDataStore().put("11-22",
        createEdge("11-22", "11", "22", "11-22", (float)(11+22)));
    getDataStore().put("22-11",
        createEdge("22-11", "22", "11", "22-11", (float)(22+11)));
    getDataStore().put("11-33",
        createEdge("11-33", "11", "33", "11-33", (float)(11+33)));
    getDataStore().put("33-11",
        createEdge("33-11", "33", "11", "33-11", (float)(33+11)));
    getDataStore().flush();
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
  private static GEdge createEdge(String id, String vertexInId,
      String vertexOutId, String edgeLabel, float edgeWeight) {
    GEdge newEdge = new GEdge();
    newEdge.setEdgeId(id);
    newEdge.setVertexInId(vertexInId);
    newEdge.setVertexOutId(vertexOutId);
    newEdge.setLabel(edgeLabel);
    newEdge.setEdgeWeight(edgeWeight);
    return newEdge;
  }

  /**
   * Gora edge reader
   */
  protected class GoraGEdgeEdgeReader extends GoraEdgeReader {

    /** source vertex of the edge */
    private LongWritable sourceId;

    /**
     * Transforms a GoraObject into an Edge object.
     * @param goraObject Object from Gora to be translated.
     * @return Edge Result from transforming the gora object.
     */
    @Override
    protected Edge<LongWritable, FloatWritable> transformEdge
    (Object goraObject) {
      Edge<LongWritable, FloatWritable> edge = null;
      GEdge goraEdge = (GEdge) goraObject;
      Long dest;
      Float value;
      dest = Long.valueOf(goraEdge.getVertexOutId().toString());
      this.sourceId = new LongWritable();
      this.sourceId.set(Long.valueOf(goraEdge.getVertexInId().toString()));
      value = (float) goraEdge.getEdgeWeight();
      edge = EdgeFactory.create(new LongWritable(dest),
          new FloatWritable(value));
      return edge;
    }

    /**
     * Gets the currentSourceId for the edge.
     * @return LongWritable currentSourceId for the edge.
     */
    @Override
    public LongWritable getCurrentSourceId() throws IOException,
        InterruptedException {
      return this.sourceId;
    }
  }
}
