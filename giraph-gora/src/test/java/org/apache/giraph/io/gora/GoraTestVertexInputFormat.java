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
import java.util.Map;
import java.util.Set;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.gora.generated.GVertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Implementation of a specific reader for a generated data bean.
 */
public class GoraTestVertexInputFormat
  extends GoraVertexInputFormat<LongWritable, DoubleWritable,
          FloatWritable> {

  /**
   * DEfault constructor
   */
  public GoraTestVertexInputFormat() {
  }

  /**
   * Creates specific vertex reader to be used inside Hadoop.
   * @param split split to be read.
   * @param context JobContext to be used.
   * @return GoraVertexReader Vertex reader to be used by Hadoop.
   */
  @Override
  public GoraVertexReader createVertexReader(
      InputSplit split, TaskAttemptContext context) throws IOException {
    putArtificialData();
    return new GoraGVertexVertexReader();
  }

  /**
   * Writes data into the data store in order to test it out.
   */
  @SuppressWarnings("unchecked")
  private static void putArtificialData() {
    getDataStore().put("1", createVertex("1", null));
    getDataStore().put("10", createVertex("10", null));
    getDataStore().put("100", createVertex("100", null));
    getDataStore().flush();
  }

  /**
   * Creates a vertex using an id and a set of edges.
   * @param id Vertex id.
   * @param edges Set of edges.
   * @return GVertex created.
   */
  public static GVertex createVertex(String id, Map<String, String> edges) {
    GVertex newVrtx = new GVertex();
    newVrtx.setVertexId(id);
    if (edges != null) {
      for (String edgeId : edges.keySet())
        newVrtx.getEdges().put(edgeId, edges.get(edgeId));
    }
    return newVrtx;
  }

  /**
   * Gora vertex reader
   */
  protected class GoraGVertexVertexReader extends GoraVertexReader {

    /**
     * Transforms a GoraObject into a Vertex object.
     * @param goraObject Object from Gora to be translated.
     * @return Vertex Result from transforming the gora object.
     */
    @Override
    protected Vertex<LongWritable, DoubleWritable, FloatWritable>
    transformVertex(Object goraObject) {
      Vertex<LongWritable, DoubleWritable, FloatWritable> vertex;
      /* create the actual vertex */
      vertex = getConf().createVertex();
      GVertex tmpGVertex = (GVertex) goraObject;

      LongWritable vrtxId = new LongWritable(
          Long.parseLong(tmpGVertex.getVertexId().toString()));
      DoubleWritable vrtxValue = new DoubleWritable(tmpGVertex.getVertexValue());
      vertex.initialize(vrtxId, vrtxValue);
      if (tmpGVertex.getEdges() != null && !tmpGVertex.getEdges().isEmpty()) {
        Set<CharSequence> keyIt = tmpGVertex.getEdges().keySet();
        for (CharSequence key : keyIt) {
          String keyVal = key.toString();
          String valVal = tmpGVertex.getEdges().get(key).toString();
          Edge<LongWritable, FloatWritable> edge;
          if (!keyVal.contains("vertexId")) {
            edge = EdgeFactory.create(
                new LongWritable(Long.parseLong(keyVal)),
                new FloatWritable(Float.parseFloat(valVal)));
            vertex.addEdge(edge);
          }
        }
      }
      return vertex;
    }
  }
}
