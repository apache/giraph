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

package org.apache.giraph.io.formats;

import org.apache.giraph.bsp.ImmutableOutputCommitter;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.io.VertexWriter;
import org.apache.giraph.utils.TestGraph;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * VertexOutputFormat which stores all vertices in memory
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
public class InMemoryVertexOutputFormat<I extends WritableComparable,
    V extends Writable, E extends Writable> extends
    VertexOutputFormat<I, V, E> {
  /** Graph where we store all vertices */
  private static TestGraph OUTPUT_GRAPH;

  /**
   * Initialize this output format - needs to be called before running the
   * application. Creates new instance of TestGraph
   *
   * @param conf Configuration
   */
  public static void initializeOutputGraph(GiraphConfiguration conf) {
    OUTPUT_GRAPH = new TestGraph(conf);
  }

  /**
   * Get graph containing all the vertices
   *
   * @param <I> Vertex id
   * @param <V> Vertex data
   * @param <E> Edge data
   * @return Output graph
   */
  public static <I extends WritableComparable, V extends Writable,
      E extends Writable> TestGraph<I, V, E> getOutputGraph() {
    return OUTPUT_GRAPH;
  }

  @Override
  public VertexWriter<I, V, E> createVertexWriter(
      TaskAttemptContext context) throws IOException, InterruptedException {
    return new VertexWriter<I, V, E>() {
      @Override
      public void initialize(
          TaskAttemptContext context) throws IOException, InterruptedException {
      }

      @Override
      public void close(
          TaskAttemptContext context) throws IOException, InterruptedException {
      }

      @Override
      public void writeVertex(
          Vertex<I, V, E> vertex) throws IOException, InterruptedException {
        synchronized (OUTPUT_GRAPH) {
          OUTPUT_GRAPH.addVertex(vertex);
        }
      }
    };
  }

  @Override
  public void checkOutputSpecs(
      JobContext context) throws IOException, InterruptedException {
  }

  @Override
  public OutputCommitter getOutputCommitter(
      TaskAttemptContext context) throws IOException, InterruptedException {
    return new ImmutableOutputCommitter();
  }
}
