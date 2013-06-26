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

package org.apache.giraph.utils;

import org.apache.giraph.bsp.BspInputSplit;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * An input format that reads the input graph in memory. Used for unit tests.
 *
 * @param <I> The Input
 * @param <V> The vertex type
 * @param <E> The edge type
 */
public class InMemoryVertexInputFormat<I extends WritableComparable,
    V extends Writable, E extends Writable>
    extends VertexInputFormat<I, V, E> {
  /** The graph */
  private static TestGraph GRAPH;

  public static void setGraph(TestGraph graph) {
    InMemoryVertexInputFormat.GRAPH = graph;
  }

  public static TestGraph getGraph() {
    return GRAPH;
  }

  @Override public void checkInputSpecs(Configuration conf) { }

  @Override
  public List<InputSplit> getSplits(JobContext context, int minSplitCountHint)
    throws IOException, InterruptedException {
    // This is meaningless, the VertexReader will generate all the test
    // data.
    List<InputSplit> inputSplitList = new ArrayList<InputSplit>();
    for (int i = 0; i < minSplitCountHint; ++i) {
      inputSplitList.add(new BspInputSplit(i, minSplitCountHint));
    }
    return inputSplitList;
  }

  @Override
  public VertexReader<I, V, E> createVertexReader(InputSplit inputSplit,
      TaskAttemptContext context) throws IOException {
    return new InMemoryVertexReader();
  }

  /**
   * Simple in memory reader
   */
  private class InMemoryVertexReader extends VertexReader<I, V, E> {
    /** The iterator */
    private Iterator<Vertex<I, V, E>> vertexIterator;
    /** Current vertex */
    private Vertex<I, V, E> currentVertex;

    @Override
    public void initialize(InputSplit inputSplit,
                           TaskAttemptContext context) {
      vertexIterator = GRAPH.iterator();
    }

    @Override
    public boolean nextVertex() {
      if (vertexIterator.hasNext()) {
        currentVertex = vertexIterator.next();
        return true;
      }
      return false;
    }

    @Override
    public Vertex<I, V, E> getCurrentVertex() {
      return currentVertex;
    }

    @Override
    public void close() {
    }

    @Override
    public float getProgress() {
      return 0;
    }
  }
}
