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

package org.apache.giraph.io;

import java.io.IOException;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * An EdgeReader that creates the opposite direction edge for each edge read.
 * Used to create an undirected graph from a directed input.
 * This class is a decorator around any other EdgeReader.
 *
 * @param <I> Vertex id
 * @param <E> Edge Value
 */
public class ReverseEdgeDuplicator<I extends WritableComparable,
    E extends Writable> extends EdgeReader<I, E> {
  /** The underlying EdgeReader to wrap */
  private final EdgeReader<I, E> baseReader;

  /** Whether the reverse edge stored currently is valid */
  private boolean haveReverseEdge = true;
  /** Reverse of the edge last read */
  private Edge<I, E> reverseEdge;
  /** Reverse source of last edge, in other words last edge's target */
  private I reverseSourceId;

  /**
   * Constructor
   * @param baseReader EdgeReader to wrap
   */
  public ReverseEdgeDuplicator(EdgeReader<I, E> baseReader) {
    this.baseReader = baseReader;
  }

  /**
   * Get wrapped EdgeReader
   * @return EdgeReader
   */
  public EdgeReader<I, E> getBaseReader() {
    return baseReader;
  }

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext context)
    throws IOException, InterruptedException {
    baseReader.initialize(inputSplit, context);
    haveReverseEdge = true;
  }

  @Override
  public boolean nextEdge() throws IOException, InterruptedException {
    boolean result = true;
    if (haveReverseEdge) {
      result = baseReader.nextEdge();
      haveReverseEdge = false;
    } else {
      Edge<I, E> currentEdge = baseReader.getCurrentEdge();
      reverseSourceId = currentEdge.getTargetVertexId();
      reverseEdge = EdgeFactory.create(baseReader.getCurrentSourceId(),
          currentEdge.getValue());
      haveReverseEdge = true;
    }
    return result;
  }

  @Override
  public I getCurrentSourceId() throws IOException, InterruptedException {
    if (haveReverseEdge) {
      return reverseSourceId;
    } else {
      return baseReader.getCurrentSourceId();
    }
  }

  @Override
  public Edge<I, E> getCurrentEdge() throws IOException, InterruptedException {
    if (haveReverseEdge) {
      return reverseEdge;
    } else {
      return baseReader.getCurrentEdge();
    }
  }

  @Override
  public void close() throws IOException {
    baseReader.close();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return baseReader.getProgress();
  }
}
