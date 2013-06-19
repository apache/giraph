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

import org.apache.giraph.conf.DefaultImmutableClassesGiraphConfigurable;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerAggregatorUsage;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Analogous to Hadoop's RecordReader for vertices.  Will read the
 * vertices from an input split.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
@SuppressWarnings("rawtypes")
public abstract class VertexReader<I extends WritableComparable,
    V extends Writable, E extends Writable> extends
    DefaultImmutableClassesGiraphConfigurable<I, V, E>
    implements WorkerAggregatorUsage {
  /** Aggregator usage for vertex reader */
  private WorkerAggregatorUsage workerAggregatorUsage;

  /**
   * Use the input split and context to setup reading the vertices.
   * Guaranteed to be called prior to any other function.
   *
   * @param inputSplit Input split to be used for reading vertices.
   * @param context Context from the task.
   * @throws IOException
   * @throws InterruptedException
   */
  public abstract void initialize(InputSplit inputSplit,
                                  TaskAttemptContext context)
    throws IOException, InterruptedException;

  /**
   * Set aggregator usage. It provides the functionality
   * of aggregation operation in reading a vertex.
   * It is invoked just after initialization.
   * E.g.,
   * vertexReader.initialize(inputSplit, context);
   * vertexReader.setAggregator(aggregatorUsage);
   * This method is only for use by the infrastructure.
   *
   * @param agg aggregator usage for vertex reader
   */
  public void setWorkerAggregatorUse(WorkerAggregatorUsage agg) {
    workerAggregatorUsage = agg;
  }

  /**
   *
   * @return false iff there are no more vertices
   * @throws IOException
   * @throws InterruptedException
   */
  public abstract boolean nextVertex() throws IOException,
      InterruptedException;

  /**
   * Get the current vertex.
   *
   * @return the current vertex which has been read.
   *         nextVertex() should be called first.
   * @throws IOException
   * @throws InterruptedException
   */
  public abstract Vertex<I, V, E> getCurrentVertex()
    throws IOException, InterruptedException;

  /**
   * Close this {@link VertexReader} to future operations.
   *
   * @throws IOException
   */
  public abstract void close() throws IOException;

  /**
   * How much of the input has the {@link VertexReader} consumed i.e.
   * has been processed by?
   *
   * @return Progress from <code>0.0</code> to <code>1.0</code>.
   * @throws IOException
   * @throws InterruptedException
   */
  public abstract float getProgress() throws IOException, InterruptedException;

  @Override
  public <A extends Writable> void aggregate(String name, A value) {
    workerAggregatorUsage.aggregate(name, value);
  }

  @Override
  public <A extends Writable> A getAggregatedValue(String name) {
    return workerAggregatorUsage.<A>getAggregatedValue(name);
  }
}
