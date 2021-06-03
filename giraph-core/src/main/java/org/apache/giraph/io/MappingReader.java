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

import org.apache.giraph.mapping.MappingEntry;
import org.apache.giraph.worker.WorkerAggregatorDelegator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Will read the mapping from an input split.
 *
 * @param <I> vertexId type
 * @param <V> vertexValue type
 * @param <E> edgeValue type
 * @param <B> mappingTarget type
 */
public abstract class MappingReader<I extends WritableComparable,
    V extends Writable, E extends Writable, B extends Writable>
    extends WorkerAggregatorDelegator<I, V, E> {
  /**
   * Use the input split and context to setup reading the vertices.
   * Guaranteed to be called prior to any other function.
   *
   * @param inputSplit Input split to be used for reading vertices.
   * @param context Context from the task.
   * @throws java.io.IOException
   * @throws InterruptedException
   */
  public abstract void initialize(InputSplit inputSplit,
    TaskAttemptContext context)
    throws IOException, InterruptedException;

  /**
   *
   * @return false iff there are no more vertices
   * @throws IOException
   * @throws InterruptedException
   */
  public abstract boolean nextEntry() throws IOException,
    InterruptedException;


  /**
   * Get the current entry.
   *
   * @return the current entry which has been read.
   *         nextVEntry() should be called first.
   * @throws IOException
   * @throws InterruptedException
   */
  public abstract MappingEntry<I, B> getCurrentEntry()
    throws IOException, InterruptedException;


  /**
   * Close this {@link MappingReader} to future operations.
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
}
