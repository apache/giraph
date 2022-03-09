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

package org.apache.giraph.io.iterables;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Wraps {@link GiraphReader} for vertices into {@link VertexReader}
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
public class VertexReaderWrapper<I extends WritableComparable,
    V extends Writable, E extends Writable> extends VertexReader<I, V, E> {
  /** Wrapped vertex reader */
  private GiraphReader<Vertex<I, V, E>> vertexReader;
  /** {@link VertexReader}-like wrapper of {@link #vertexReader} */
  private IteratorToReaderWrapper<Vertex<I, V, E>> iterator;

  /**
   * Constructor
   *
   * @param vertexReader GiraphReader for vertices to wrap
   */
  public VertexReaderWrapper(GiraphReader<Vertex<I, V, E>> vertexReader) {
    this.vertexReader = vertexReader;
    iterator = new IteratorToReaderWrapper<Vertex<I, V, E>>(vertexReader);
  }

  @Override
  public void setConf(
      ImmutableClassesGiraphConfiguration<I, V, E> conf) {
    super.setConf(conf);
    conf.configureIfPossible(vertexReader);
  }

  @Override
  public boolean nextVertex() throws IOException, InterruptedException {
    return iterator.nextObject();
  }

  @Override
  public Vertex<I, V, E> getCurrentVertex() throws IOException,
      InterruptedException {
    return iterator.getCurrentObject();
  }

  @Override
  public void initialize(InputSplit inputSplit,
      TaskAttemptContext context) throws IOException, InterruptedException {
    vertexReader.initialize(inputSplit, context);
  }

  @Override
  public void close() throws IOException {
    vertexReader.close();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return vertexReader.getProgress();
  }
}
