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

import java.io.IOException;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.io.EdgeReader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Wraps {@link GiraphReader} for edges into {@link EdgeReader}
 *
 * @param <I> Vertex id
 * @param <E> Edge data
 */
public class EdgeReaderWrapper<I extends WritableComparable,
    E extends Writable> extends EdgeReader<I, E> {
  /** Wrapped edge reader */
  private GiraphReader<EdgeWithSource<I, E>> edgeReader;
  /** {@link EdgeReader}-like wrapper of {@link #edgeReader} */
  private IteratorToReaderWrapper<EdgeWithSource<I, E>> iterator;

  /**
   * Constructor
   *
   * @param edgeReader GiraphReader for edges to wrap
   */
  public EdgeReaderWrapper(GiraphReader<EdgeWithSource<I, E>> edgeReader) {
    this.edgeReader = edgeReader;
    iterator = new IteratorToReaderWrapper<EdgeWithSource<I, E>>(edgeReader);
  }

  @Override
  public void setConf(
      ImmutableClassesGiraphConfiguration<I, Writable, E> conf) {
    super.setConf(conf);
    conf.configureIfPossible(edgeReader);
  }

  @Override
  public boolean nextEdge() throws IOException, InterruptedException {
    return iterator.nextObject();
  }

  @Override
  public I getCurrentSourceId() throws IOException, InterruptedException {
    return iterator.getCurrentObject().getSourceVertexId();
  }

  @Override
  public Edge<I, E> getCurrentEdge() throws IOException, InterruptedException {
    return iterator.getCurrentObject().getEdge();
  }

  @Override
  public void initialize(InputSplit inputSplit,
      TaskAttemptContext context) throws IOException, InterruptedException {
    edgeReader.initialize(inputSplit, context);
  }

  @Override
  public void close() throws IOException {
    edgeReader.close();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return edgeReader.getProgress();
  }
}
