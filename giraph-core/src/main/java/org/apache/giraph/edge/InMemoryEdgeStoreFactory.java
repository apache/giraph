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

package org.apache.giraph.edge;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.primitives.IntEdgeStore;
import org.apache.giraph.edge.primitives.LongEdgeStore;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.Progressable;

/**
 * Edge store factory which produces message stores which hold all
 * edges in memory. It creates primitive edges stores when vertex id is
 * IntWritable or LongWritable
 *
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@SuppressWarnings("unchecked")
public class InMemoryEdgeStoreFactory<I extends WritableComparable,
  V extends Writable, E extends Writable>
  implements EdgeStoreFactory<I, V, E> {
  /** Service worker. */
  protected CentralizedServiceWorker<I, V, E> service;
  /** Giraph configuration. */
  protected ImmutableClassesGiraphConfiguration<I, V, E> conf;
  /** Progressable to report progress. */
  protected Progressable progressable;

  @Override
  public EdgeStore<I, V, E> newStore() {
    Class<I> vertexIdClass = conf.getVertexIdClass();
    EdgeStore<I, V, E> edgeStore;
    if (vertexIdClass.equals(IntWritable.class)) {
      edgeStore = (EdgeStore<I, V, E>) new IntEdgeStore<>(
          (CentralizedServiceWorker<IntWritable, V, E>) service,
          (ImmutableClassesGiraphConfiguration<IntWritable, V, E>) conf,
          progressable);
    } else if (vertexIdClass.equals(LongWritable.class)) {
      edgeStore = (EdgeStore<I, V, E>) new LongEdgeStore<>(
          (CentralizedServiceWorker<LongWritable, V, E>) service,
          (ImmutableClassesGiraphConfiguration<LongWritable, V, E>) conf,
          progressable);
    } else {
      edgeStore = new SimpleEdgeStore<>(service, conf, progressable);
    }
    return edgeStore;
  }

  @Override
  public void initialize(CentralizedServiceWorker<I, V, E> service,
    ImmutableClassesGiraphConfiguration<I, V, E> conf,
    Progressable progressable) {
    this.service = service;
    this.conf = conf;
    this.progressable = progressable;
  }
}
