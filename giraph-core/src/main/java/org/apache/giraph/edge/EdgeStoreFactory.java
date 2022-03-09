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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.Progressable;

/**
 * Factory to create a new Edge Store
 * @param <I> vertex id
 * @param <V> vertex value
 * @param <E> edge value
 */
public interface EdgeStoreFactory<I extends WritableComparable,
  V extends Writable, E extends Writable> {

  /**
   * Creates new edge store.
   *
   * @return edge store
   */
  EdgeStore<I, V, E> newStore();

  /**
   * Implementation class should use this method of initialization
   * of any required internal state.
   *
   * @param service Service to get partition mappings
   * @param conf Configuration
   * @param progressable Progressable
   */
  void initialize(CentralizedServiceWorker<I, V, E> service,
    ImmutableClassesGiraphConfiguration<I, V, E> conf,
    Progressable progressable);
}
