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

package org.apache.giraph.mapping.translate;

import org.apache.giraph.conf.ImmutableClassesGiraphConfigurable;
import org.apache.giraph.worker.BspServiceWorker;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Used to conduct expensive translation of edges
 * during vertex input phase
 *
 * @param <I> vertexId type
 * @param <E> edgeValue type
 */
public interface TranslateEdge<I extends WritableComparable, E extends Writable>
  extends ImmutableClassesGiraphConfigurable {
  /**
   * Must be called before other methods can be used
   *
   * @param service bsp service worker
   */
  void initialize(BspServiceWorker<I, ? extends Writable, E> service);

  /**
   * Translate Id &amp; return a new instance
   *
   * @param targetId edge target Id
   * @return a new translated Id instance
   */
  I translateId(I targetId);

  /**
   * Clone edge value
   *
   * @param edgeValue edge value
   * @return clone of edge value
   */
  E cloneValue(E edgeValue);
}
