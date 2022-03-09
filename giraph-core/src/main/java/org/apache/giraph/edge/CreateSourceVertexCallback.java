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

import org.apache.giraph.conf.GiraphConfigurationSettable;
import org.apache.hadoop.io.Writable;

/**
 * Implementations of this interface can decide whether
 * we should create a vertex when it is not present in vertex input
 * but exists in edge input.
 *
 * @param <I> vertex id
 */
public interface CreateSourceVertexCallback<I extends Writable>
    extends GiraphConfigurationSettable {

  /**
   * Should we create a vertex that doesn't exist in vertex input
   * but only exists in edge input
   * @param vertexId the id of vertex to be created
   * @return true if we should create a vertex
   */
  boolean shouldCreateSourceVertex(I vertexId);

}
