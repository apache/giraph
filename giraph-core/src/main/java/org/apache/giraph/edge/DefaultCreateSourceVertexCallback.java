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

import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.Writable;

/**
 * Default implementation of vertex creation decision maker.
 * By default you can either create all vertices or not create
 * implicit vertices at all.
 *
 * @param <I> Vertex id
 */
public class DefaultCreateSourceVertexCallback<I extends Writable>
    implements CreateSourceVertexCallback<I> {
  /**
   * True if giraph has to create even vertices that only exist
   * in edge input
   */
  private boolean shouldCreateVertices;

  @Override
  public boolean shouldCreateSourceVertex(I vertexId) {
    return shouldCreateVertices;
  }

  @Override
  public void setConf(ImmutableClassesGiraphConfiguration configuration) {
    shouldCreateVertices =
        GiraphConstants.CREATE_EDGE_SOURCE_VERTICES.get(configuration);
  }
}
