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

package org.apache.giraph.mapping;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * An entry in MappingStore
 *
 * @param <I> vertexId type
 * @param <B> mappingTarget type
 */
public class MappingEntry<I extends WritableComparable, B extends Writable> {
  /** Vertex Id */
  private I vertexId;
  /** Mapping Target */
  private B mappingTarget;

  /**
   * Constructor
   *
   * @param vertexId vertexId
   * @param mappingTarget mappingTarget
   */
  public MappingEntry(I vertexId, B mappingTarget) {
    this.vertexId = vertexId;
    this.mappingTarget = mappingTarget;
  }

  public I getVertexId() {
    return vertexId;
  }

  public B getMappingTarget() {
    return mappingTarget;
  }

  public void setVertexId(I vertexId) {
    this.vertexId = vertexId;
  }

  public void setMappingTarget(B mappingTarget) {
    this.mappingTarget = mappingTarget;
  }
}
