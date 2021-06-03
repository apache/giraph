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
package org.apache.giraph.block_app.framework.piece.interfaces;

import org.apache.giraph.writable.kryo.markers.NonKryoWritable;

/**
 * Interface containing a single function - postprocess.
 *
 * Marked to not allow seriazliation, as it should be created on the worker,
 * so should never be serialiized, disallow only for catching problems early.
 */
public interface VertexPostprocessor extends NonKryoWritable {
  /**
   * Override to finish computation. This method is executed exactly once
   * after computation for all vertices in the partition is complete.
   */
  void postprocess();
}
