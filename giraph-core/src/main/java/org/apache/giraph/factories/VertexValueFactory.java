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

package org.apache.giraph.factories;

import org.apache.hadoop.io.Writable;

/**
 * Factory class to create default vertex values.
 * A user can extend this class in order to customize the creation of new
 * vertex values when a vertex is created by the infrastructure
 * (e.g., if edges for a vertex are read).
 *
 * @param <V> Vertex value
 */
public interface VertexValueFactory<V extends Writable>
    extends ValueFactory<V> {
}
