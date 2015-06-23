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
package org.apache.giraph.block_app.test_setup;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Test graph modifier, function that initializes graph
 * (i.e. creates edges and vertices) for a given test.
 *
 * @param <I> Vertex id type
 * @param <V> Vertex value type
 * @param <E> Edge value type
 */
public interface TestGraphModifier<I extends WritableComparable,
    V extends Writable, E extends Writable> {
  void modifyGraph(NumericTestGraph<I, V, E> graph);
}
