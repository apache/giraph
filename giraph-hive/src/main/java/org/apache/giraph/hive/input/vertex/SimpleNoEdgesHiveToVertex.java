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

package org.apache.giraph.hive.input.vertex;

import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.facebook.hiveio.record.HiveReadableRecord;
import com.google.common.collect.ImmutableList;

/**
 * Simple implementation of {@link HiveToVertex} when each vertex is in the one
 * row of the input, and there are no edges in vertex input.
 *
 * @param <I> Vertex ID
 * @param <V> Vertex Value
 */
public abstract class SimpleNoEdgesHiveToVertex<I extends WritableComparable,
    V extends Writable> extends SimpleHiveToVertex<I, V, Writable> {
  @Override
  public final Iterable<Edge<I, Writable>> getEdges(HiveReadableRecord record) {
    return ImmutableList.of();
  }
}
