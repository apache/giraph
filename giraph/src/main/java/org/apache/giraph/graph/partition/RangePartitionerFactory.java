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

package org.apache.giraph.graph.partition;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Range partitioning will split the vertices by a key range based on a generic
 * type.  This allows vertices that have some locality with the vertex ids
 * to reduce the amount of messages sent.  The tradeoffs are that
 * range partitioning is more susceptible to hot spots if the keys
 * are not randomly distributed.  Another negative is the user must implement
 * some of the functionality around how to split the key range.
 *
 * See {@link RangeWorkerPartitioner}
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 * @param <M> Message value
 */
@SuppressWarnings("rawtypes")
public abstract class RangePartitionerFactory<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable>
    implements GraphPartitionerFactory<I, V, E, M> {
}
