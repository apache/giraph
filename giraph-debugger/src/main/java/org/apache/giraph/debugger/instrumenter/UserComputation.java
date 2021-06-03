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
package org.apache.giraph.debugger.instrumenter;

import java.io.IOException;

import org.apache.commons.lang.NotImplementedException;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * A dummy Computation class that will sit between the
 * {@link AbstractInterceptingComputation} class at the top and the
 * {@link BottomInterceptingComputation} at the bottom.
 *
 * author netj
 *
 * @param <I> Vertex id type.
 * @param <V> Vertex value type.
 * @param <E> Edge value type.
 * @param <M1> Incoming message type.
 * @param <M2> Outgoing message type.
 */
@SuppressWarnings("rawtypes")
public abstract class UserComputation<I extends WritableComparable,
  V extends Writable, E extends Writable,
  M1 extends Writable, M2 extends Writable>
  extends AbstractInterceptingComputation<I, V, E, M1, M2> {

  @Override
  public void compute(Vertex<I, V, E> vertex, Iterable<M1> messages)
    throws IOException {
    throw new NotImplementedException();
  }

}
