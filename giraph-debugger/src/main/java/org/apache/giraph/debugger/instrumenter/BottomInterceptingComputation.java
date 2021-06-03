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

import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.GraphTaskManager;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * The intercepting Computation class to be instrumented as one that extends
 * user's actual Computation class, and run by Graft for debugging.
 *
 * @param <I> Vertex id type.
 * @param <V> Vertex value type.
 * @param <E> Edge value type.
 * @param <M1> Incoming message type.
 * @param <M2> Outgoing message type.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class BottomInterceptingComputation<I extends WritableComparable,
  V extends Writable, E extends Writable, M1 extends Writable,
  M2 extends Writable> extends UserComputation<I, V, E, M1, M2> {

  /**
   * A flag to quickly decide whether to skip intercepting compute().
   */
  private boolean shouldStopInterceptingCompute;

  @Intercept
  @Override
  public void initialize(GraphState graphState,
    WorkerClientRequestProcessor<I, V, E> workerClientRequestProcessor,
    GraphTaskManager<I, V, E> graphTaskManager,
    WorkerGlobalCommUsage workerGlobalCommUsage, WorkerContext workerContext) {
    try {
      // We first call super.initialize so that the getConf() call below
      // returns a non-null value.
      super.initialize(graphState, workerClientRequestProcessor,
        graphTaskManager, workerGlobalCommUsage, workerContext);
    } finally {
      if (!AbstractInterceptingComputation.IS_INITIALIZED) { // short circuit
        initializeAbstractInterceptingComputation();
      }
    }
  }

  @Intercept
  @Override
  public void preSuperstep() {
    shouldStopInterceptingCompute = interceptPreSuperstepBegin();
    super.preSuperstep();
  }

  @Intercept
  @Override
  public final void compute(Vertex<I, V, E> vertex, Iterable<M1> messages)
    throws IOException {
    if (shouldStopInterceptingCompute) {
      super.compute(vertex, messages);
    } else {
      interceptComputeBegin(vertex, messages);
      if (AbstractInterceptingComputation.SHOULD_CATCH_EXCEPTIONS) {
        // CHECKSTYLE: stop IllegalCatch
        try {
          super.compute(vertex, messages);
        } catch (Throwable e) {
          interceptComputeException(vertex, messages, e);
          throw e;
        }
        // CHECKSTYLE: resume IllegalCatch
      } else {
        super.compute(vertex, messages);
      }
      shouldStopInterceptingCompute = interceptComputeEnd(vertex, messages);
    }
  }

  @Intercept
  @Override
  public void postSuperstep() {
    super.postSuperstep();
    interceptPostSuperstepEnd();
  }

  @Override
  public Class getActualTestedClass() {
    return getClass();
  }

}
