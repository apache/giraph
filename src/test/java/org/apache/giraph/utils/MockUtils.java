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

package org.apache.giraph.utils;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.WorkerClientServer;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.partition.BasicPartitionOwner;
import org.apache.giraph.graph.partition.PartitionOwner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/** simplify mocking for unit testing vertices */
public class MockUtils {

    private MockUtils() {
    }

    /**
     * mocks and holds  "environment objects" that are injected into a vertex
     *
     * @param <I> vertex id
     * @param <V> vertex data
     * @param <E> edge data
     * @param <M> message data
     */
    public static class MockedEnvironment<I extends WritableComparable,
            V extends Writable, E extends Writable, M extends Writable> {

        private final GraphState<I, V, E, M> graphState;
        private final Mapper.Context context;
        private final Configuration conf;
        private final WorkerClientServer communications;

        public MockedEnvironment() {
            graphState = Mockito.mock(GraphState.class);
            context = Mockito.mock(Mapper.Context.class);
            conf = Mockito.mock(Configuration.class);
            communications = Mockito.mock(WorkerClientServer.class);
        }

        /** the injected graph state */
        public GraphState getGraphState() {
            return graphState;
        }

        /** the injected mapper context  */
        public Mapper.Context getContext() {
            return context;
        }

        /** the injected hadoop configuration */
        public Configuration getConfiguration() {
            return conf;
        }

        /** the injected worker communications */
        public WorkerClientServer getCommunications() {
            return communications;
        }

        /** assert that the test vertex message has been sent to a particular vertex */
        public void verifyMessageSent(I targetVertexId, M message) {
            Mockito.verify(communications).sendMessageRequest(targetVertexId,
                message);
        }

        /** assert that the test vertex has sent no message to a particular vertex */
        public void verifyNoMessageSent() {
            Mockito.verifyZeroInteractions(communications);
        }
    }

    /**
     * prepare a vertex for use in a unit test by setting its internal state and injecting mocked
     * dependencies,
     *
     * @param vertex
     * @param superstep the superstep to emulate
     * @param vertexId initial vertex id
     * @param vertexValue initial vertex value
     * @param isHalted initial halted state of the vertex
     * @param <I> vertex id
     * @param <V> vertex data
     * @param <E> edge data
     * @param <M> message data
     * @return
     * @throws Exception
     */
    public static <I extends WritableComparable, V extends Writable,
            E extends Writable, M extends Writable>
            MockedEnvironment<I, V, E, M> prepareVertex(
            Vertex<I, V, E, M> vertex, long superstep, I vertexId,
            V vertexValue, boolean isHalted) throws Exception {

        MockedEnvironment<I, V, E, M>  env =
                new MockedEnvironment<I, V, E, M>();

        Mockito.when(env.getGraphState().getSuperstep()).thenReturn(superstep);
        Mockito.when(env.getGraphState().getContext())
                .thenReturn(env.getContext());
        Mockito.when(env.getContext().getConfiguration())
                .thenReturn(env.getConfiguration());
        Mockito.when(env.getGraphState().getWorkerCommunications())
                .thenReturn(env.getCommunications());

        ReflectionUtils.setField(vertex, "id", vertexId);
        ReflectionUtils.setField(vertex, "value", vertexValue);
        ReflectionUtils.setField(vertex, "graphState", env.getGraphState());
        ReflectionUtils.setField(vertex, "halt", isHalted);

        return env;
    }

  public static CentralizedServiceWorker<IntWritable, IntWritable,
      IntWritable, IntWritable> mockServiceGetVertexPartitionOwner(final int
      numOfPartitions) {
    CentralizedServiceWorker<IntWritable, IntWritable, IntWritable,
        IntWritable> service = Mockito.mock(CentralizedServiceWorker.class);
    Answer<PartitionOwner> answer = new Answer<PartitionOwner>() {
      @Override
      public PartitionOwner answer(InvocationOnMock invocation) throws
          Throwable {
        IntWritable vertexId = (IntWritable) invocation.getArguments()[0];
        return new BasicPartitionOwner(vertexId.get() % numOfPartitions, null);
      }
    };
    Mockito.when(service.getVertexPartitionOwner(
        Mockito.any(IntWritable.class))).thenAnswer(answer);
    return service;
  }
}
