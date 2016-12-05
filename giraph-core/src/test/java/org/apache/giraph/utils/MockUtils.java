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
import org.apache.giraph.comm.ServerData;
import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.comm.WorkerServer;
import org.apache.giraph.comm.messages.ByteArrayMessagesPerVertexStore;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.ArrayListEdges;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.partition.BasicPartitionOwner;
import org.apache.giraph.partition.PartitionOwner;
import org.apache.giraph.partition.SimplePartition;
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

        private final GraphState graphState;
        private final Mapper.Context context;
        private final Configuration conf;
        private final WorkerClientRequestProcessor workerClientRequestProcessor;

        public MockedEnvironment() {
            graphState = Mockito.mock(GraphState.class);
            context = Mockito.mock(Mapper.Context.class);
            conf = Mockito.mock(Configuration.class);
            workerClientRequestProcessor =
                Mockito.mock(WorkerClientRequestProcessor.class);
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
        public WorkerClientRequestProcessor getWorkerClientRequestProcessor() {
            return workerClientRequestProcessor;
        }

        /** assert that the test vertex message has been sent to a particular vertex */
        public void verifyMessageSent(I targetVertexId, M message) {
            Mockito.verify(workerClientRequestProcessor).sendMessageRequest
                (targetVertexId, message);
        }

        public void verifyMessageSentToAllEdges(Vertex<I, V, E> vertex, M message) {
          Mockito.verify(workerClientRequestProcessor).sendMessageToAllRequest(vertex, message);
      }

        /** assert that the test vertex has sent no message to a particular vertex */
        public void verifyNoMessageSent() {
            Mockito.verifyZeroInteractions(workerClientRequestProcessor);
        }
    }

    /**
     * prepare a vertex and computation for use in a unit test by setting its
     * internal state and injecting mocked dependencies,
     *
     * @param vertex Vertex
     * @param vertexId initial vertex id
     * @param vertexValue initial vertex value
     * @param isHalted initial halted state of the vertex
     * @param computation Computation
     * @param superstep Superstep
     * @param <I> vertex id
     * @param <V> vertex data
     * @param <E> edge data
     * @param <M> message data
     * @return
     * @throws Exception
     */
  public static <I extends WritableComparable, V extends Writable,
      E extends Writable, M extends Writable>
  MockedEnvironment<I, V, E, M> prepareVertexAndComputation(
      Vertex<I, V, E> vertex, I vertexId, V vertexValue, boolean isHalted,
      Computation<I, V, E, M, M> computation, long superstep) throws
      Exception {
    MockedEnvironment<I, V, E, M> env = new MockedEnvironment<I, V, E, M>();
    Mockito.when(env.getGraphState().getSuperstep()).thenReturn(superstep);
    Mockito.when(env.getGraphState().getContext())
        .thenReturn(env.getContext());
    Mockito.when(env.getContext().getConfiguration())
        .thenReturn(env.getConfiguration());
    computation.initialize(env.getGraphState(),
        env.getWorkerClientRequestProcessor(), null, null);

    GiraphConfiguration giraphConf = new GiraphConfiguration();
    giraphConf.setComputationClass(computation.getClass());
    giraphConf.setOutEdgesClass(ArrayListEdges.class);
    ImmutableClassesGiraphConfiguration<I, V, E> conf =
        new ImmutableClassesGiraphConfiguration<I, V, E>(giraphConf);
    computation.setConf(conf);

    vertex.setConf(conf);
    vertex.initialize(vertexId, vertexValue);
    if (isHalted) {
      vertex.voteToHalt();
    }

    return env;
  }

  /**
   * Prepare a mocked CentralizedServiceWorker.
   *
   * @param numOfPartitions The number of partitions
   * @return CentralizedServiceWorker
   */
  public static CentralizedServiceWorker<IntWritable, IntWritable,
      IntWritable> mockServiceGetVertexPartitionOwner(final int
      numOfPartitions) {
    CentralizedServiceWorker<IntWritable, IntWritable, IntWritable> service =
        Mockito.mock(CentralizedServiceWorker.class);
    Answer<PartitionOwner> answerOwner = new Answer<PartitionOwner>() {
      @Override
      public PartitionOwner answer(InvocationOnMock invocation) throws
          Throwable {
        IntWritable vertexId = (IntWritable) invocation.getArguments()[0];
        return new BasicPartitionOwner(vertexId.get() % numOfPartitions, null);
      }
    };
    Mockito.when(service.getVertexPartitionOwner(
      Mockito.any(IntWritable.class))).thenAnswer(answerOwner);

    Answer<Integer> answerId = new Answer<Integer>() {
      @Override
      public Integer answer(InvocationOnMock invocation) throws
          Throwable {
        IntWritable vertexId = (IntWritable) invocation.getArguments()[0];
        return vertexId.get() % numOfPartitions;
      }
    };
    Mockito.when(service.getPartitionId(
      Mockito.any(IntWritable.class))).thenAnswer(answerId);
    return service;
  }

  /**
   * Prepare a ServerData object.
   *
   * @param conf Configuration
   * @param context Context
   * @return ServerData
   */
  public static ServerData<IntWritable, IntWritable, IntWritable>
    createNewServerData(
    ImmutableClassesGiraphConfiguration conf, Mapper.Context context) {
    CentralizedServiceWorker<IntWritable, IntWritable, IntWritable> serviceWorker =
      MockUtils.mockServiceGetVertexPartitionOwner(1);
    GiraphConstants.MESSAGE_STORE_FACTORY_CLASS.set(conf,
        ByteArrayMessagesPerVertexStore.newFactory(serviceWorker, conf)
            .getClass());

    WorkerServer workerServer = Mockito.mock(WorkerServer.class);
    ServerData<IntWritable, IntWritable, IntWritable> serverData =
      new ServerData<IntWritable, IntWritable, IntWritable>(
          serviceWorker, workerServer, conf, context);
    // Here we add a partition to simulate the case that there is one partition.
    serverData.getPartitionStore().addPartition(new SimplePartition());
    return serverData;
  }
}
