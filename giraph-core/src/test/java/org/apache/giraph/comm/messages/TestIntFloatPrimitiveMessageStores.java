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

package org.apache.giraph.comm.messages;

import java.io.IOException;
import java.util.Iterator;

import junit.framework.Assert;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.combiner.FloatSumMessageCombiner;
import org.apache.giraph.comm.messages.primitives.IdByteArrayMessageStore;
import org.apache.giraph.comm.messages.primitives.IntFloatMessageStore;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.factories.TestMessageValueFactory;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.partition.Partition;
import org.apache.giraph.partition.PartitionStore;
import org.apache.giraph.utils.ByteArrayVertexIdMessages;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class TestIntFloatPrimitiveMessageStores {
  private static final int NUM_PARTITIONS = 2;
  private static CentralizedServiceWorker<IntWritable, Writable, Writable>
    service;
  private static ImmutableClassesGiraphConfiguration<IntWritable, Writable,
      Writable> conf;

  @Before
  public void prepare() {
    service = Mockito.mock(CentralizedServiceWorker.class);
    Mockito.when(
        service.getPartitionId(Mockito.any(IntWritable.class))).thenAnswer(
        new Answer<Integer>() {
          @Override
          public Integer answer(InvocationOnMock invocation) {
            IntWritable vertexId = (IntWritable) invocation.getArguments()[0];
            return vertexId.get() % NUM_PARTITIONS;
          }
        }
    );
    PartitionStore partitionStore = Mockito.mock(PartitionStore.class);
    Mockito.when(service.getPartitionStore()).thenReturn(partitionStore);
    Mockito.when(service.getPartitionIds()).thenReturn(
      Lists.newArrayList(0, 1));
    Mockito.when(partitionStore.getPartitionIds()).thenReturn(
      Lists.newArrayList(0, 1));
    Partition partition = Mockito.mock(Partition.class);
    Mockito.when(partition.getVertexCount()).thenReturn(Long.valueOf(1));
    Mockito.when(partitionStore.getNextPartition()).thenReturn(partition);
    Mockito.when(partitionStore.getNextPartition()).thenReturn(partition);

    GiraphConfiguration initConf = new GiraphConfiguration();
    initConf.setComputationClass(IntFloatNoOpComputation.class);
    conf = new ImmutableClassesGiraphConfiguration(initConf);
  }

  private static class IntFloatNoOpComputation extends
      BasicComputation<IntWritable, NullWritable, NullWritable,
          FloatWritable> {
    @Override
    public void compute(Vertex<IntWritable, NullWritable, NullWritable> vertex,
        Iterable<FloatWritable> messages) throws IOException {
    }
  }

  private static ByteArrayVertexIdMessages<IntWritable, FloatWritable>
  createIntFloatMessages() {
    ByteArrayVertexIdMessages<IntWritable, FloatWritable> messages =
        new ByteArrayVertexIdMessages<IntWritable, FloatWritable>(
            new TestMessageValueFactory<FloatWritable>(FloatWritable.class));
    messages.setConf(conf);
    messages.initialize();
    return messages;
  }

  private static void insertIntFloatMessages(
      MessageStore<IntWritable, FloatWritable> messageStore) {
    ByteArrayVertexIdMessages<IntWritable, FloatWritable> messages =
        createIntFloatMessages();
    messages.add(new IntWritable(0), new FloatWritable(1));
    messages.add(new IntWritable(2), new FloatWritable(3));
    messages.add(new IntWritable(0), new FloatWritable(4));
    messageStore.addPartitionMessages(0, messages);
    messages = createIntFloatMessages();
    messages.add(new IntWritable(1), new FloatWritable(1));
    messages.add(new IntWritable(1), new FloatWritable(3));
    messages.add(new IntWritable(1), new FloatWritable(4));
    messageStore.addPartitionMessages(1, messages);
    messages = createIntFloatMessages();
    messages.add(new IntWritable(0), new FloatWritable(5));
    messageStore.addPartitionMessages(0, messages);
  }

  @Test
  public void testIntFloatMessageStore() {
    IntFloatMessageStore messageStore =
        new IntFloatMessageStore(service, new FloatSumMessageCombiner());
    insertIntFloatMessages(messageStore);

    Iterable<FloatWritable> m0 =
        messageStore.getVertexMessages(new IntWritable(0));
    Assert.assertEquals(1, Iterables.size(m0));
    Assert.assertEquals((float) 10.0, m0.iterator().next().get());
    Iterable<FloatWritable> m1 =
        messageStore.getVertexMessages(new IntWritable(1));
    Assert.assertEquals(1, Iterables.size(m1));
    Assert.assertEquals((float) 8.0, m1.iterator().next().get());
    Iterable<FloatWritable> m2 =
        messageStore.getVertexMessages(new IntWritable(2));
    Assert.assertEquals(1, Iterables.size(m2));
    Assert.assertEquals((float) 3.0, m2.iterator().next().get());
    Assert.assertTrue(
        Iterables.isEmpty(messageStore.getVertexMessages(new IntWritable(3))));
  }

  @Test
  public void testIntByteArrayMessageStore() {
    IdByteArrayMessageStore<IntWritable, FloatWritable> messageStore =
        new IdByteArrayMessageStore<>(new
            TestMessageValueFactory<FloatWritable>(FloatWritable.class),
            service, conf);
    insertIntFloatMessages(messageStore);

    Iterable<FloatWritable> m0 =
        messageStore.getVertexMessages(new IntWritable(0));
    Assert.assertEquals(3, Iterables.size(m0));
    Iterator<FloatWritable> i0 = m0.iterator();
    Assert.assertEquals((float) 1.0, i0.next().get());
    Assert.assertEquals((float) 4.0, i0.next().get());
    Assert.assertEquals((float) 5.0, i0.next().get());
    Iterable<FloatWritable> m1 =
        messageStore.getVertexMessages(new IntWritable(1));
    Assert.assertEquals(3, Iterables.size(m1));
    Iterator<FloatWritable> i1 = m1.iterator();
    Assert.assertEquals((float) 1.0, i1.next().get());
    Assert.assertEquals((float) 3.0, i1.next().get());
    Assert.assertEquals((float) 4.0, i1.next().get());
    Iterable<FloatWritable> m2 =
        messageStore.getVertexMessages(new IntWritable(2));
    Assert.assertEquals(1, Iterables.size(m2));
    Assert.assertEquals((float) 3.0, m2.iterator().next().get());
    Assert.assertTrue(
        Iterables.isEmpty(messageStore.getVertexMessages(new IntWritable(3))));
  }

  @Test
  public void testIntByteArrayMessageStoreWithMessageEncoding() {
    GiraphConstants.USE_MESSAGE_SIZE_ENCODING.set(conf, true);
    testIntByteArrayMessageStore();
    GiraphConstants.USE_MESSAGE_SIZE_ENCODING.set(conf, false);
  }
}
