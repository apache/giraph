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
import org.apache.giraph.combiner.DoubleSumMessageCombiner;
import org.apache.giraph.comm.messages.primitives.IdByteArrayMessageStore;
import org.apache.giraph.comm.messages.primitives.LongDoubleMessageStore;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.factories.TestMessageValueFactory;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.partition.Partition;
import org.apache.giraph.partition.PartitionStore;
import org.apache.giraph.utils.ByteArrayVertexIdMessages;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class TestLongDoublePrimitiveMessageStores {
  private static final int NUM_PARTITIONS = 2;
  private static CentralizedServiceWorker<LongWritable, Writable, Writable>
    service;

  @Before
  public void prepare() {
    service = Mockito.mock(CentralizedServiceWorker.class);
    Mockito.when(
        service.getPartitionId(Mockito.any(LongWritable.class))).thenAnswer(
        new Answer<Integer>() {
          @Override
          public Integer answer(InvocationOnMock invocation) {
            LongWritable vertexId = (LongWritable) invocation.getArguments()[0];
            return (int) (vertexId.get() % NUM_PARTITIONS);
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
  }

  private static class LongDoubleNoOpComputation extends
      BasicComputation<LongWritable, NullWritable, NullWritable,
          DoubleWritable> {
    @Override
    public void compute(Vertex<LongWritable, NullWritable, NullWritable> vertex,
        Iterable<DoubleWritable> messages) throws IOException {
    }
  }

  private static ImmutableClassesGiraphConfiguration<LongWritable, Writable,
    Writable> createLongDoubleConf() {

    GiraphConfiguration initConf = new GiraphConfiguration();
    initConf.setComputationClass(LongDoubleNoOpComputation.class);
    return new ImmutableClassesGiraphConfiguration(initConf);
  }

  private static ByteArrayVertexIdMessages<LongWritable, DoubleWritable>
  createLongDoubleMessages() {
    ByteArrayVertexIdMessages<LongWritable, DoubleWritable> messages =
        new ByteArrayVertexIdMessages<LongWritable, DoubleWritable>(
            new TestMessageValueFactory<DoubleWritable>(DoubleWritable.class));
    messages.setConf(createLongDoubleConf());
    messages.initialize();
    return messages;
  }

  private static void insertLongDoubleMessages(
      MessageStore<LongWritable, DoubleWritable> messageStore) {
    ByteArrayVertexIdMessages<LongWritable, DoubleWritable> messages =
        createLongDoubleMessages();
    messages.add(new LongWritable(0), new DoubleWritable(1));
    messages.add(new LongWritable(2), new DoubleWritable(3));
    messages.add(new LongWritable(0), new DoubleWritable(4));
    messageStore.addPartitionMessages(0, messages);
    messages = createLongDoubleMessages();
    messages.add(new LongWritable(1), new DoubleWritable(1));
    messages.add(new LongWritable(1), new DoubleWritable(3));
    messages.add(new LongWritable(1), new DoubleWritable(4));
    messageStore.addPartitionMessages(1, messages);
    messages = createLongDoubleMessages();
    messages.add(new LongWritable(0), new DoubleWritable(5));
    messageStore.addPartitionMessages(0, messages);
  }

  @Test
  public void testLongDoubleMessageStore() {
    LongDoubleMessageStore messageStore =
        new LongDoubleMessageStore(service, new DoubleSumMessageCombiner());
    insertLongDoubleMessages(messageStore);

    Iterable<DoubleWritable> m0 =
        messageStore.getVertexMessages(new LongWritable(0));
    Assert.assertEquals(1, Iterables.size(m0));
    Assert.assertEquals(10.0, m0.iterator().next().get());
    Iterable<DoubleWritable> m1 =
        messageStore.getVertexMessages(new LongWritable(1));
    Assert.assertEquals(1, Iterables.size(m1));
    Assert.assertEquals(8.0, m1.iterator().next().get());
    Iterable<DoubleWritable> m2 =
        messageStore.getVertexMessages(new LongWritable(2));
    Assert.assertEquals(1, Iterables.size(m2));
    Assert.assertEquals(3.0, m2.iterator().next().get());
    Assert.assertTrue(
        Iterables.isEmpty(messageStore.getVertexMessages(new LongWritable(3))));
  }

  @Test
  public void testLongByteArrayMessageStore() {
    IdByteArrayMessageStore<LongWritable, DoubleWritable> messageStore =
        new IdByteArrayMessageStore<>(
            new TestMessageValueFactory<DoubleWritable>(DoubleWritable.class),
            service, createLongDoubleConf());
    insertLongDoubleMessages(messageStore);

    Iterable<DoubleWritable> m0 =
        messageStore.getVertexMessages(new LongWritable(0));
    Assert.assertEquals(3, Iterables.size(m0));
    Iterator<DoubleWritable> i0 = m0.iterator();
    Assert.assertEquals(1.0, i0.next().get());
    Assert.assertEquals(4.0, i0.next().get());
    Assert.assertEquals(5.0, i0.next().get());
    Iterable<DoubleWritable> m1 =
        messageStore.getVertexMessages(new LongWritable(1));
    Assert.assertEquals(3, Iterables.size(m1));
    Iterator<DoubleWritable> i1 = m1.iterator();
    Assert.assertEquals(1.0, i1.next().get());
    Assert.assertEquals(3.0, i1.next().get());
    Assert.assertEquals(4.0, i1.next().get());
    Iterable<DoubleWritable> m2 =
        messageStore.getVertexMessages(new LongWritable(2));
    Assert.assertEquals(1, Iterables.size(m2));
    Assert.assertEquals(3.0, m2.iterator().next().get());
    Assert.assertTrue(
        Iterables.isEmpty(messageStore.getVertexMessages(new LongWritable(3))));
  }
}
