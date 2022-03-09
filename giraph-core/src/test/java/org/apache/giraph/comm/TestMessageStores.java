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

package org.apache.giraph.comm;

import static org.junit.Assert.assertTrue;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.messages.ByteArrayMessagesPerVertexStore;
import org.apache.giraph.comm.messages.MessageEncodeAndStoreType;
import org.apache.giraph.comm.messages.MessageStore;
import org.apache.giraph.comm.messages.MessageStoreFactory;
import org.apache.giraph.conf.DefaultMessageClasses;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.factories.DefaultMessageValueFactory;
import org.apache.giraph.factories.TestMessageValueFactory;
import org.apache.giraph.utils.ByteArrayVertexIdMessages;
import org.apache.giraph.utils.CollectionUtils;
import org.apache.giraph.utils.IntNoOpComputation;
import org.apache.giraph.utils.MockUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

/** Test for different types of message stores */
public class TestMessageStores {
  private static File directory;
  private static ImmutableClassesGiraphConfiguration<IntWritable,
      IntWritable, IntWritable> config;
  private static TestData testData;
  private static
  CentralizedServiceWorker<IntWritable, IntWritable, IntWritable> service;
  /**
   * Pseudo-random number generator with the same seed to help with
   * debugging)
   */
  private static final Random RANDOM = new Random(101);

  @Before
  public void prepare() {
    Configuration.addDefaultResource("giraph-site.xml");
    GiraphConfiguration initConfig = new GiraphConfiguration();
    initConfig.setComputationClass(IntNoOpComputation.class);
    config = new ImmutableClassesGiraphConfiguration<IntWritable,
        IntWritable, IntWritable>(initConfig);

    testData = new TestData();
    testData.maxId = 1000000;
    testData.maxMessage = 1000000;
    testData.maxNumberOfMessages = 100;
    testData.numVertices = 50;
    testData.numTimes = 10;
    testData.numOfPartitions = 5;
    testData.maxMessagesInMemory = 20;

    service =
        MockUtils.mockServiceGetVertexPartitionOwner(testData.numOfPartitions);
  }

  @After
  public void cleanUp() {
  }

  private static class TestData {
    int numTimes;
    int numVertices;
    int maxNumberOfMessages;
    int maxId;
    int maxMessage;
    int numOfPartitions;
    int maxMessagesInMemory;
  }

  private SortedMap<IntWritable, Collection<IntWritable>> createRandomMessages(
      TestData testData) {
    SortedMap<IntWritable, Collection<IntWritable>> allMessages =
        new TreeMap<IntWritable, Collection<IntWritable>>();
    for (int v = 0; v < testData.numVertices; v++) {
      int messageNum =
          (int) (RANDOM.nextFloat() * testData.maxNumberOfMessages);
      Collection<IntWritable> vertexMessages = Lists.newArrayList();
      for (int m = 0; m < messageNum; m++) {
        vertexMessages.add(
            new IntWritable((int) (RANDOM.nextFloat() * testData.maxMessage)));
      }
      IntWritable vertexId =
          new IntWritable((int) (RANDOM.nextFloat() * testData.maxId));
      allMessages.put(vertexId, vertexMessages);
    }
    return allMessages;
  }

  private static void addMessages(
      MessageStore<IntWritable, IntWritable> messageStore,
      CentralizedServiceWorker<IntWritable, ?, ?> service,
      ImmutableClassesGiraphConfiguration<IntWritable, ?, ?> config,
      Map<IntWritable, Collection<IntWritable>> inputMap) {
    for (Map.Entry<IntWritable, Collection<IntWritable>> entry :
        inputMap.entrySet()) {
      int partitionId =
          service.getVertexPartitionOwner(entry.getKey()).getPartitionId();
      ByteArrayVertexIdMessages<IntWritable, IntWritable>
          byteArrayVertexIdMessages =
          new ByteArrayVertexIdMessages<IntWritable, IntWritable>(
              new TestMessageValueFactory(IntWritable.class));
      byteArrayVertexIdMessages.setConf(config);
      byteArrayVertexIdMessages.initialize();
      for (IntWritable message : entry.getValue()) {
        byteArrayVertexIdMessages.add(entry.getKey(), message);
      }
      messageStore.addPartitionMessages(partitionId, byteArrayVertexIdMessages);
    }
  }

  private void putNTimes(
      MessageStore<IntWritable, IntWritable> messageStore,
      Map<IntWritable, Collection<IntWritable>> messages,
      TestData testData) {
    for (int n = 0; n < testData.numTimes; n++) {
      SortedMap<IntWritable, Collection<IntWritable>> batch =
          createRandomMessages(testData);
      addMessages(messageStore, service, config, batch);
      for (Entry<IntWritable, Collection<IntWritable>> entry :
          batch.entrySet()) {
        if (messages.containsKey(entry.getKey())) {
          messages.get(entry.getKey()).addAll(entry.getValue());
        } else {
          messages.put(entry.getKey(), entry.getValue());
        }
      }
    }
  }

  private <I extends WritableComparable, M extends Writable> boolean
  equalMessages(
      MessageStore<I, M> messageStore,
      Map<I, Collection<M>> expectedMessages,
      TestData testData) {
    for (int partitionId = 0; partitionId < testData.numOfPartitions;
         partitionId++) {
      TreeSet<I> vertexIds = Sets.newTreeSet();
      Iterables.addAll(vertexIds,
          messageStore.getPartitionDestinationVertices(partitionId));
      for (I vertexId : vertexIds) {
        Iterable<M> expected = expectedMessages.get(vertexId);
        if (expected == null) {
          return false;
        }
        Iterable<M> actual = messageStore.getVertexMessages(vertexId);
        if (!CollectionUtils.isEqual(expected, actual)) {
          System.err.println("equalMessages: For vertexId " + vertexId +
              " expected " + expected + ", but got " + actual);
          return false;
        }
      }
    }
    return true;
  }

  private <S extends MessageStore<IntWritable, IntWritable>> S doCheckpoint(
      MessageStoreFactory<IntWritable, IntWritable, S> messageStoreFactory,
      S messageStore, TestData testData) throws IOException {
    File file = new File(directory, "messageStoreTest");
    if (file.exists()) {
      file.delete();
    }
    file.createNewFile();
    DataOutputStream out = new DataOutputStream(new BufferedOutputStream(
        (new FileOutputStream(file))));
    for (int partitionId = 0; partitionId < testData.numOfPartitions;
         partitionId++) {
      messageStore.writePartition(out, partitionId);
    }
    out.close();

    messageStore = (S) messageStoreFactory.newStore(
        new DefaultMessageClasses(
            IntWritable.class,
            DefaultMessageValueFactory.class,
            null,
            MessageEncodeAndStoreType.BYTEARRAY_PER_PARTITION));

    DataInputStream in = new DataInputStream(new BufferedInputStream(
        (new FileInputStream(file))));
    for (int partitionId = 0; partitionId < testData.numOfPartitions;
         partitionId++) {
      messageStore.readFieldsForPartition(in, partitionId);
    }
    in.close();
    file.delete();

    return messageStore;
  }

  private <S extends MessageStore<IntWritable, IntWritable>> void
  testMessageStore(
      MessageStoreFactory<IntWritable, IntWritable, S> messageStoreFactory,
      TestData testData) throws IOException {
    SortedMap<IntWritable, Collection<IntWritable>> messages =
        new TreeMap<IntWritable, Collection<IntWritable>>();
    S messageStore = (S) messageStoreFactory.newStore(
        new DefaultMessageClasses(
            IntWritable.class,
            DefaultMessageValueFactory.class,
            null,
            MessageEncodeAndStoreType.BYTEARRAY_PER_PARTITION));
    putNTimes(messageStore, messages, testData);
    assertTrue(equalMessages(messageStore, messages, testData));
    messageStore.clearAll();
    messageStore = doCheckpoint(messageStoreFactory, messageStore, testData);
    assertTrue(equalMessages(messageStore, messages, testData));
    messageStore.clearAll();
  }

  @Test
  public void testByteArrayMessagesPerVertexStore() {
    try {
      testMessageStore(
          ByteArrayMessagesPerVertexStore.<IntWritable, IntWritable>newFactory(
              service, config),
          testData);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
