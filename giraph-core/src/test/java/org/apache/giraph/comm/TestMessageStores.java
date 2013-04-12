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

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.messages.BasicMessageStore;
import org.apache.giraph.comm.messages.ByteArrayMessagesPerVertexStore;
import org.apache.giraph.comm.messages.DiskBackedMessageStore;
import org.apache.giraph.comm.messages.DiskBackedMessageStoreByPartition;
import org.apache.giraph.comm.messages.FlushableMessageStore;
import org.apache.giraph.comm.messages.MessageStore;
import org.apache.giraph.comm.messages.MessageStoreFactory;
import org.apache.giraph.comm.messages.SequentialFileMessageStore;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.ByteArrayVertexIdMessages;
import org.apache.giraph.utils.CollectionUtils;
import org.apache.giraph.utils.MockUtils;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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

import static org.junit.Assert.assertTrue;

/** Test for different types of message stores */
public class TestMessageStores {
  private static File directory;
  private static ImmutableClassesGiraphConfiguration config;
  private static TestData testData;
  private static
  CentralizedServiceWorker<IntWritable, IntWritable, IntWritable, IntWritable>
      service;
  /**
   * Pseudo-random number generator with the same seed to help with
   * debugging)
   */
  private static final Random RANDOM = new Random(101);

  private static class IntVertex extends Vertex<IntWritable,
        IntWritable, IntWritable, IntWritable> {

    @Override
    public void compute(Iterable<IntWritable> messages) throws IOException {
    }
  }

  @Before
  public void prepare() throws IOException {
    directory = Files.createTempDir();

    Configuration.addDefaultResource("giraph-site.xml");
    GiraphConfiguration initConfig = new GiraphConfiguration();
    initConfig.setVertexClass(IntVertex.class);
    GiraphConstants.MESSAGES_DIRECTORY.set(
        initConfig, new File(directory, "giraph_messages").toString());
    config = new ImmutableClassesGiraphConfiguration(initConfig);

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
  public void cleanUp() throws IOException {
    FileUtils.deleteDirectory(directory);
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

  /**
   * Used for testing only
   */
  private static class InputMessageStore extends
      ByteArrayMessagesPerVertexStore<IntWritable, IntWritable> {

    /**
     * Constructor
     *
     * @param service Service worker
     * @param config  Hadoop configuration
     */
    InputMessageStore(
        CentralizedServiceWorker<IntWritable, ?, ?, IntWritable> service,
        ImmutableClassesGiraphConfiguration<IntWritable, ?, ?,
            IntWritable> config,
        Map<IntWritable, Collection<IntWritable>> inputMap) throws IOException {
      super(service, config);
      // Adds all the messages to the store
      for (Map.Entry<IntWritable, Collection<IntWritable>> entry :
          inputMap.entrySet()) {
        int partitionId = getPartitionId(entry.getKey());
        ByteArrayVertexIdMessages<IntWritable, IntWritable>
            byteArrayVertexIdMessages =
            new ByteArrayVertexIdMessages<IntWritable, IntWritable>();
        byteArrayVertexIdMessages.setConf(config);
        byteArrayVertexIdMessages.initialize();
        for (IntWritable message : entry.getValue()) {
          byteArrayVertexIdMessages.add(entry.getKey(), message);
        }
        try {
          addPartitionMessages(partitionId, byteArrayVertexIdMessages);
        } catch (IOException e) {
          throw new IllegalStateException("Got IOException", e);
        }
      }
    }
  }

  private void putNTimes(
      MessageStore<IntWritable, IntWritable> messageStore,
      Map<IntWritable, Collection<IntWritable>> messages,
      TestData testData) throws IOException {
    for (int n = 0; n < testData.numTimes; n++) {
      SortedMap<IntWritable, Collection<IntWritable>> batch =
          createRandomMessages(testData);
      messageStore.addMessages(new InputMessageStore(service, config,
          batch));
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
      Map<I, Collection<M>> expectedMessages) throws IOException {
    TreeSet<I> vertexIds = Sets.newTreeSet();
    Iterables.addAll(vertexIds, messageStore.getDestinationVertices());
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
    return true;
  }

  private <S extends MessageStore<IntWritable, IntWritable>> S doCheckpoint(
      MessageStoreFactory<IntWritable, IntWritable, S> messageStoreFactory,
      S messageStore) throws IOException {
    File file = new File(directory, "messageStoreTest");
    if (file.exists()) {
      file.delete();
    }
    file.createNewFile();
    DataOutputStream out = new DataOutputStream(new BufferedOutputStream(
        (new FileOutputStream(file))));
    messageStore.write(out);
    out.close();

    messageStore = messageStoreFactory.newStore();

    DataInputStream in = new DataInputStream(new BufferedInputStream(
        (new FileInputStream(file))));
    messageStore.readFields(in);
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
    S messageStore = messageStoreFactory.newStore();
    putNTimes(messageStore, messages, testData);
    assertTrue(equalMessages(messageStore, messages));
    messageStore.clearAll();
    messageStore = doCheckpoint(messageStoreFactory, messageStore);
    assertTrue(equalMessages(messageStore, messages));
    messageStore.clearAll();
  }

  @Test
  public void testByteArrayMessagesPerVertexStore() {
    try {
      testMessageStore(
          ByteArrayMessagesPerVertexStore.newFactory(service, config),
          testData);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testDiskBackedMessageStore() {
    try {
      MessageStoreFactory<IntWritable, IntWritable,
          BasicMessageStore<IntWritable, IntWritable>> fileStoreFactory =
          SequentialFileMessageStore.newFactory(config);
      MessageStoreFactory<IntWritable, IntWritable,
          FlushableMessageStore<IntWritable, IntWritable>> diskStoreFactory =
          DiskBackedMessageStore.newFactory(config, fileStoreFactory);
      testMessageStore(diskStoreFactory, testData);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testDiskBackedMessageStoreByPartition() {
    try {
      MessageStoreFactory<IntWritable, IntWritable,
          BasicMessageStore<IntWritable, IntWritable>> fileStoreFactory =
          SequentialFileMessageStore.newFactory(config);
      MessageStoreFactory<IntWritable, IntWritable,
          FlushableMessageStore<IntWritable, IntWritable>> diskStoreFactory =
          DiskBackedMessageStore.newFactory(config, fileStoreFactory);
      testMessageStore(DiskBackedMessageStoreByPartition.newFactory(service,
          testData.maxMessagesInMemory, diskStoreFactory), testData);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
