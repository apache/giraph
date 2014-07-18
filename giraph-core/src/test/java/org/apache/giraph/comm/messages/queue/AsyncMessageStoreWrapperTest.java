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

package org.apache.giraph.comm.messages.queue;

import org.apache.giraph.comm.messages.MessageStore;
import org.apache.giraph.factories.TestMessageValueFactory;
import org.apache.giraph.utils.ByteArrayVertexIdMessages;
import org.apache.giraph.utils.VertexIdMessages;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.junit.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Test case for AsyncMessageStoreWrapper
 */
public class AsyncMessageStoreWrapperTest {


  @Test
  public void testAsyncQueue() throws IOException {
    TestMessageStore store = new TestMessageStore();

    AsyncMessageStoreWrapper<LongWritable, IntWritable> queue =
        new AsyncMessageStoreWrapper<>(store,
        Arrays.asList(0, 1, 2, 3, 4), 2);

    for (int i = 0; i < 1000; i++) {
      queue.addPartitionMessages(i % 5, new ByteArrayVertexIdMessages<LongWritable, IntWritable>(new TestMessageValueFactory<>(IntWritable.class)));
    }

    queue.waitToComplete();

    assertArrayEquals(new int[] {200, 200, 200, 200, 200}, store.counters);

    queue.clearAll();
  }


  static class TestMessageStore implements MessageStore<LongWritable, IntWritable> {

    private int counters[] = new int[5];

    @Override
    public void addPartitionMessages(int partition, VertexIdMessages messages) throws IOException {
      assertNotNull(messages);
      counters[partition]++;
    }

    @Override
    public boolean isPointerListEncoding() {
      return false;
    }

    @Override
    public Iterable<IntWritable> getVertexMessages(LongWritable vertexId) throws IOException {
      return null;
    }

    @Override
    public void clearVertexMessages(LongWritable vertexId) throws IOException {

    }

    @Override
    public void clearAll() throws IOException {

    }

    @Override
    public boolean hasMessagesForVertex(LongWritable vertexId) {
      return false;
    }

    @Override
    public void finalizeStore() {

    }

    @Override
    public Iterable<LongWritable> getPartitionDestinationVertices(int partitionId) {
      return null;
    }

    @Override
    public void clearPartition(int partitionId) throws IOException {

    }

    @Override
    public void writePartition(DataOutput out, int partitionId) throws IOException {

    }

    @Override
    public void readFieldsForPartition(DataInput in, int partitionId) throws IOException {

    }
  }
}
