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

import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.graph.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Used for writing and reading collection of messages to the disk. {@link
 * #addMessages(java.util.Map)} should be called only once with the messages
 * we want to store.
 * <p/>
 * It's optimized for retrieving messages in the natural order of vertex ids
 * they are sent to.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
public class SequentialFileMessageStore<I extends WritableComparable,
    M extends Writable> implements BasicMessageStore<I, M> {
  /** File in which we store data */
  private final File file;
  /** Configuration which we need for reading data */
  private final Configuration config;
  /** Buffer size to use when reading and writing files */
  private final int bufferSize;
  /** File input stream */
  private DataInputStream in;
  /** How many vertices do we have left to read in the file */
  private int verticesLeft;
  /** Id of currently read vertex */
  private I currentVertexId;

  /**
   * Stores message on the disk.
   *
   * @param config     Configuration used later for reading
   * @param bufferSize Buffer size to use when reading and writing
   * @param fileName   File in which we want to store messages
   * @throws IOException
   */
  public SequentialFileMessageStore(Configuration config, int bufferSize,
      String fileName) {
    this.config = config;
    this.bufferSize = bufferSize;
    file = new File(fileName);
  }

  @Override
  public void addMessages(Map<I, Collection<M>> messages) throws IOException {
    SortedMap<I, Collection<M>> map;
    if (!(messages instanceof SortedMap)) {
      map = Maps.newTreeMap();
      map.putAll(messages);
    } else {
      map = (SortedMap) messages;
    }
    writeToFile(map);
  }

  /**
   * Writes messages to its file.
   *
   * @param messages Messages to write
   * @throws IOException
   */
  private void writeToFile(SortedMap<I, Collection<M>> messages) throws
      IOException {
    if (file.exists()) {
      file.delete();
    }
    file.createNewFile();
    DataOutputStream out = null;

    try {
      out = new DataOutputStream(
          new BufferedOutputStream(new FileOutputStream(file), bufferSize));
      out.writeInt(messages.size());
      for (Entry<I, Collection<M>> entry : messages.entrySet()) {
        entry.getKey().write(out);
        out.writeInt(entry.getValue().size());
        for (M message : entry.getValue()) {
          message.write(out);
        }
      }
    } finally {
      if (out != null) {
        out.close();
      }
    }
  }

  /**
   * Reads messages for a vertex. It will find the messages only if all
   * previous reads used smaller vertex ids than this one - messages should
   * be retrieved in increasing order of vertex ids.
   *
   * @param vertexId Vertex id for which we want to get messages
   * @return Messages for the selected vertex, or empty list if not used
   *         correctly
   * @throws IOException
   */
  @Override
  public Collection<M> getVertexMessages(I vertexId) throws
      IOException {
    if (in == null) {
      startReading();
    }

    I nextVertexId = getCurrentVertexId();
    while (nextVertexId != null && vertexId.compareTo(nextVertexId) > 0) {
      nextVertexId = getNextVertexId();
    }

    if (nextVertexId == null || vertexId.compareTo(nextVertexId) < 0) {
      return Collections.emptyList();
    }
    return readMessagesForCurrentVertex();
  }

  @Override
  public void clearVertexMessages(I vertexId) throws IOException {
  }

  @Override
  public void clearAll() throws IOException {
    endReading();
    file.delete();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(file.length());
    FileInputStream input = new FileInputStream(file);
    try {
      byte[] buffer = new byte[bufferSize];
      while (true) {
        int length = input.read(buffer);
        if (length < 0) {
          break;
        }
        out.write(buffer, 0, length);
      }
    } finally {
      input.close();
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    FileOutputStream output = new FileOutputStream(file);
    try {
      long fileLength = in.readLong();
      byte[] buffer = new byte[bufferSize];
      for (long position = 0; position < fileLength; position += bufferSize) {
        int bytes = (int) Math.min(bufferSize, fileLength - position);
        in.readFully(buffer, 0, bytes);
        output.write(buffer);
      }
    } finally {
      output.close();
    }
  }

  /**
   * Prepare for reading
   *
   * @throws IOException
   */
  private void startReading() throws IOException {
    currentVertexId = null;
    in = new DataInputStream(
        new BufferedInputStream(new FileInputStream(file), bufferSize));
    verticesLeft = in.readInt();
  }

  /**
   * Gets current vertex id.
   * <p/>
   * If there is a vertex id whose messages haven't been read yet it
   * will return that vertex id, otherwise it will read and return the next
   * one.
   *
   * @return Current vertex id
   * @throws IOException
   */
  private I getCurrentVertexId() throws IOException {
    if (currentVertexId != null) {
      return currentVertexId;
    } else {
      return getNextVertexId();
    }
  }

  /**
   * Gets next vertex id.
   * <p/>
   * If there is a vertex whose messages haven't been read yet it
   * will read and skip over its messages to get to the next vertex.
   *
   * @return Next vertex id
   * @throws IOException
   */
  private I getNextVertexId() throws IOException {
    if (currentVertexId != null) {
      readMessagesForCurrentVertex();
    }
    if (verticesLeft == 0) {
      return null;
    }
    currentVertexId = BspUtils.<I>createVertexId(config);
    currentVertexId.readFields(in);
    return currentVertexId;
  }

  /**
   * Reads messages for current vertex.
   *
   * @return Messages for current vertex
   * @throws IOException
   */
  private Collection<M> readMessagesForCurrentVertex() throws IOException {
    int messagesSize = in.readInt();
    ArrayList<M> messages = Lists.newArrayList();
    for (int i = 0; i < messagesSize; i++) {
      M message = BspUtils.<M>createMessageValue(config);
      message.readFields(in);
      messages.add(message);
    }
    currentVertexDone();
    return messages;
  }

  /**
   * Release current vertex.
   *
   * @throws IOException
   */
  private void currentVertexDone() throws IOException {
    currentVertexId = null;
    verticesLeft--;
    if (verticesLeft == 0) {
      endReading();
    }
  }

  /**
   * Call when we are done reading, for closing files.
   *
   * @throws IOException
   */
  private void endReading() throws IOException {
    if (in != null) {
      in.close();
      in = null;
    }
  }


  /**
   * Create new factory for this message store
   *
   * @param config Hadoop configuration
   * @param <I>    Vertex id
   * @param <M>    Message data
   * @return Factory
   */
  public static <I extends WritableComparable, M extends Writable>
  MessageStoreFactory<I, M, BasicMessageStore<I, M>> newFactory(
      Configuration config) {
    return new Factory<I, M>(config);
  }

  /**
   * Factory for {@link SequentialFileMessageStore}
   *
   * @param <I> Vertex id
   * @param <M> Message data
   */
  private static class Factory<I extends WritableComparable,
      M extends Writable>
      implements MessageStoreFactory<I, M, BasicMessageStore<I, M>> {
    /** Hadoop configuration */
    private final Configuration config;
    /** Directory in which we'll keep necessary files */
    private final String directory;
    /** Buffer size to use when reading and writing */
    private final int bufferSize;
    /** Counter for created message stores */
    private final AtomicInteger storeCounter;

    /** @param config Hadoop configuration */
    public Factory(Configuration config) {
      this.config = config;
      String jobId = config.get("mapred.job.id", "Unknown Job");
      this.directory = config.get(GiraphJob.MESSAGES_DIRECTORY,
          GiraphJob.MESSAGES_DIRECTORY_DEFAULT) + jobId + File.separator;
      this.bufferSize = config.getInt(GiraphJob.MESSAGES_BUFFER_SIZE,
          GiraphJob.MESSAGES_BUFFER_SIZE_DEFAULT);
      storeCounter = new AtomicInteger();
      new File(directory).mkdirs();
    }

    @Override
    public BasicMessageStore<I, M> newStore() {
      String fileName = directory + storeCounter.getAndIncrement();
      return new SequentialFileMessageStore<I, M>(config, bufferSize,
          fileName);
    }
  }
}
