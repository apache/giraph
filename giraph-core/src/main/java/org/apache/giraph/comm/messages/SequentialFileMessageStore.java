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

import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.EmptyIterable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.giraph.conf.GiraphConstants.MESSAGES_DIRECTORY;

/**
 * Used for writing and reading collection of messages to the disk. {@link
 * #addMessages(MessageStore<I, M>)} should be called only once with
 * the messages we want to store.
 * <p/>
 * It's optimized for retrieving messages in the natural order of vertex ids
 * they are sent to.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
public class SequentialFileMessageStore<I extends WritableComparable,
    M extends Writable> implements BasicMessageStore<I, M> {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(SequentialFileMessageStore.class);
  /** File in which we store data */
  private final File file;
  /** Configuration which we need for reading data */
  private final ImmutableClassesGiraphConfiguration<I, ?, ?, M> config;
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
  public SequentialFileMessageStore(
      ImmutableClassesGiraphConfiguration<I, ?, ?, M> config,
      int bufferSize,
      String fileName) {
    this.config = config;
    this.bufferSize = bufferSize;
    file = new File(fileName);
  }

  @Override
  public void addMessages(MessageStore<I, M> messageStore) throws IOException {
    // Writes messages to its file
    if (file.exists()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("addMessages: Deleting " + file);
      }
      file.delete();
    }
    file.createNewFile();
    if (LOG.isDebugEnabled()) {
      LOG.debug("addMessages: Creating " + file);
    }

    DataOutputStream out = null;

    try {
      out = new DataOutputStream(
          new BufferedOutputStream(new FileOutputStream(file), bufferSize));
      int destinationVertexIdCount =
          Iterables.size(messageStore.getDestinationVertices());
      out.writeInt(destinationVertexIdCount);

      // Since the message store messages might not be sorted, sort them if
      // necessary
      SortedSet<I> sortedSet;
      if (messageStore.getDestinationVertices() instanceof SortedSet) {
        sortedSet = (SortedSet<I>) messageStore.getDestinationVertices();
      } else {
        sortedSet =
            Sets.newTreeSet(messageStore.getDestinationVertices());
        for (I destinationVertexId : messageStore.getDestinationVertices()) {
          sortedSet.add(destinationVertexId);
        }
      }

      // Dump the vertices and their messages in a sorted order
      for (I destinationVertexId : sortedSet) {
        destinationVertexId.write(out);
        Iterable<M> messages =
            messageStore.getVertexMessages(destinationVertexId);
        int messageCount = Iterables.size(messages);
        out.writeInt(messageCount);
        if (LOG.isDebugEnabled()) {
          LOG.debug("addMessages: For vertex id " + destinationVertexId +
              ", messages = " + messageCount + " to file " + file);
        }
        for (M message : messages) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("addMessages: Wrote " + message + " to " + file);
          }
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
  public Iterable<M> getVertexMessages(I vertexId) throws
      IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("getVertexMessages: Reading for vertex id " + vertexId +
          " (currently " + currentVertexId + ") from " + file);
    }
    if (in == null) {
      startReading();
    }

    I nextVertexId = getCurrentVertexId();
    while (nextVertexId != null && vertexId.compareTo(nextVertexId) > 0) {
      nextVertexId = getNextVertexId();
    }

    if (nextVertexId == null || vertexId.compareTo(nextVertexId) < 0) {
      return EmptyIterable.get();
    }

    return readMessagesForCurrentVertex();
  }

  @Override
  public void clearVertexMessages(I vertexId) throws IOException { }

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
    if (LOG.isDebugEnabled()) {
      LOG.debug("startReading: File " + file + " with " +
          verticesLeft + " vertices left");
    }
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
    currentVertexId = config.createVertexId();
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
    List<M> messages = Lists.newArrayListWithCapacity(messagesSize);
    for (int i = 0; i < messagesSize; i++) {
      M message = config.createMessageValue();
      try {
        message.readFields(in);
      } catch (IOException e) {
        throw new IllegalStateException("readMessagesForCurrentVertex: " +
            "Failed to read message from " + i + " of " +
            messagesSize + " for vertex id " + currentVertexId + " from " +
            file, e);
      }
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
    if (LOG.isDebugEnabled()) {
      LOG.debug("endReading: Stopped reading " + file);
    }
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
      ImmutableClassesGiraphConfiguration config) {
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
    private final ImmutableClassesGiraphConfiguration config;
    /** Directories in which we'll keep necessary files */
    private final String[] directories;
    /** Buffer size to use when reading and writing */
    private final int bufferSize;
    /** Counter for created message stores */
    private final AtomicInteger storeCounter;

    /**
     * Constructor.
     *
     * @param config Hadoop configuration
     */
    public Factory(ImmutableClassesGiraphConfiguration config) {
      this.config = config;
      String jobId = config.get("mapred.job.id", "Unknown Job");
      int taskId   = config.getTaskPartition();
      List<String> userPaths = MESSAGES_DIRECTORY.getList(config);
      Collections.shuffle(userPaths);
      directories = new String[userPaths.size()];
      int i = 0;
      for (String path : userPaths) {
        String directory = path + File.separator + jobId + File.separator +
            taskId + File.separator;
        directories[i++] = directory;
        new File(directory).mkdirs();
      }
      this.bufferSize = GiraphConstants.MESSAGES_BUFFER_SIZE.get(config);
      storeCounter = new AtomicInteger();
    }

    @Override
    public BasicMessageStore<I, M> newStore() {
      int idx = Math.abs(storeCounter.getAndIncrement());
      String fileName =
          directories[idx % directories.length] + "messages-" + idx;
      return new SequentialFileMessageStore<I, M>(config, bufferSize,
          fileName);
    }
  }
}
