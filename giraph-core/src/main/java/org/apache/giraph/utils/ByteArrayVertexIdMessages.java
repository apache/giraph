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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.giraph.conf.ImmutableClassesGiraphConfigurable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Stores vertex id and message pairs in a single byte array
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
public class ByteArrayVertexIdMessages<I extends WritableComparable,
    M extends Writable> implements Writable,
    ImmutableClassesGiraphConfigurable {
  /** Extended data output */
  private ExtendedDataOutput extendedDataOutput;
  /** Configuration */
  private ImmutableClassesGiraphConfiguration<I, ?, ?, M> configuration;
  /** Add the message size to the stream? (Depends on the message store) */
  private boolean useMessageSizeEncoding = false;

  /**
   * Constructor for reflection
   */
  public ByteArrayVertexIdMessages() { }

  /**
   * Set whether message sizes should be encoded.  This should only be a
   * possibility when not combining.  When combining, all messages need to be
   * deserializd right away, so this won't help.
   */
  private void setUseMessageSizeEncoding() {
    if (!configuration.useCombiner()) {
      useMessageSizeEncoding = configuration.useMessageSizeEncoding();
    } else {
      useMessageSizeEncoding = false;
    }
  }

  /**
   * Initialize the inner state. Must be called before {@code add()} is
   * called.
   */
  public void initialize() {
    extendedDataOutput = configuration.createExtendedDataOutput();
    setUseMessageSizeEncoding();
  }

  /**
   * Initialize the inner state, with a known size. Must be called before
   * {@code add()} is called.
   *
   * @param expectedSize Number of bytes to be expected
   */
  public void initialize(int expectedSize) {
    extendedDataOutput = configuration.createExtendedDataOutput(expectedSize);
    setUseMessageSizeEncoding();
  }

  /**
   * Add a vertex id and message pair to the collection.
   *
   * @param vertexId Vertex id
   * @param message Message
   */
  public void add(I vertexId, M message) {
    try {
      vertexId.write(extendedDataOutput);
      // Write the size if configured this way, else, just the message
      if (useMessageSizeEncoding) {
        int pos = extendedDataOutput.getPos();
        extendedDataOutput.skipBytes(4);
        message.write(extendedDataOutput);
        extendedDataOutput.writeInt(
            pos, extendedDataOutput.getPos() - pos - 4);
      } else {
        message.write(extendedDataOutput);
      }
    } catch (IOException e) {
      throw new IllegalStateException("add: IOException", e);
    }
  }

  /**
   * Get the number of bytes used
   *
   * @return Number of bytes used
   */
  public int getSize() {
    return extendedDataOutput.getPos();
  }

  /**
   * Check if the list is empty.
   *
   * @return True iff there are no pairs in the list
   */
  public boolean isEmpty() {
    return extendedDataOutput.getPos() == 0;
  }

  /**
   * Clear the collection.
   */
  public void clear() {
    extendedDataOutput.reset();
  }

  /**
   * Get specialized iterator that will instiantiate the vertex id and
   * message of this object.
   *
   * @return Special iterator that reuses vertex ids and messages unless
   *         specified
   */
  public VertexIdMessageIterator getVertexIdMessageIterator() {
    return new VertexIdMessageIterator();
  }

  /**
   * Get specialized iterator that will instiantiate the vertex id and
   * message of this object.  It will only produce message bytes, not actual
   * messages and expects a different encoding.
   *
   * @return Special iterator that reuses vertex ids (unless released) and
   *         copies message bytes
   */
  public VertexIdMessageBytesIterator getVertexIdMessageBytesIterator() {
    if (!useMessageSizeEncoding) {
      return null;
    }
    return new VertexIdMessageBytesIterator();
  }

  @Override
  public void setConf(ImmutableClassesGiraphConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  public ImmutableClassesGiraphConfiguration getConf() {
    return configuration;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeBoolean(useMessageSizeEncoding);
    dataOutput.writeInt(extendedDataOutput.getPos());
    dataOutput.write(extendedDataOutput.getByteArray(), 0,
        extendedDataOutput.getPos());
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    useMessageSizeEncoding = dataInput.readBoolean();
    int size = dataInput.readInt();
    byte[] buf = new byte[size];
    dataInput.readFully(buf);
    extendedDataOutput = configuration.createExtendedDataOutput(buf, size);
  }

  /**
   * Get the size of this object in serialized form.
   *
   * @return The size (in bytes) of serialized object
   */
  public int getSerializedSize() {
    return 1 + 4 + getSize();
  }

  /**
   * Common implementation for VertexIdMessageIterator
   * and VertexIdMessageBytesIterator
   */
  public abstract class VertexIdIterator {
    /** Reader of the serialized messages */
    protected final ExtendedDataInput extendedDataInput =
        configuration.createExtendedDataInput(
            extendedDataOutput.getByteArray(), 0, extendedDataOutput.getPos());
    /** Current vertex id */
    protected I vertexId;

    /**
     * Returns true if the iteration has more elements.
     *
     * @return True if the iteration has more elements.
     */
    public boolean hasNext() {
      return extendedDataInput.available() > 0;
    }
    /**
     * Moves to the next element in the iteration.
     */
    public abstract void next();

    /**
     * Get the current vertex id.  Ihis object's contents are only guaranteed
     * until next() is called.  To take ownership of this object call
     * releaseCurrentVertexId() after getting a reference to this object.
     *
     * @return Current vertex id
     */
    public I getCurrentVertexId() {
      return vertexId;
    }
    /**
     * The backing store of the current vertex id is now released.
     * Further calls to getCurrentVertexId () without calling next()
     * will return null.
     *
     * @return Current vertex id that was released
     */
    public I releaseCurrentVertexId() {
      I releasedVertexId = vertexId;
      vertexId = null;
      return releasedVertexId;
    }
  }

  /**
   * Special iterator that reuses vertex ids and messages so that the
   * lifetime of the object is only until next() is called.
   *
   * Vertex id ownership can be released if desired through
   * releaseCurrentVertexId().  This optimization allows us to cut down
   * on the number of objects instantiated and garbage collected.
   *
   * Not thread-safe.
   */
  public class VertexIdMessageIterator extends VertexIdIterator {
    /** Current message */
    private M message;

    @Override
    public void next() {
      if (vertexId == null) {
        vertexId = configuration.createVertexId();
      }
      if (message == null) {
        message = configuration.createMessageValue();
      }
      try {
        vertexId.readFields(extendedDataInput);
        message.readFields(extendedDataInput);
      } catch (IOException e) {
        throw new IllegalStateException("next: IOException", e);
      }
    }

    /**
     * Get the current message
     *
     * @return Current message
     */
    public M getCurrentMessage() {
      return message;
    }
  }

  /**
   * Special iterator that reuses vertex ids and messages bytes so that the
   * lifetime of the object is only until next() is called.
   *
   * Vertex id ownership can be released if desired through
   * releaseCurrentVertexId().  This optimization allows us to cut down
   * on the number of objects instantiated and garbage collected.  Messages
   * can only be copied to an ExtendedDataOutput object
   *
   * Not thread-safe.
   */
  public class VertexIdMessageBytesIterator extends VertexIdIterator {
    /** Last message offset */
    private int messageOffset = -1;
    /** Number of bytes in the last message */
    private int messageBytes = -1;

    /**
     * Moves to the next element in the iteration.
     */
    @Override
    public void next() {
      if (vertexId == null) {
        vertexId = configuration.createVertexId();
      }

      try {
        vertexId.readFields(extendedDataInput);
        messageBytes = extendedDataInput.readInt();
        messageOffset = extendedDataInput.getPos();
        if (extendedDataInput.skipBytes(messageBytes) != messageBytes) {
          throw new IllegalStateException("next: Failed to skip " +
              messageBytes);
        }
      } catch (IOException e) {
        throw new IllegalStateException("next: IOException", e);
      }
    }

    /**
     * Write the current message to an ExtendedDataOutput object
     *
     * @param dataOutput Where the current message will be written to
     */
    public void writeCurrentMessageBytes(
        ExtendedDataOutput dataOutput) {
      try {
        dataOutput.write(
            extendedDataOutput.getByteArray(), messageOffset, messageBytes);
      } catch (IOException e) {
        throw new IllegalStateException("writeCurrentMessageBytes: Got " +
            "IOException", e);
      }
    }
  }
}
