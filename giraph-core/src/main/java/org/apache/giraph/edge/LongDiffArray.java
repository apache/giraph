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
package org.apache.giraph.edge;

import it.unimi.dsi.fastutil.bytes.ByteArrays;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.giraph.utils.ExtendedByteArrayDataInput;
import org.apache.giraph.utils.ExtendedByteArrayDataOutput;
import org.apache.giraph.utils.ExtendedDataInput;
import org.apache.giraph.utils.ExtendedDataOutput;
import org.apache.giraph.utils.UnsafeByteArrayInputStream;
import org.apache.giraph.utils.UnsafeByteArrayOutputStream;
import org.apache.giraph.utils.Varint;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Preconditions;

/**
 * Compressed list array of long ids.
 * Note: this implementation is optimized for space usage,
 * but random access and edge removals are expensive.
 * Users of this class should explicitly call {@link #trim()} function
 * to compact in-memory representation after all updates are done.
 * Compacting object is expensive so should only be done once after bulk update.
 * Compaction can also be caused by serialization attempt or
 * by calling {@link #iterator()}
 */
@NotThreadSafe
public class LongDiffArray implements Writable {

  /**
   * Array of target vertex ids.
   */
  private byte[] compressedData;
  /**
   * Number of edges stored in compressed array.
   * There may be some extra edges in transientData or there may be some edges
   * removed. These will not count here. To get real number of elements stored
   * in this object @see {@link #size()}
   */
  private int size;

  /**
   * Last updates are stored here. We clear them out after object is compacted.
   */
  private TransientChanges transientData;

  /**
   * Use unsafe serialization?
   */
  private boolean useUnsafeSerialization = true;

  /**
   * Set whether to use unsafe serailization
   * @param useUnsafeSerialization use unsafe serialization
   */
  public void setUseUnsafeSerialization(boolean useUnsafeSerialization) {
    this.useUnsafeSerialization = useUnsafeSerialization;
  }

  /**
   * Initialize with a given capacity
   * @param capacity capacity
   */
  public void initialize(int capacity) {
    reset();
    if (capacity > 0) {
      transientData = new TransientChanges(capacity);
    }
  }

  /**
   * Initialize array
   */
  public void initialize() {
    reset();
  }

  /**
   * Add a value
   * @param id id to add
   */
  public void add(long id) {
    checkTransientData();
    transientData.add(id);
  }


  /**
   * Remove a given value
   * @param id id to remove
   */
  public void remove(long id) {
    checkTransientData();

    if (size > 0) {
      LongsDiffReader reader = new LongsDiffReader(
        compressedData,
        useUnsafeSerialization
      );
      for (int i = 0; i < size; i++) {
        long cur = reader.readNext();
        if (cur == id) {
          transientData.markRemoved(i);
        } else if (cur > id) {
          break;
        }
      }
    }
    transientData.removeAdded(id);
  }

  /**
   * The number of stored ids
   * @return the number of stored ids
   */
  public int size() {
    int result = size;
    if (transientData != null) {
      result += transientData.size();
    }
    return result;
  }

  /**
   * Returns an iterator that reuses objects.
   * @return Iterator
   */
  public Iterator<LongWritable> iterator() {
    trim();
    return new Iterator<LongWritable>() {
      /** Current position in the array. */
      private int position;
      private final LongsDiffReader reader =
        new LongsDiffReader(compressedData, useUnsafeSerialization);

      /** Representative edge object. */
      private final LongWritable reusableLong = new LongWritable();

      @Override
      public boolean hasNext() {
        return position < size;
      }

      @Override
      public LongWritable next() {
        position++;
        reusableLong.set(reader.readNext());
        return reusableLong;
      }

      @Override
      public void remove() {
        removeAt(position - 1);
      }
    };
  }

  @Override
  public void write(DataOutput out) throws IOException {
    trim();
    Varint.writeUnsignedVarInt(compressedData.length, out);
    Varint.writeUnsignedVarInt(size, out);
    out.write(compressedData);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    reset();
    compressedData = new byte[Varint.readUnsignedVarInt(in)];
    // We can actually calculate size after data array is read,
    // the trade-off is memory vs speed
    size = Varint.readUnsignedVarInt(in);
    in.readFully(compressedData);
  }

  /**
   * This function takes all recent updates and stores them efficiently.
   * It is safe to call this function multiple times.
   */
  public void trim() {
    if (transientData == null) {
      // We don't have any updates to this object. Return quickly.
      return;
    }

    // Beware this array is longer than the number of elements we interested in
    long[] transientValues = transientData.sortedValues();
    int pCompressed = 0;
    int pTransient = 0;

    LongsDiffReader reader = new LongsDiffReader(
      compressedData,
      useUnsafeSerialization
    );
    LongsDiffWriter writer = new LongsDiffWriter(useUnsafeSerialization);

    long curValue = size > 0 ? reader.readNext() : Long.MAX_VALUE;

    // Here we merge freshly added elements and old elements, we also want
    // to prune removed elements. Both arrays are sorted so in order to merge
    // them, we move to pointers and store result in the new array
    while (pTransient < transientData.numberOfAddedElements() ||
        pCompressed < size) {
      if (pTransient < transientData.numberOfAddedElements() &&
          curValue >= transientValues[pTransient]) {
        writer.writeNext(transientValues[pTransient]);
        pTransient++;
      } else {
        if (!transientData.isRemoved(pCompressed)) {
          writer.writeNext(curValue);
        }
        pCompressed++;
        if (pCompressed < size) {
          curValue = reader.readNext();
        } else {
          curValue = Long.MAX_VALUE;
        }
      }
    }

    compressedData = writer.toByteArray();
    size += transientData.size();
    transientData = null;
  }


  /**
   * Remove edge at position i.
   *
   * @param i Position of edge to be removed
   */
  private void removeAt(int i) {
    checkTransientData();
    if (i < size) {
      transientData.markRemoved(i);
    } else {
      transientData.removeAddedAt(i - size);
    }
  }

  /**
   * Check if transient data needs to be created.
   */
  private void checkTransientData() {
    if (transientData == null) {
      transientData = new TransientChanges();
    }
  }

  /**
   * Reset object to completely empty state.
   */
  private void reset() {
    compressedData = ByteArrays.EMPTY_ARRAY;
    size = 0;
    transientData = null;
  }

  /**
   * Reading array of longs diff encoded from byte array.
   */
  private static class LongsDiffReader {
    /** Input stream */
    private final ExtendedDataInput input;
    /** last read value */
    private long current;
    /** True if we haven't read any numbers yet */
    private boolean first = true;

    /**
     * Construct LongsDiffReader
     *
     * @param compressedData Input byte array
     * @param useUnsafeReader use unsafe reader
     */
    public LongsDiffReader(byte[] compressedData, boolean useUnsafeReader) {
      if (useUnsafeReader) {
        input = new UnsafeByteArrayInputStream(compressedData);
      } else {
        input = new ExtendedByteArrayDataInput(compressedData);
      }
    }

    /**
     * Read next value from reader
     * @return next value
     */
    long readNext() {
      try {
        if (first) {
          current = input.readLong();
          first = false;
        } else {
          current += Varint.readUnsignedVarLong(input);
        }
        return current;
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  /**
   * Writing array of longs diff encoded into the byte array.
   */
  private static class LongsDiffWriter {
    /** Wrapping resultStream into DataOutputStream */
    private final ExtendedDataOutput out;
    /** last value written */
    private long lastWritten;
    /** True if we haven't written any numbers yet */
    private boolean first = true;

    /**
     * Construct LongsDiffWriter
     * @param useUnsafeWriter use unsafe writer
     */
    public LongsDiffWriter(boolean useUnsafeWriter) {
      if (useUnsafeWriter) {
        out = new UnsafeByteArrayOutputStream();
      } else {
        out = new ExtendedByteArrayDataOutput();
      }
    }

    /**
     * Write next value to writer
     * @param value Value to be written
     */
    void writeNext(long value) {
      try {
        if (first) {
          out.writeLong(value);
          first = false;
        } else {
          Preconditions.checkState(value >= lastWritten,
              "Values need to be in order");
          Preconditions.checkState((value - lastWritten) >= 0,
              "In order to use this class, difference of consecutive IDs " +
              "cannot overflow longs");
          Varint.writeUnsignedVarLong(value - lastWritten, out);
        }
        lastWritten = value;
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }

    /**
     * Get resulting byte array
     * @return resulting byte array
     */
    byte[] toByteArray() {
      return out.toByteArray();
    }
  }

  /**
   * Temporary storage for all updates.
   * We don't want to update compressed array frequently so we only update it
   * on request at the same time we allow temporary updates to persist in this
   * class.
   */
  private static class TransientChanges {
    /** Neighbors that were added since last flush */
    private final LongArrayList neighborsAdded;
    /** Removed indices in original array */
    private final BitSet removed = new BitSet();
    /** Number of values removed */
    private int removedCount;

    /**
     * Construct transient changes with given capacity
     * @param capacity capacity
     */
    private TransientChanges(int capacity) {
      neighborsAdded = new LongArrayList(capacity);
    }

    /**
     * Construct transient changes
     */
    private TransientChanges() {
      neighborsAdded = new LongArrayList();
    }

    /**
     * Add new value
     * @param value value to add
     */
    private void add(long value) {
      neighborsAdded.add(value);
    }

    /**
     * Mark given index to remove
     * @param index Index to remove
     */
    private void markRemoved(int index) {
      if (!removed.get(index)) {
        removedCount++;
        removed.set(index);
      }
    }

    /**
     * Remove value from neighborsAdded
     * @param index Position to remove from
     */
    private void removeAddedAt(int index) {
      // The order of the edges is irrelevant, so we can simply replace
      // the deleted edge with the rightmost element, thus achieving constant
      // time.
      if (index == neighborsAdded.size() - 1) {
        neighborsAdded.popLong();
      } else {
        neighborsAdded.set(index, neighborsAdded.popLong());
      }
    }

    /**
     * Number of added elements
     * @return number of added elements
     */
    private int numberOfAddedElements() {
      return neighborsAdded.size();
    }

    /**
     * Remove added value
     * @param target value to remove
     */
    private void removeAdded(long target) {
      neighborsAdded.rem(target);
    }

    /**
     * Additional size in transient changes
     * @return additional size
     */
    private int size() {
      return neighborsAdded.size() - removedCount;
    }

    /**
     * Sorted added values
     * @return sorted added values
     */
    private long[] sortedValues() {
      long[] ret = neighborsAdded.elements();
      Arrays.sort(ret, 0, neighborsAdded.size());
      return ret;
    }

    /**
     * Check if index was removed
     * @param i Index to check
     * @return Whether it was removed
     */
    private boolean isRemoved(int i) {
      return removed.get(i);
    }
  }
}
