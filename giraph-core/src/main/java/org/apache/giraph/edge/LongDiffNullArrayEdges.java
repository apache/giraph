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

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.EdgeIterables;
import org.apache.giraph.utils.ExtendedDataInput;
import org.apache.giraph.utils.ExtendedDataOutput;
import org.apache.giraph.utils.Trimmable;
import org.apache.giraph.utils.Varint;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Preconditions;

/**
 * Implementation of {@link org.apache.giraph.edge.OutEdges} with long ids
 * and null edge values, backed by a dynamic primitive array.
 * Parallel edges are allowed.
 * Note: this implementation is optimized for space usage,
 * but random access and edge removals are expensive.
 * Users of this class should explicitly call {@link #trim()} function
 * to compact in-memory representation after all updates are done.
 * Compacting object is expensive so should only be done once after bulk update.
 * Compaction can also be caused by serialization attempt or
 * by calling {@link #iterator()}
 */
@NotThreadSafe
public class LongDiffNullArrayEdges
    extends ConfigurableOutEdges<LongWritable, NullWritable>
    implements ReuseObjectsOutEdges<LongWritable, NullWritable>,
    MutableOutEdges<LongWritable, NullWritable>, Trimmable {

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

  @Override
  public void initialize(Iterable<Edge<LongWritable, NullWritable>> edges) {
    reset();
    EdgeIterables.initialize(this, edges);
    trim();
  }

  @Override
  public void initialize(int capacity) {
    reset();
    if (capacity > 0) {
      transientData = new TransientChanges(capacity);
    }
  }

  @Override
  public void initialize() {
    reset();
  }

  @Override
  public void add(Edge<LongWritable, NullWritable> edge) {
    checkTransientData();
    transientData.add(edge.getTargetVertexId().get());
  }


  @Override
  public void remove(LongWritable targetVertexId) {
    checkTransientData();
    long target = targetVertexId.get();

    if (size > 0) {
      LongsDiffReader reader = new LongsDiffReader(compressedData, getConf());
      for (int i = 0; i < size; i++) {
        long cur = reader.readNext();
        if (cur == target) {
          transientData.markRemoved(i);
        } else if (cur > target) {
          break;
        }
      }
    }
    transientData.removeAdded(target);
  }

  @Override
  public int size() {
    int result = size;
    if (transientData != null) {
      result += transientData.size();
    }
    return result;
  }

  @Override
  public Iterator<Edge<LongWritable, NullWritable>> iterator() {
    // Returns an iterator that reuses objects.
    // The downcast is fine because all concrete Edge implementations are
    // mutable, but we only expose the mutation functionality when appropriate.
    return (Iterator) mutableIterator();
  }

  @Override
  public Iterator<MutableEdge<LongWritable, NullWritable>> mutableIterator() {
    trim();
    return new Iterator<MutableEdge<LongWritable, NullWritable>>() {
      /** Current position in the array. */
      private int position;
      private final LongsDiffReader reader =
          new LongsDiffReader(compressedData, getConf());

      /** Representative edge object. */
      private final MutableEdge<LongWritable, NullWritable> representativeEdge =
          EdgeFactory.createReusable(new LongWritable());

      @Override
      public boolean hasNext() {
        return position < size;
      }

      @Override
      public MutableEdge<LongWritable, NullWritable> next() {
        position++;
        representativeEdge.getTargetVertexId().set(reader.readNext());
        return representativeEdge;
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
  @Override
  public void trim() {
    if (transientData == null) {
      // We don't have any updates to this object. Return quickly.
      return;
    }

    // Beware this array is longer than the number of elements we interested in
    long[] transientValues = transientData.sortedValues();
    int pCompressed = 0;
    int pTransient = 0;

    LongsDiffReader reader = new LongsDiffReader(compressedData, getConf());
    LongsDiffWriter writer = new LongsDiffWriter(getConf());

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
     * @param conf Conf
     */
    public LongsDiffReader(
      byte[] compressedData,
      ImmutableClassesGiraphConfiguration<LongWritable, Writable, NullWritable>
        conf
    ) {
      input = conf.createExtendedDataInput(compressedData);
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
     *
     * @param conf Conf
     */
    public LongsDiffWriter(
      ImmutableClassesGiraphConfiguration<LongWritable, Writable, NullWritable>
        conf
    ) {
      out = conf.createExtendedDataOutput();
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
