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

package org.apache.giraph.io.hcatalog;

import org.apache.giraph.io.VertexValueInputFormat;
import org.apache.giraph.io.VertexValueReader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hcatalog.data.HCatRecord;

import java.io.IOException;
import java.util.List;

/**
 * HCatalog {@link VertexValueInputFormat} for reading vertex values from
 * Hive/Pig.
 *
 * @param <I> Vertex id
 * @param <V> Vertex value
 */
public abstract class HCatalogVertexValueInputFormat<I extends
    WritableComparable, V extends Writable>
    extends VertexValueInputFormat<I, V> {
  /**
   * HCatalog input format.
   */
  private GiraphHCatInputFormat hCatInputFormat = new GiraphHCatInputFormat();

  @Override
  public List<InputSplit> getSplits(JobContext context, int minSplitCountHint)
    throws IOException, InterruptedException {
    return hCatInputFormat.getVertexSplits(context);
  }

  /**
   * {@link VertexValueReader} for {@link HCatalogVertexValueInputFormat}.
   */
  protected abstract class HCatalogVertexValueReader
      extends VertexValueReader<I, V> {
    /** Internal {@link RecordReader}. */
    private RecordReader<WritableComparable, HCatRecord> hCatRecordReader;
    /** Context passed to initialize. */
    private TaskAttemptContext context;

    @Override
    public final void initialize(InputSplit inputSplit,
                                 TaskAttemptContext context)
      throws IOException, InterruptedException {
      super.initialize(inputSplit, context);
      hCatRecordReader =
          hCatInputFormat.createVertexRecordReader(inputSplit, context);
      hCatRecordReader.initialize(inputSplit, context);
      this.context = context;
    }

    @Override
    public boolean nextVertex() throws IOException, InterruptedException {
      return hCatRecordReader.nextKeyValue();
    }

    @Override
    public final void close() throws IOException {
      hCatRecordReader.close();
    }

    @Override
    public final float getProgress() throws IOException, InterruptedException {
      return hCatRecordReader.getProgress();
    }

    /**
     * Get the record reader.
     *
     * @return Record reader to be used for reading.
     */
    protected final RecordReader<WritableComparable, HCatRecord>
    getRecordReader() {
      return hCatRecordReader;
    }

    /**
     * Get the context.
     *
     * @return Context passed to initialize.
     */
    protected final TaskAttemptContext getContext() {
      return context;
    }
  }

  /**
   * Create {@link VertexValueReader}.

   * @return {@link HCatalogVertexValueReader} instance.
   */
  protected abstract HCatalogVertexValueReader createVertexValueReader();

  @Override
  public final VertexValueReader<I, V>
  createVertexValueReader(InputSplit split, TaskAttemptContext context)
    throws IOException {
    try {
      HCatalogVertexValueReader reader = createVertexValueReader();
      reader.initialize(split, context);
      return reader;
    } catch (InterruptedException e) {
      throw new IllegalStateException(
          "createVertexValueReader: Interrupted creating reader.", e);
    }
  }

  /**
   * {@link HCatalogVertexValueReader} for tables holding a complete vertex
   * value in each row.
   */
  protected abstract class SingleRowHCatalogVertexValueReader
      extends HCatalogVertexValueReader {
    /**
     * Get vertex id from a record.
     *
     * @param record Input record
     * @return I Vertex id
     */
    protected abstract I getVertexId(HCatRecord record);

    /**
     * Get vertex value from a record.
     *
     * @param record Input record
     * @return V Vertex value
     */
    protected abstract V getVertexValue(HCatRecord record);

    @Override
    public final I getCurrentVertexId() throws IOException,
        InterruptedException {
      return getVertexId(getRecordReader().getCurrentValue());
    }

    @Override
    public final V getCurrentVertexValue() throws IOException,
        InterruptedException {
      return getVertexValue(getRecordReader().getCurrentValue());
    }
  }
}
