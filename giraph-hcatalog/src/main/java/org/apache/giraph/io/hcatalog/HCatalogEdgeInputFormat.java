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

import java.io.IOException;
import java.util.List;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.EdgeInputFormat;
import org.apache.giraph.io.EdgeReader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hcatalog.data.HCatRecord;

/**
 * HCatalog {@link EdgeInputFormat} for reading edges from Hive/Pig.
 *
 * @param <I> Vertex id
 * @param <E> Edge value
 */
public abstract class HCatalogEdgeInputFormat<
    I extends WritableComparable,
    E extends Writable>
    extends EdgeInputFormat<I, E> {
  /**
   * HCatalog input format.
   */
  private GiraphHCatInputFormat hCatInputFormat = new GiraphHCatInputFormat();

  @Override
  public final List<InputSplit> getSplits(JobContext context,
                                          int minSplitCountHint)
    throws IOException, InterruptedException {
    return hCatInputFormat.getEdgeSplits(context);
  }

  /**
   * Get underlying HCatalog input format. Used for creating readers.
   *
   * @return GiraphHCatInputFormat stored.
   */
  protected GiraphHCatInputFormat getHCatInputFormat() {
    return hCatInputFormat;
  }

  /**
   * {@link EdgeReader} for {@link HCatalogEdgeInputFormat}.
   */
  protected abstract static class HCatalogEdgeReader<
      I extends WritableComparable, E extends Writable>
      extends EdgeReader<I, E> {
    /** HCatalog input format to use */
    private final GiraphHCatInputFormat hCatInputFormat;
    /** Internal {@link RecordReader}. */
    private RecordReader<WritableComparable, HCatRecord> hCatRecordReader;
    /** Context passed to initialize. */
    private TaskAttemptContext context;

    /**
     * Constructor taking hcat input format to use.
     *
     * @param hCatInputFormat HCatalog input format
     */
    public HCatalogEdgeReader(GiraphHCatInputFormat hCatInputFormat) {
      this.hCatInputFormat = hCatInputFormat;
    }

    @Override
    public final void initialize(InputSplit inputSplit,
                                 TaskAttemptContext context)
      throws IOException, InterruptedException {
      hCatRecordReader =
          hCatInputFormat.createEdgeRecordReader(inputSplit, context);
      hCatRecordReader.initialize(inputSplit, context);
      this.context = context;
    }

    @Override
    public boolean nextEdge() throws IOException, InterruptedException {
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
   * Create {@link EdgeReader}.
   *
   * @return {@link HCatalogEdgeReader} instance.
   */
  protected abstract HCatalogEdgeReader<I, E> createEdgeReader();

  @Override
  public EdgeReader<I, E>
  createEdgeReader(InputSplit split, TaskAttemptContext context)
    throws IOException {
    try {
      HCatalogEdgeReader reader = createEdgeReader();
      reader.initialize(split, context);
      return reader;
    } catch (InterruptedException e) {
      throw new IllegalStateException(
          "createEdgeReader: Interrupted creating reader.", e);
    }
  }

  /**
   * {@link HCatalogEdgeReader} for tables holding a complete edge
   * in each row.
   */
  protected abstract static class SingleRowHCatalogEdgeReader<
      I extends WritableComparable, E extends Writable>
      extends HCatalogEdgeReader<I, E> {
    /**
     * Constructor
     * @param hCatInputFormat giraph input format to use
     */
    public SingleRowHCatalogEdgeReader(GiraphHCatInputFormat hCatInputFormat) {
      super(hCatInputFormat);
    }

    /**
     * Get source vertex id from a record.
     *
     * @param record Input record
     * @return I Source vertex id
     */
    protected abstract I getSourceVertexId(HCatRecord record);

    /**
     * Get target vertex id from a record.
     *
     * @param record Input record
     * @return I Target vertex id
     */
    protected abstract I getTargetVertexId(HCatRecord record);

    /**
     * Get edge value from a record.
     *
     * @param record Input record
     * @return E Edge value
     */
    protected abstract E getEdgeValue(HCatRecord record);

    @Override
    public I getCurrentSourceId() throws IOException, InterruptedException {
      HCatRecord record = getRecordReader().getCurrentValue();
      return getSourceVertexId(record);
    }

    @Override
    public Edge<I, E> getCurrentEdge() throws IOException,
        InterruptedException {
      HCatRecord record = getRecordReader().getCurrentValue();
      return EdgeFactory.create(getTargetVertexId(record),
          getEdgeValue(record));
    }
  }

  /**
   * {@link HCatalogEdgeReader} for tables holding a complete edge
   * in each row where the edges contain no data other than IDs they point to.
   */
  protected abstract static class SingleRowHCatalogEdgeNoValueReader<
      I extends WritableComparable>
      extends HCatalogEdgeReader<I, NullWritable> {
    /**
     * Constructor
     * @param hCatInputFormat giraph input format to use
     */
    public SingleRowHCatalogEdgeNoValueReader(
        GiraphHCatInputFormat hCatInputFormat) {
      super(hCatInputFormat);
    }

    /**
     * Get source vertex id from a record.
     *
     * @param record Input record
     * @return I Source vertex id
     */
    protected abstract I getSourceVertexId(HCatRecord record);

    /**
     * Get target vertex id from a record.
     *
     * @param record Input record
     * @return I Target vertex id
     */
    protected abstract I getTargetVertexId(HCatRecord record);

    @Override
    public I getCurrentSourceId() throws IOException, InterruptedException {
      HCatRecord record = getRecordReader().getCurrentValue();
      return getSourceVertexId(record);
    }

    @Override
    public Edge<I, NullWritable> getCurrentEdge() throws IOException,
        InterruptedException {
      HCatRecord record = getRecordReader().getCurrentValue();
      return EdgeFactory.create(getTargetVertexId(record));
    }
  }
}
