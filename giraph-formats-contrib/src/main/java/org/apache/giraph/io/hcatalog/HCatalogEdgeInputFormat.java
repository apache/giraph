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

import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.EdgeInputFormat;
import org.apache.giraph.graph.EdgeReader;
import org.apache.giraph.graph.EdgeWithSource;
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
  public final List<InputSplit> getSplits(JobContext context, int numWorkers)
    throws IOException, InterruptedException {
    return hCatInputFormat.getEdgeSplits(context);
  }

  /**
   * {@link EdgeReader} for {@link HCatalogEdgeInputFormat}.
   */
  protected abstract class HCatalogEdgeReader implements EdgeReader<I, E> {
    /** Internal {@link RecordReader}. */
    private RecordReader<WritableComparable, HCatRecord> hCatRecordReader;
    /** Context passed to initialize. */
    private TaskAttemptContext context;

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

   * @return {@link HCatalogEdgeReader} instance.
   */
  protected abstract HCatalogEdgeReader createEdgeReader();

  @Override
  public final EdgeReader<I, E>
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
  protected abstract class SingleRowHCatalogEdgeReader
      extends HCatalogEdgeReader {
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
    public EdgeWithSource<I, E> getCurrentEdge() throws IOException,
        InterruptedException {
      HCatRecord record = getRecordReader().getCurrentValue();
      return new EdgeWithSource<I, E>(
          getSourceVertexId(record),
          new Edge<I, E>(getTargetVertexId(record), getEdgeValue(record)));
    }
  }
}
