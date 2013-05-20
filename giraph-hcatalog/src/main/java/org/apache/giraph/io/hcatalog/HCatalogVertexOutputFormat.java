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

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.io.VertexWriter;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hcatalog.data.DefaultHCatRecord;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.mapreduce.HCatOutputFormat;

import java.io.IOException;

/**
 * Abstract class that users should subclass to store data to Hive or Pig table.
 * You can easily implement a {@link HCatalogVertexWriter} by extending
 * {@link SingleRowHCatalogVertexWriter} or {@link MultiRowHCatalogVertexWriter}
 * depending on how you want to fit your vertices into the output table.
 * <p>
 * The desired database and table name to store to can be specified via
 * {@link HCatOutputFormat#setOutput(org.apache.hadoop.mapreduce.Job,
 * org.apache.hcatalog.mapreduce.OutputJobInfo)}
 * as you setup your vertex output format with
 * {@link org.apache.giraph.conf.GiraphConfiguration}
 * setVertexOutputFormatClass(Class)}. You must create the output table.
 *
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public abstract class HCatalogVertexOutputFormat<
        I extends WritableComparable,
        V extends Writable,
        E extends Writable>
        extends VertexOutputFormat<I, V, E> {
  /**
  * hcat output format
  */
  protected HCatOutputFormat hCatOutputFormat = new HCatOutputFormat();

  @Override
  public final void checkOutputSpecs(JobContext context) throws IOException,
      InterruptedException {
    hCatOutputFormat.checkOutputSpecs(context);
  }

  @Override
  public final OutputCommitter getOutputCommitter(TaskAttemptContext context)
    throws IOException, InterruptedException {
    return hCatOutputFormat.getOutputCommitter(context);
  }

  /**
   * Abstract class that users should
   * subclass based on their specific vertex
   * output. Users should implement
   * writeVertex to create a HCatRecord that is
   * valid to for writing by HCatalogRecordWriter.
   *
   * @param <I> Vertex id
   * @param <V> Vertex value
   * @param <E> Edge value
  */
  protected abstract static class HCatalogVertexWriter<
      I extends WritableComparable,
      V extends Writable,
      E extends Writable>
      extends VertexWriter<I, V, E> {

    /** Internal HCatRecordWriter */
    private RecordWriter<WritableComparable<?>, HCatRecord> hCatRecordWriter;
    /** Context passed to initialize */
    private TaskAttemptContext context;

    /**
    * Initialize with the HCatRecordWriter
    * @param hCatRecordWriter
    *            Internal writer
    */
    protected void initialize(
                    RecordWriter<WritableComparable<?>,
                    HCatRecord> hCatRecordWriter) {
      this.hCatRecordWriter = hCatRecordWriter;
    }

    /**
    * Get the record reader.
    * @return Record reader to be used for reading.
    */
    protected RecordWriter<WritableComparable<?>,
            HCatRecord> getRecordWriter() {
      return hCatRecordWriter;
    }

    /**
    * Get the context.
    *
    * @return Context passed to initialize.
    */
    protected TaskAttemptContext getContext() {
      return context;
    }

    @Override
    public void initialize(TaskAttemptContext context) throws IOException {
      this.context = context;
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException,
        InterruptedException {
      hCatRecordWriter.close(context);
    }

  }

  /**
  * create vertex writer.
  * @return HCatalogVertexWriter
  */
  protected abstract HCatalogVertexWriter<I, V, E> createVertexWriter();

  @Override
  public final VertexWriter<I, V, E> createVertexWriter(
    TaskAttemptContext context) throws IOException,
    InterruptedException {
    HCatalogVertexWriter<I, V, E>  writer = createVertexWriter();
    writer.initialize(hCatOutputFormat.getRecordWriter(context));
    return writer;
  }

  /**
   * HCatalogVertexWriter to write each vertex in each row.
   *
   * @param <I> Vertex id
   * @param <V> Vertex value
   * @param <E> Edge value
   */
  protected abstract static class SingleRowHCatalogVertexWriter<
      I extends WritableComparable,
      V extends Writable,
      E extends Writable>
      extends HCatalogVertexWriter<I, V, E> {
    /**
    * get num columns
    * @return intcolumns
    */
    protected abstract int getNumColumns();

    /**
    * fill record
    * @param record to fill
    * @param vertex to populate record
    */
    protected abstract void fillRecord(HCatRecord record,
        Vertex<I, V, E> vertex);

    /**
    * create record
    * @param vertex to populate record
    * @return HCatRecord newly created
    */
    protected HCatRecord createRecord(Vertex<I, V, E> vertex) {
      HCatRecord record = new DefaultHCatRecord(getNumColumns());
      fillRecord(record, vertex);
      return record;
    }

    @Override
    public final void writeVertex(Vertex<I, V, E> vertex) throws IOException,
        InterruptedException {
      getRecordWriter().write(null, createRecord(vertex));
    }

  }

  /**
   * HCatalogVertexWriter to write each vertex in multiple rows.
   *
   * @param <I> Vertex id
   * @param <V> Vertex value
   * @param <E> Edge value
   */
  public abstract static class MultiRowHCatalogVertexWriter<
      I extends WritableComparable,
      V extends Writable,
      E extends Writable>
      extends HCatalogVertexWriter<I, V, E> {
    /**
    * create records
    * @param vertex to populate records
    * @return Iterable of records
    */
    protected abstract Iterable<HCatRecord> createRecords(
        Vertex<I, V, E> vertex);

    @Override
    public final void writeVertex(Vertex<I, V, E> vertex) throws IOException,
        InterruptedException {
      Iterable<HCatRecord> records = createRecords(vertex);
      for (HCatRecord record : records) {
        getRecordWriter().write(null, record);
      }
    }
  }
}
