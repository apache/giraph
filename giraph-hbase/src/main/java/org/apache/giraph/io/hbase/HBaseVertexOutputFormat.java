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

package org.apache.giraph.io.hbase;

import java.io.IOException;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.io.VertexWriter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 *
 * Base class for writing Vertex mutations back to specific
 * rows in an HBase table. This class wraps an instance of TableOutputFormat
 * for easy configuration with the existing properties.
 *
 * Setting conf.set(TableOutputFormat.OUTPUT_TABLE, "out_table");
 * will properly delegate to the TableOutputFormat instance contained
 * in this class. The Configurable interface prevents specific
 * wrapper methods from having to be called.
 *
 * Works with {@link HBaseVertexInputFormat}
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public abstract class HBaseVertexOutputFormat<
        I extends WritableComparable,
        V extends Writable,
        E extends Writable>
        extends VertexOutputFormat
                <I, V, E> {

  /**
   * delegate output format that writes to HBase
   */
  protected static final TableOutputFormat<ImmutableBytesWritable>
  BASE_FORMAT = new TableOutputFormat<ImmutableBytesWritable>();

  /**
   *   Constructor
   *
   *   Simple class which takes an instance of RecordWriter
   *   over Writable objects. Subclasses are
   *   expected to implement writeVertex()
   *
   * @param <I> Vertex index value
   * @param <V> Vertex value
   * @param <E> Edge value
   */
  public abstract static class HBaseVertexWriter<
      I extends WritableComparable,
      V extends Writable,
      E extends Writable>
      extends VertexWriter<I, V, E> {

    /** Context */
    private TaskAttemptContext context;
    /** Record writer instance */
    private RecordWriter<ImmutableBytesWritable, Writable> recordWriter;

   /**
    * Sets up base table output format and creates a record writer.
    * @param context task attempt context
    */
    public HBaseVertexWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
      BASE_FORMAT.setConf(context.getConfiguration());
      this.recordWriter = BASE_FORMAT.getRecordWriter(context);
    }

    /**
     * initialize
     *
     * @param context Context used to write the vertices.
     * @throws IOException
     */
    public void initialize(TaskAttemptContext context)
      throws IOException {
      this.context = context;
    }

    /**
     * close
     *
     * @param context the context of the task
     * @throws IOException
     * @throws InterruptedException
     */
    public void close(TaskAttemptContext context)
      throws IOException, InterruptedException {
      recordWriter.close(context);
    }

    /**
     * Get the table record writer;
     *
     * @return Record writer to be used for writing.
     */
    public RecordWriter<ImmutableBytesWritable,
            Writable> getRecordWriter() {
      return recordWriter;
    }

    /**
     * getContext
     *
     * @return Context passed to initialize.
     */
    public TaskAttemptContext getContext() {
      return context;
    }
  }

  /**
   * checkOutputSpecs
   *
   * @param context information about the job
   * @throws IOException
   * @throws InterruptedException
   */
  public void checkOutputSpecs(JobContext context)
    throws IOException, InterruptedException {
    BASE_FORMAT.checkOutputSpecs(context);
  }

  /**
   * getOutputCommitter
   *
   * @param context the task context
   * @return  OutputCommitter ouputCommitter
   * @throws IOException
   * @throws InterruptedException
   */
  public OutputCommitter getOutputCommitter(
    TaskAttemptContext context)
    throws IOException, InterruptedException {
    BASE_FORMAT.setConf(getConf());
    return BASE_FORMAT.getOutputCommitter(context);
  }
}
