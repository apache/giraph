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
package org.apache.giraph.io.accumulo;

import java.io.IOException;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Mutation;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.io.VertexWriter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
/**
 *
 *  Class which wraps the AccumuloOutputFormat. It's designed
 *  as an extension point to VertexOutputFormat subclasses who wish
 *  to write vertices back to an Accumulo table.
 *
 *  Works with
 *  {@link AccumuloVertexInputFormat}
 *
 *
 * @param <I> vertex id type
 * @param <V>  vertex value type
 * @param <E>  edge type
 */
public abstract class AccumuloVertexOutputFormat<
        I extends WritableComparable,
        V extends Writable,
        E extends Writable>
        extends VertexOutputFormat<I, V, E> {


  /**
   * Output table parameter
   */
  public static final String OUTPUT_TABLE = "OUTPUT_TABLE";

  /**
   * Accumulo delegate for table output
   */
  protected AccumuloOutputFormat accumuloOutputFormat =
          new AccumuloOutputFormat();

  /**
   *
   * Main abstraction point for vertex writers to persist back
   * to Accumulo tables.
   *
   * @param <I> vertex id type
   * @param <V> vertex value type
   * @param <E>  edge type
   */
  public abstract static class AccumuloVertexWriter<
      I extends WritableComparable,
      V extends Writable,
      E extends Writable>
      extends VertexWriter<I, V, E> {

    /**
     * task attempt context.
     */
    private TaskAttemptContext context;

    /**
     * Accumulo record writer
     */
    private RecordWriter<Text, Mutation> recordWriter;

    /**
     * Constructor for use with subclasses
     *
     * @param recordWriter accumulo record writer
     */
    public AccumuloVertexWriter(RecordWriter<Text, Mutation> recordWriter) {
      this.recordWriter = recordWriter;
    }

    /**
     * initialize
     *
     * @param context Context used to write the vertices.
     * @throws IOException
     */
    public void initialize(TaskAttemptContext context) throws IOException {
      this.context = context;
    }

    /**
     *  close
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
    public RecordWriter<Text, Mutation> getRecordWriter() {
      return recordWriter;
    }

    /**
     * Get the context.
     *
     * @return Context passed to initialize.
     */
    public TaskAttemptContext getContext() {
      return context;
    }

  }
  /**
   *
   * checkOutputSpecs
   *
   * @param context information about the job
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public void checkOutputSpecs(JobContext context)
    throws IOException, InterruptedException {
    try {
      accumuloOutputFormat.checkOutputSpecs(context);
    } catch (IOException e) {
      if (e.getMessage().contains("Output info has not been set")) {
        throw new IOException(e.getMessage() + " Make sure you initialized" +
                " AccumuloOutputFormat static setters " +
                "before passing the config to GiraphJob.");
      }
    }
  }

  /**
   * getOutputCommitter
   *
   * @param context the task context
   * @return OutputCommitter
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
    throws IOException, InterruptedException {
    return accumuloOutputFormat.getOutputCommitter(context);
  }
}
