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

package org.apache.giraph.io.formats;

import static org.apache.giraph.conf.GiraphConstants.EDGE_OUTPUT_FORMAT_SUBDIR;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.io.EdgeOutputFormat;
import org.apache.giraph.io.EdgeWriter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Abstract class that users should subclass to use their own text based
 * edge output format.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public abstract class TextEdgeOutputFormat<I extends WritableComparable,
    V extends Writable, E extends Writable>
    extends EdgeOutputFormat<I, V, E> {
  /** Uses the TextOutputFormat to do everything */
  protected GiraphTextOutputFormat textOutputFormat =
    new GiraphTextOutputFormat() {
      @Override
      protected String getSubdir() {
        return EDGE_OUTPUT_FORMAT_SUBDIR.get(getConf());
      }
    };

  @Override
  public void checkOutputSpecs(JobContext context)
    throws IOException, InterruptedException {
    textOutputFormat.checkOutputSpecs(context);
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
    throws IOException, InterruptedException {
    return textOutputFormat.getOutputCommitter(context);
  }

  /**
   * The factory method which produces the {@link TextEdgeWriter} used by this
   * output format.
   *
   * @param context  the information about the task
   * @return         the text edge writer to be used
   */
  @Override
  public abstract TextEdgeWriter createEdgeWriter(TaskAttemptContext
      context) throws IOException, InterruptedException;

  /**
   * Abstract class to be implemented by the user based on their specific
   * edge output.  Easiest to ignore the key value separator and only use
   * key instead.
   */
  protected abstract class TextEdgeWriter<I extends WritableComparable,
    V extends Writable, E extends Writable>
      extends EdgeWriter<I, V, E> {
    /** Internal line record writer */
    private RecordWriter<Text, Text> lineRecordWriter;
    /** Context passed to initialize */
    private TaskAttemptContext context;

    @Override
    public void initialize(TaskAttemptContext context) throws IOException,
           InterruptedException {
      lineRecordWriter = createLineRecordWriter(context);
      this.context = context;
    }

    /**
     * Create the line record writer. Override this to use a different
     * underlying record writer (useful for testing).
     *
     * @param  context the context passed to initialize
     * @return the record writer to be used
     * @throws IOException          exception that can be thrown during creation
     * @throws InterruptedException exception that can be thrown during creation
     */
    protected RecordWriter<Text, Text> createLineRecordWriter(
        TaskAttemptContext context) throws IOException, InterruptedException {
      return textOutputFormat.getRecordWriter(context);
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException,
        InterruptedException {
      lineRecordWriter.close(context);
    }

    /**
     * Get the line record writer.
     *
     * @return Record writer to be used for writing.
     */
    public RecordWriter<Text, Text> getRecordWriter() {
      return lineRecordWriter;
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
   * Abstract class to be implemented by the user to write a line for each
   * edge.
   */
  protected abstract class TextEdgeWriterToEachLine<
    I extends WritableComparable, V extends Writable, E extends Writable>
    extends TextEdgeWriter<I, V, E> {

    @Override
    public final void writeEdge(I sourceId, V sourceValue, Edge<I, E> edge)
      throws IOException, InterruptedException {

      // Note we are writing line as key with null value
      getRecordWriter().write(
        convertEdgeToLine(sourceId, sourceValue, edge), null);
    }

    /**
     * Writes a line for the given edge.
     *
     * @param sourceId    the current id of the source vertex
     * @param sourceValue the current value of the source vertex
     * @param edge        the current vertex for writing
     * @return the text line to be written
     * @throws IOException exception that can be thrown while writing
     */
    protected abstract Text convertEdgeToLine(I sourceId,
      V sourceValue, Edge<I, E> edge) throws IOException;
  }
}
