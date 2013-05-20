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
import java.util.List;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 *  Class which wraps the AccumuloInputFormat. It's designed
 *  as an extension point to VertexInputFormat subclasses who wish
 *  to read from AccumuloTables.
 *
 *  Works with
 *  {@link AccumuloVertexOutputFormat}
 *
 * @param <I> vertex id type
 * @param <V>  vertex value type
 * @param <E>  edge type
 */
public abstract class AccumuloVertexInputFormat<
        I extends WritableComparable,
        V extends Writable,
        E extends Writable>
        extends VertexInputFormat<I, V, E> {
  /**
   * delegate input format for all accumulo operations.
   */
  protected AccumuloInputFormat accumuloInputFormat =
      new AccumuloInputFormat();

  /**
  * Abstract class which provides a template for instantiating vertices
  * from Accumulo Key/Value pairs.
  *
  * @param <I>  vertex id type
  * @param <V>  vertex value type
  * @param <E>  edge type
  */
  public abstract static class AccumuloVertexReader<
      I extends WritableComparable,
      V extends Writable, E extends Writable>
      extends VertexReader<I, V, E> {

    /** Giraph configuration */
    private ImmutableClassesGiraphConfiguration<I, V, E>
    configuration;
    /**
     * Used by subclasses to read key/value pairs.
     */
    private final RecordReader<Key, Value> reader;
    /** Context passed to initialize */
    private TaskAttemptContext context;

    /**
     * Constructor used to pass Record Reader instance
     * @param reader  Accumulo record reader
     */
    public AccumuloVertexReader(RecordReader<Key, Value> reader) {
      this.reader = reader;
    }

    public ImmutableClassesGiraphConfiguration<I, V, E>
    getConfiguration() {
      return configuration;
    }

    /**
     * initialize the reader.
     *
     * @param inputSplit Input split to be used for reading vertices.
     * @param context Context from the task.
     * @throws IOException
     * @throws InterruptedException
     */
    public void initialize(InputSplit inputSplit,
                           TaskAttemptContext context)
      throws IOException, InterruptedException {
      reader.initialize(inputSplit, context);
      this.context = context;
      this.configuration =
          new ImmutableClassesGiraphConfiguration<I, V, E>(
              context.getConfiguration());
    }

    /**
     * close
     *
     * @throws IOException
     */
    public void close() throws IOException {
      reader.close();
    }

    /**
     * getProgress
     *
     * @return progress
     * @throws IOException
     * @throws InterruptedException
     */
    public float getProgress() throws IOException, InterruptedException {
      return reader.getProgress();
    }

    /**
    * Get the result record reader
    *
    * @return Record reader to be used for reading.
    */
    protected RecordReader<Key, Value> getRecordReader() {
      return reader;
    }

    /**
     * getContext
     *
     * @return Context passed to initialize.
     */
    protected TaskAttemptContext getContext() {
      return context;
    }

  }

  @Override
  public List<InputSplit> getSplits(
    JobContext context, int minSplitCountHint)
    throws IOException, InterruptedException {
    List<InputSplit> splits = null;
    try {
      splits = accumuloInputFormat.getSplits(context);
    } catch (IOException e) {
      if (e.getMessage().contains("Input info has not been set")) {
        throw new IOException(e.getMessage() +
                " Make sure you initialized" +
                " AccumuloInputFormat static setters " +
                "before passing the config to GiraphJob.");
      }
    }
    return splits;
  }
}
