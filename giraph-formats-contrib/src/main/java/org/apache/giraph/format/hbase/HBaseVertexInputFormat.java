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
package org.apache.giraph.format.hbase;

import org.apache.giraph.graph.VertexInputFormat;
import org.apache.giraph.graph.VertexReader;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;

/**
 *
 * Base class that wraps an HBase TableInputFormat and underlying Scan object
 * to help instantiate vertices from an HBase table. All
 * the static TableInputFormat properties necessary to configure
 * an HBase job are available.
 *
 * For example, setting conf.set(TableInputFormat.INPUT_TABLE, "in_table");
 * from the job setup routine will properly delegate to the
 * TableInputFormat instance. The Configurable interface prevents specific
 * wrapper methods from having to be called.
 *
 * Works with {@link HBaseVertexOutputFormat}
 *
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 * @param <M> Message data
 */
@SuppressWarnings("rawtypes")
public abstract  class HBaseVertexInputFormat<
          I extends WritableComparable,
        V extends Writable,
        E extends Writable,
        M extends Writable>
        extends VertexInputFormat<I, V, E, M> implements Configurable {

  /**
   * delegate HBase table input format
   */
  protected TableInputFormat tableInputFormat =
         new TableInputFormat();
  /**
   * Injected conf by Configurable interface
   */
  private Configuration conf;

  public Configuration getConf() {
    return conf;
  }

  /**
   * setConf()
   *
   * We must initialize the table format since we manually instantiate it.
   *
   * @param conf Configuration object
   */
  public void setConf(Configuration conf) {
    tableInputFormat.setConf(conf);
    this.conf = conf;
  }

  /**
   * Takes an instance of RecordReader that supports
   * HBase row-key, result records.  Subclasses can focus on
   * vertex instantiation details without worrying about connection
   * semantics. Subclasses are expected to implement nextVertex() and
   * getCurrentVertex()
   *
   *
   *
   * @param <I> Vertex index value
   * @param <V> Vertex value
   * @param <E> Edge value
   * @param <M> Message data
   */
  public abstract static class HBaseVertexReader<
          I extends WritableComparable,
          V extends Writable,
          E extends Writable, M extends Writable>
          implements VertexReader<I, V, E, M> {

    /**
     * reader instance
     */
    private final RecordReader<ImmutableBytesWritable, Result> reader;
    /** Context passed to initialize */
    private TaskAttemptContext context;

    /**
     * constructor used for subclassing vertex record reader.
     * @param reader HBase record reader instance
     */
    public HBaseVertexReader(RecordReader<ImmutableBytesWritable,
                                              Result> reader) {
      this.reader = reader;
    }

    /**
     * initialize
     *
     * @param inputSplit Input split to be used for reading vertices.
     * @param context Context from the task.
     * @throws IOException
     * @throws InterruptedException
     */
    public void initialize(InputSplit inputSplit,
                           TaskAttemptContext context)
      throws IOException,
      InterruptedException {
      reader.initialize(inputSplit, context);
      this.context = context;
    }

    /**
     * close
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
    public float getProgress() throws
      IOException, InterruptedException {
      return reader.getProgress();
    }

    /**
     * getRecordReader
     *
     * @return Record reader to be used for reading.
     */
    protected RecordReader<ImmutableBytesWritable,
      Result> getRecordReader() {
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

  /**
   * getSplits
   *
   * @param context Context of the job
   * @param numWorkers Number of workers used for this job
   * @return HBase region splits
   * @throws IOException
   * @throws InterruptedException
   */
  public List<InputSplit> getSplits(
  JobContext context, int numWorkers)
    throws IOException, InterruptedException {
    return tableInputFormat.getSplits(context);
  }
}
