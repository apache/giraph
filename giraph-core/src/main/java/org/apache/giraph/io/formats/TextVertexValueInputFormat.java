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

import org.apache.giraph.io.VertexValueInputFormat;
import org.apache.giraph.io.VertexValueReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;

/**
 * Abstract class that users should subclass to use their own text based
 * vertex value input format.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public abstract class TextVertexValueInputFormat<I extends WritableComparable,
    V extends Writable, E extends Writable>
    extends VertexValueInputFormat<I, V> {
  /** Uses the GiraphTextInputFormat to do everything */
  protected GiraphTextInputFormat textInputFormat = new GiraphTextInputFormat();

  @Override public void checkInputSpecs(Configuration conf) { }

  @Override
  public List<InputSplit> getSplits(JobContext context, int minSplitCountHint)
    throws IOException, InterruptedException {
    // Ignore the hint of numWorkers here since we are using
    // GiraphTextInputFormat to do this for us
    return textInputFormat.getVertexSplits(context);
  }

  @Override
  public abstract TextVertexValueReader createVertexValueReader(
      InputSplit split, TaskAttemptContext context) throws IOException;

  /**
   * {@link VertexValueReader} for {@link VertexValueInputFormat}.
   */
  protected abstract class TextVertexValueReader extends
      VertexValueReader<I, V> {
    /** Internal line record reader */
    private RecordReader<LongWritable, Text> lineRecordReader;
    /** Context passed to initialize */
    private TaskAttemptContext context;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context)
      throws IOException, InterruptedException {
      super.initialize(inputSplit, context);
      this.context = context;
      lineRecordReader = createLineRecordReader(inputSplit, context);
      lineRecordReader.initialize(inputSplit, context);
    }

    /**
     * Create the line record reader. Override this to use a different
     * underlying record reader (useful for testing).
     *
     * @param inputSplit the split to read
     * @param context the context passed to initialize
     * @return the record reader to be used
     * @throws IOException exception that can be thrown during creation
     * @throws InterruptedException exception that can be thrown during creation
     */
    protected RecordReader<LongWritable, Text>
    createLineRecordReader(InputSplit inputSplit, TaskAttemptContext context)
      throws IOException, InterruptedException {
      return textInputFormat.createRecordReader(inputSplit, context);
    }

    @Override
    public void close() throws IOException {
      lineRecordReader.close();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return lineRecordReader.getProgress();
    }

    /**
     * Get the line record reader.
     *
     * @return Record reader to be used for reading.
     */
    protected RecordReader<LongWritable, Text> getRecordReader() {
      return lineRecordReader;
    }

    /**
     * Get the context.
     *
     * @return Context passed to initialize.
     */
    protected TaskAttemptContext getContext() {
      return context;
    }
  }

  /**
   * Abstract class to be implemented by the user to read a vertex value from
   * each text line.
   */
  protected abstract class TextVertexValueReaderFromEachLine extends
      TextVertexValueReader {
    @Override
    public final I getCurrentVertexId() throws IOException,
        InterruptedException {
      return getId(getRecordReader().getCurrentValue());
    }

    @Override
    public final V getCurrentVertexValue() throws IOException,
        InterruptedException {
      return getValue(getRecordReader().getCurrentValue());
    }

    @Override
    public final boolean nextVertex() throws IOException, InterruptedException {
      return getRecordReader().nextKeyValue();
    }

    /**
     * Reads vertex id from the current line.
     *
     * @param line the current line
     * @return the vertex id corresponding to the line
     * @throws IOException exception that can be thrown while reading
     */
    protected abstract I getId(Text line) throws IOException;

    /**
     * Reads vertex value from the current line.
     *
     * @param line the current line
     * @return the vertex value corresponding to the line
     * @throws IOException
     *           exception that can be thrown while reading
     */
    protected abstract V getValue(Text line) throws IOException;
  }

  /**
   * Abstract class to be implemented by the user to read a vertex value from
   * each text line after preprocessing it.
   *
   * @param <T> The resulting type of preprocessing.
   */
  protected abstract class TextVertexValueReaderFromEachLineProcessed<T>
      extends TextVertexValueReader {
    /** Last preprocessed line. */
    private T processedLine = null;

    /** Get last preprocessed line. Generate it if missing.
     *
     * @return The last preprocessed line
     * @throws IOException
     * @throws InterruptedException
     */
    private T getProcessedLine() throws IOException, InterruptedException {
      if (processedLine == null) {
        processedLine = preprocessLine(getRecordReader().getCurrentValue());
      }
      return processedLine;
    }

    @Override
    public I getCurrentVertexId() throws IOException,
        InterruptedException {
      return getId(getProcessedLine());
    }

    @Override
    public V getCurrentVertexValue() throws IOException,
        InterruptedException {
      return getValue(getProcessedLine());
    }

    @Override
    public final boolean nextVertex() throws IOException, InterruptedException {
      processedLine = null;
      return getRecordReader().nextKeyValue();
    }

    /**
     * Preprocess the line so other methods can easily read necessary
     * information for creating vertex.
     *
     * @param line the current line to be read
     * @return the preprocessed object
     * @throws IOException exception that can be thrown while reading
     */
    protected abstract T preprocessLine(Text line) throws IOException;

    /**
     * Reads vertex id from the preprocessed line.
     *
     * @param line
     *          the object obtained by preprocessing the line
     * @return the vertex id
     * @throws IOException exception that can be thrown while reading
     */
    protected abstract I getId(T line) throws IOException;

    /**
     * Reads vertex value from the preprocessed line.
     *
     * @param line the object obtained by preprocessing the line
     * @return the vertex value
     * @throws IOException exception that can be thrown while reading
     */
    protected abstract V getValue(T line) throws IOException;
  }
}
