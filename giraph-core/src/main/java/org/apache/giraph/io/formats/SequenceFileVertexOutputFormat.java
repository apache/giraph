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

import java.io.IOException;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.io.VertexWriter;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * Sequence file vertex output format. It allows to convert a vertex into a key
 * and value pair of desired types, and output the pair into a sequence file.
 * A subclass has to provide two conversion methods convertToSequenceFileKey()
 * and convertToSequenceFileValue().
 *
 * @param <I> Vertex id type
 * @param <V> Vertex value type
 * @param <E> Edge value type
 * @param <OK> Output key data type for a sequence file
 * @param <OV> Output value data type for a sequence file
 */
public abstract class SequenceFileVertexOutputFormat<
  I extends WritableComparable,
  V extends Writable,
  E extends Writable,
  OK extends Writable,
  OV extends Writable>
  extends VertexOutputFormat<I, V, E> {
  /**
   * Output format of a sequence file that stores key-value pairs of the
   * desired types.
   */
  private SequenceFileOutputFormat<OK, OV> sequenceFileOutputFormat =
      new SequenceFileOutputFormat<OK, OV>();

  @Override
  public void checkOutputSpecs(JobContext context)
    throws IOException, InterruptedException {
    sequenceFileOutputFormat.checkOutputSpecs(context);
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
    throws IOException, InterruptedException {
    return sequenceFileOutputFormat.getOutputCommitter(context);
  }

  @Override
  public VertexWriter createVertexWriter(TaskAttemptContext
      context) throws IOException, InterruptedException {
    return new SequenceFileVertexWriter();
  }

  /**
   * Converts a vertex identifier into a sequence file key.
   * @param vertexId Vertex identifier.
   * @return Sequence file key.
   */
  protected abstract OK convertToSequenceFileKey(I vertexId);

  /**
   * Converts a vertex value into a sequence file value.
   * @param vertexValue Vertex value.
   * @return Sequence file value.
   */
  protected abstract OV convertToSequenceFileValue(V vertexValue);

  /**
   * Vertex writer that converts a vertex into a key-value pair and writes
   * the result into a sequence file for a context.
   */
  private class SequenceFileVertexWriter extends VertexWriter<I, V, E> {
    /**
     * A record writer that will write into a sequence file initialized for
     * a context.
     */
    private RecordWriter<OK, OV> recordWriter;

    @Override
    public void initialize(TaskAttemptContext context) throws IOException,
           InterruptedException {
      recordWriter = sequenceFileOutputFormat.getRecordWriter(context);
    }

    @Override
    public final void writeVertex(Vertex<I, V, E> vertex) throws
      IOException, InterruptedException {
      // Convert vertex id to type OK.
      OK outKey = convertToSequenceFileKey(vertex.getId());
      // Convert vertex value to type OV.
      OV outValue = convertToSequenceFileValue(vertex.getValue());
      recordWriter.write(outKey, outValue);
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException,
        InterruptedException {
      recordWriter.close(context);
    }
  }
}
