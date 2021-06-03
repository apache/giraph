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

package org.apache.giraph.io.superstep_output;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.SimpleVertexWriter;
import org.apache.giraph.io.VertexWriter;
import org.apache.giraph.io.internal.WrappedVertexOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Class to use as {@link SuperstepOutput} when chosen VertexOutputFormat is
 * not thread-safe.
 *
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge value
 */
public class SynchronizedSuperstepOutput<I extends WritableComparable,
    V extends Writable, E extends Writable> implements
    SuperstepOutput<I, V, E> {
  /** Mapper context */
  private final Mapper<?, ?, ?, ?>.Context context;
  /** Main vertex writer */
  private final VertexWriter<I, V, E> vertexWriter;
  /** Vertex output format */
  private final WrappedVertexOutputFormat<I, V, E> vertexOutputFormat;
  /**
   * Simple vertex writer, wrapper for {@link #vertexWriter}.
   * Call to writeVertex is thread-safe.
   */
  private final SimpleVertexWriter<I, V, E> simpleVertexWriter;

  /**
   * Constructor
   *
   * @param conf Configuration
   * @param context Mapper context
   */
  @SuppressWarnings("unchecked")
  public SynchronizedSuperstepOutput(
      ImmutableClassesGiraphConfiguration<I, V, E> conf,
      Mapper<?, ?, ?, ?>.Context context) {
    this.context = context;
    try {
      vertexOutputFormat = conf.createWrappedVertexOutputFormat();
      vertexOutputFormat.preWriting(context);
      vertexWriter = vertexOutputFormat.createVertexWriter(context);
      vertexWriter.setConf(conf);
      vertexWriter.initialize(context);
    } catch (IOException e) {
      throw new IllegalStateException("SynchronizedSuperstepOutput: " +
          "IOException occurred", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException("SynchronizedSuperstepOutput: " +
          "InterruptedException occurred", e);
    }
    simpleVertexWriter = new SimpleVertexWriter<I, V, E>() {
      @Override
      public synchronized void writeVertex(
          Vertex<I, V, E> vertex) throws IOException, InterruptedException {
        vertexWriter.writeVertex(vertex);
      }
    };
  }

  @Override
  public SimpleVertexWriter<I, V, E> getVertexWriter() {
    return simpleVertexWriter;
  }

  @Override
  public void returnVertexWriter(SimpleVertexWriter<I, V, E> vertexWriter) {
  }

  @Override
  public void postApplication() throws IOException, InterruptedException {
    vertexWriter.close(context);
    vertexOutputFormat.postWriting(context);
  }
}
