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
import org.apache.giraph.io.SimpleVertexWriter;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.io.VertexWriter;
import org.apache.giraph.utils.CallableFactory;
import org.apache.giraph.utils.ProgressableUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * Class to use as {@link SuperstepOutput} when chosen VertexOutputFormat is
 * thread-safe.
 *
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge value
 */
public class MultiThreadedSuperstepOutput<I extends WritableComparable,
    V extends Writable, E extends Writable> implements
    SuperstepOutput<I, V, E> {
  /** Mapper context */
  private final Mapper<?, ?, ?, ?>.Context context;
  /** Configuration */
  private ImmutableClassesGiraphConfiguration<I, V, E> configuration;
  /** Vertex output format, used to get new vertex writers */
  private final VertexOutputFormat<I, V, E> vertexOutputFormat;
  /**
   * List of returned vertex writers, these can be reused and will all be
   * closed in the end of the application
   */
  private final List<VertexWriter<I, V, E>> availableVertexWriters;
  /** Vertex writes which were created by this class and are currently used */
  private final Set<VertexWriter<I, V, E>> occupiedVertexWriters;

  /**
   * Constructor
   *
   * @param conf    Configuration
   * @param context Mapper context
   */
  public MultiThreadedSuperstepOutput(
      ImmutableClassesGiraphConfiguration<I, V, E> conf,
      Mapper<?, ?, ?, ?>.Context context) {
    this.configuration = conf;
    vertexOutputFormat = conf.createWrappedVertexOutputFormat();
    this.context = context;
    availableVertexWriters = Lists.newArrayList();
    occupiedVertexWriters = Sets.newHashSet();
    vertexOutputFormat.preWriting(context);
  }

  @Override
  public synchronized SimpleVertexWriter<I, V, E> getVertexWriter() {
    VertexWriter<I, V, E> vertexWriter;
    if (availableVertexWriters.isEmpty()) {
      try {
        vertexWriter = vertexOutputFormat.createVertexWriter(context);
        vertexWriter.setConf(configuration);
        vertexWriter.initialize(context);
      } catch (IOException e) {
        throw new IllegalStateException("getVertexWriter: " +
            "IOException occurred", e);
      } catch (InterruptedException e) {
        throw new IllegalStateException("getVertexWriter: " +
            "InterruptedException occurred", e);
      }
    } else {
      vertexWriter =
          availableVertexWriters.remove(availableVertexWriters.size() - 1);
    }
    occupiedVertexWriters.add(vertexWriter);
    return vertexWriter;
  }

  @Override
  public synchronized void returnVertexWriter(
      SimpleVertexWriter<I, V, E> vertexWriter) {
    VertexWriter<I, V, E> returnedWriter = (VertexWriter<I, V, E>) vertexWriter;
    if (!occupiedVertexWriters.remove(returnedWriter)) {
      throw new IllegalStateException("returnVertexWriter: " +
          "Returned vertex writer which is not currently occupied!");
    }
    availableVertexWriters.add(returnedWriter);
  }

  @Override
  public synchronized void postApplication() throws IOException,
      InterruptedException {
    if (!occupiedVertexWriters.isEmpty()) {
      throw new IllegalStateException("postApplication: " +
          occupiedVertexWriters.size() +
          " vertex writers were not returned!");
    }

    // Closing writers can take time - use multiple threads and call progress
    CallableFactory<Void> callableFactory = new CallableFactory<Void>() {
      @Override
      public Callable<Void> newCallable(int callableId) {
        return new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            while (true) {
              VertexWriter<I, V, E> vertexWriter;
              synchronized (availableVertexWriters) {
                if (availableVertexWriters.isEmpty()) {
                  return null;
                }
                vertexWriter = availableVertexWriters.remove(
                    availableVertexWriters.size() - 1);
              }
              vertexWriter.close(context);
            }
          }
        };
      }
    };
    ProgressableUtils.getResultsWithNCallables(callableFactory,
        Math.min(configuration.getNumOutputThreads(),
            availableVertexWriters.size()), "close-writers-%d", context);
    vertexOutputFormat.postWriting(context);
  }
}
