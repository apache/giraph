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
package org.apache.giraph.debugger.examples.pagerank;

import java.io.IOException;

import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.GraphTaskManager;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

/**
 * Simple PageRank implementation in Giraph to be easily run with Graft.
 */
public class SimplePageRankComputation extends
  BasicComputation<LongWritable, DoubleWritable, NullWritable, DoubleWritable> {

  /** Sum aggregator name */
  public static final String SUM_AGG = "sum";
  /** Min aggregator name */
  public static final String MIN_AGG = "min";
  /** Max aggregator name */
  public static final String MAX_AGG = "max";
  /** Number of supersteps for this test */
  private static int MAX_SUPERSTEPS = 10;
  /** Logger */
  private static final Logger LOG = Logger
    .getLogger(SimplePageRankComputation.class);

  @Override
  public void initialize(
    GraphState graphState,
    WorkerClientRequestProcessor<LongWritable, DoubleWritable, NullWritable>
    workerClientRequestProcessor,
    GraphTaskManager<LongWritable, DoubleWritable, NullWritable>
    graphTaskManager,
    WorkerGlobalCommUsage workerGlobalCommUsage, WorkerContext workerContext) {
    MAX_SUPERSTEPS = workerContext.getConf().getInt(
      getClass().getName() + ".maxSupersteps", 10);
    super.initialize(graphState, workerClientRequestProcessor,
      graphTaskManager, workerGlobalCommUsage, workerContext);
  }

  @Override
  public void compute(
    Vertex<LongWritable, DoubleWritable, NullWritable> vertex,
    Iterable<DoubleWritable> messages) throws IOException {
    if (getSuperstep() >= 1) {
      double sum = 0;
      for (DoubleWritable message : messages) {
        sum += message.get();
      }
      DoubleWritable vertexValue = new DoubleWritable(
        (0.15f / getTotalNumVertices()) + 0.85f * sum);
      vertex.setValue(vertexValue);
      aggregate(MAX_AGG, vertexValue);
      aggregate(MIN_AGG, vertexValue);
      aggregate(SUM_AGG, new LongWritable(1));
      // LOG.info(vertex.getId() + ": PageRank=" + vertexValue + " max=" +
      // getAggregatedValue(MAX_AGG) + " min=" + getAggregatedValue(MIN_AGG));
    }

    if (getSuperstep() < MAX_SUPERSTEPS) {
      long edges = vertex.getNumEdges();
      sendMessageToAllEdges(vertex, new DoubleWritable(vertex.getValue().get() /
        edges));
    } else {
      vertex.voteToHalt();
    }
  }

}
