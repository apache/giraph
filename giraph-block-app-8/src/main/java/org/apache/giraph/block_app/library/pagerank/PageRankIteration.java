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

package org.apache.giraph.block_app.library.pagerank;

import org.apache.giraph.block_app.framework.api.BlockMasterApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerReceiveApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.api.CreateReducersApi;
import org.apache.giraph.block_app.framework.piece.Piece;
import org.apache.giraph.block_app.framework.piece.global_comm.ReducerAndBroadcastWrapperHandle;
import org.apache.giraph.block_app.framework.piece.global_comm.ReducerHandle;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexReceiver;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexSender;
import org.apache.giraph.combiner.MessageCombiner;
import org.apache.giraph.combiner.SumMessageCombiner;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.function.Consumer;
import org.apache.giraph.function.ObjectTransfer;
import org.apache.giraph.function.vertex.ConsumerWithVertex;
import org.apache.giraph.function.vertex.SupplierFromVertex;
import org.apache.giraph.reducers.impl.MaxReduce;
import org.apache.giraph.reducers.impl.SumReduce;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

/**
 * Single iteration of page rank
 *
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge value
 */
public class PageRankIteration<I extends WritableComparable,
    V extends Writable, E extends Writable> extends
    Piece<I, V, E, DoubleWritable, Object> {
  /** Logger */
  private static final Logger LOG = Logger.getLogger(PageRankIteration.class);

  /** Consumer which sets pagerank value in vertex */
  private final ConsumerWithVertex<I, V, E, DoubleWritable>
      valueSetter;
  /** Supplier which reads pagerank value from vertex */
  private final SupplierFromVertex<I, V, E, DoubleWritable>
      valueGetter;
  /** Supplier which reads edge value from an edge */
  private final EdgeValueGetter<I, V, E> edgeValueGetter;
  /**
   * Object transfer for passing new pagerank value between consecutive
   * PageRankIteration pieces
   */
  private final ObjectTransfer<DoubleWritable> valueTransfer;
  /** Consumer which sets halt condition based on convergence criteria */
  private final Consumer<Boolean> haltCondition;

  /** Damping factor */
  private final double dampingFactor;
  /** Convergence type */
  private final PageRankConvergenceType convergenceType;
  /** Convergence threshold */
  private final float convergenceThreshold;

  /** Sums the errors for each vertex */
  private ReducerHandle<DoubleWritable, DoubleWritable> superstepErrorSum;
  /** Maximum of the errors for each vertex */
  private ReducerHandle<DoubleWritable, DoubleWritable> superstepErrorMax;
  /** Sums the RMSE errors for each vertex */
  private ReducerHandle<DoubleWritable, DoubleWritable> superstepErrorRMSESum;
  /** Sums the errors for each vertex */
  private ReducerHandle<DoubleWritable, DoubleWritable>
      superstepRelativeErrorSum;
  /** Maximum of the errors for each vertex */
  private ReducerHandle<DoubleWritable, DoubleWritable>
      superstepRelativeErrorMax;
  /** Number of modified vertices */
  private ReducerHandle<LongWritable, LongWritable> verticesModified;

  /** Sum of pagerank values for sink vertices */
  private final
  ReducerAndBroadcastWrapperHandle<DoubleWritable, DoubleWritable>
      superstepPageRankSinks = new ReducerAndBroadcastWrapperHandle<>();
  /** Sum of all pagerank values */
  private final
  ReducerAndBroadcastWrapperHandle<DoubleWritable, DoubleWritable>
      superstepPageRankAll = new ReducerAndBroadcastWrapperHandle<>();

  /**
   * Constructor
   *
   * @param valueSetter Consumer which sets pagerank value in vertex
   * @param valueGetter Supplier which reads pagerank value from vertex
   * @param edgeValueGetter Supplier which reads edge value from an edge
   * @param valueTransfer Object transfer for passing new pagerank value
   *                      between consecutive PageRankIteration pieces
   * @param haltCondition Consumer which sets halt condition based on
   *                      convergence criteria
   * @param conf Configuration
   */
  public PageRankIteration(
      ConsumerWithVertex<I, V, E, DoubleWritable> valueSetter,
      SupplierFromVertex<I, V, E, DoubleWritable> valueGetter,
      EdgeValueGetter<I, V, E> edgeValueGetter,
      ObjectTransfer<DoubleWritable> valueTransfer,
      ObjectTransfer<Boolean> haltCondition,
      GiraphConfiguration conf) {
    this.valueSetter = valueSetter;
    this.valueGetter = valueGetter;
    this.edgeValueGetter = edgeValueGetter;
    this.valueTransfer = valueTransfer;
    this.haltCondition = haltCondition;
    dampingFactor = PageRankSettings.getDampingFactor(conf);
    convergenceType = PageRankSettings.getConvergenceType(conf);
    convergenceThreshold = PageRankSettings.getConvergenceThreshold(conf);
  }


  @Override
  public VertexSender<I, V, E> getVertexSender(
      final BlockWorkerSendApi<I, V, E, DoubleWritable> workerApi,
      Object executionStage) {
    final DoubleWritable message = new DoubleWritable();
    return vertex -> {
      DoubleWritable newValue = valueTransfer.get();
      // Update stats
      if (newValue != null) {
        DoubleWritable oldValue = valueGetter.get(vertex);
        double diff = Math.abs(oldValue.get() - newValue.get());
        reduceDouble(superstepErrorSum, diff);
        reduceDouble(superstepErrorMax, diff);

        reduceDouble(superstepErrorRMSESum, diff * diff);
        if (oldValue.get() > 0) {
          reduceDouble(superstepRelativeErrorSum, diff / oldValue.get());
          reduceDouble(superstepRelativeErrorMax, diff / oldValue.get());
        }
        valueSetter.apply(vertex, newValue);
        reduceLong(verticesModified, 1);
      }

      // Send pagerank value to neighbors, or update sink sum
      DoubleWritable value = valueGetter.get(vertex);
      superstepPageRankAll.reduce(value);
      if (vertex.getNumEdges() == 0) { // sink vertex
        superstepPageRankSinks.reduce(value);
      } else { // not a sink
        if (value.get() > 0) {
          if (edgeValueGetter.allVertexEdgesTheSame()) {
            message.set(value.get() *
                edgeValueGetter.getEdgeValue(vertex, null));
            workerApi.sendMessageToAllEdges(vertex, message);
          } else {
            for (Edge<I, E> edge : vertex.getEdges()) {
              message.set(value.get() *
                  edgeValueGetter.getEdgeValue(vertex, edge.getValue()));
              workerApi.sendMessage(edge.getTargetVertexId(), message);
            }
          }
        }
      }
    };
  }

  @Override
  public void masterCompute(BlockMasterApi masterApi, Object executionStage) {
    if (LOG.isInfoEnabled()) {
      LOG.info("Superstep statistics:");
      LOG.info("\t sum_error: " + superstepErrorSum.getReducedValue(masterApi));
      LOG.info("\t max_error: " + superstepErrorMax.getReducedValue(masterApi));
      LOG.info("\t relative_sum_error: " +
          superstepRelativeErrorSum.getReducedValue(masterApi));
      LOG.info("\t relative_max_error: " +
          superstepRelativeErrorMax.getReducedValue(masterApi));
      LOG.info("\t rmse_error: " +
          superstepErrorRMSESum.getReducedValue(masterApi));
      LOG.info("\t sink_sum: " +
          superstepPageRankSinks.getReducedValue(masterApi));
      LOG.info("\t all_sum: " +
          superstepPageRankAll.getReducedValue(masterApi));
    }

    superstepPageRankSinks.broadcastValue(masterApi);
    superstepPageRankAll.broadcastValue(masterApi);

    // For each superstep check the convergence criteria
    ReducerHandle<DoubleWritable, DoubleWritable> reducerToCheckForConvergence;
    switch (convergenceType) {
    case SUM_DIFFERENCES:
      reducerToCheckForConvergence = superstepErrorSum;
      break;
    case MAX_DIFFERENCES:
      reducerToCheckForConvergence = superstepErrorMax;
      break;
    case SUM_RELATIVE_DIFFERENCES:
      reducerToCheckForConvergence = superstepRelativeErrorSum;
      break;
    case MAX_RELATIVE_DIFFERENCES:
      reducerToCheckForConvergence = superstepRelativeErrorMax;
      break;
    case RMSE_DIFFERENCES:
      reducerToCheckForConvergence = superstepErrorRMSESum;
      break;
    default:
      reducerToCheckForConvergence = null;
      break;
    }
    boolean shouldHalt = reducerToCheckForConvergence != null &&
        reducerToCheckForConvergence.getReducedValue(masterApi).get() <
            convergenceThreshold;
    // If halt condition is met and it's not the first iteration
    haltCondition.apply(shouldHalt &&
        verticesModified.getReducedValue(masterApi).get() > 0);
  }

  @Override
  public VertexReceiver<I, V, E, DoubleWritable> getVertexReceiver(
      final BlockWorkerReceiveApi<I> workerApi, Object executionStage) {
    double sinkSum = superstepPageRankSinks.getBroadcast(workerApi).get();
    double allSum = superstepPageRankAll.getBroadcast(workerApi).get();
    DoubleWritable reusableDoubleWritable = new DoubleWritable();
    return (vertex, messages) -> {
      double newValue = calculateNewValue(workerApi.getTotalNumVertices(),
          sinkSum, allSum, messages);
      reusableDoubleWritable.set(newValue);
      valueTransfer.apply(reusableDoubleWritable);
    };
  }

  /**
   * Calculates the new value of pagerank at a vertex
   *
   * @param messages Messages
   * @return new value or change
   */
  protected double calculateNewValue(long totalVertices, double sinkSum,
      double allSum, Iterable<DoubleWritable> messages) {
    double sum = 0.0;
    for (DoubleWritable message : messages) {
      sum += message.get();
    }
    // Every vertex also receives equal fraction of pagerank value from sinks
    sum += sinkSum / totalVertices;
    return dampingFactor * sum +
        (1.0 - dampingFactor) * allSum / totalVertices;
  }

  @Override
  public void registerReducers(
      CreateReducersApi reduceApi, Object executionStage) {
    super.registerReducers(reduceApi, executionStage);
    superstepErrorSum = reduceApi.createLocalReducer(SumReduce.DOUBLE);
    superstepErrorRMSESum = reduceApi.createLocalReducer(SumReduce.DOUBLE);
    superstepErrorMax = reduceApi.createLocalReducer(MaxReduce.DOUBLE);
    superstepRelativeErrorSum = reduceApi.createLocalReducer(SumReduce.DOUBLE);
    superstepRelativeErrorMax = reduceApi.createLocalReducer(MaxReduce.DOUBLE);
    verticesModified = reduceApi.createLocalReducer(SumReduce.LONG);
    superstepPageRankSinks.registeredReducer(
        reduceApi.createLocalReducer(SumReduce.DOUBLE));
    superstepPageRankAll.registeredReducer(
        reduceApi.createLocalReducer(SumReduce.DOUBLE));
  }

  @Override
  protected MessageCombiner<? super I, DoubleWritable> getMessageCombiner(
      ImmutableClassesGiraphConfiguration conf) {
    return SumMessageCombiner.DOUBLE;
  }

  @Override
  protected boolean allowOneMessageToManyIdsEncoding() {
    return edgeValueGetter.allVertexEdgesTheSame();
  }
}
