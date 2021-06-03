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
package org.apache.giraph.block_app.framework.api.local;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.giraph.block_app.framework.BlockFactory;
import org.apache.giraph.block_app.framework.BlockUtils;
import org.apache.giraph.block_app.framework.api.local.InternalApi.InternalWorkerApi;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.internal.BlockMasterLogic;
import org.apache.giraph.block_app.framework.internal.BlockWorkerContextLogic;
import org.apache.giraph.block_app.framework.internal.BlockWorkerLogic;
import org.apache.giraph.block_app.framework.internal.BlockWorkerPieces;
import org.apache.giraph.block_app.framework.output.BlockOutputHandle;
import org.apache.giraph.conf.BooleanConfOption;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.graph.OnlyIdVertex;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.SimpleVertexWriter;
import org.apache.giraph.partition.Partition;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.giraph.utils.TestGraph;
import org.apache.giraph.utils.Trimmable;
import org.apache.giraph.utils.WritableUtils;
import org.apache.giraph.writable.kryo.KryoWritableWrapper;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.Progressable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

/**
 * Local in-memory Block application job runner.
 * Implementation should be faster then using InternalVertexRunner.
 *
 * Useful for fast testing.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class LocalBlockRunner {
  /** Number of threads to use */
  public static final IntConfOption NUM_THREADS = new IntConfOption(
      "test.LocalBlockRunner.NUM_THREADS", 3, "");
  /** Number of vertex partitions */
  public static final IntConfOption NUM_PARTITIONS = new IntConfOption(
      "test.LocalBlockRunner.NUM_PARTITIONS", 16, "");
  /**
   * Whether to run all supported checks. Disable if you are running this
   * not within a unit test, and on a large graph, where performance matters.
   */
  public static final BooleanConfOption RUN_ALL_CHECKS = new BooleanConfOption(
      "test.LocalBlockRunner.RUN_ALL_CHECKS", true, "");
  // merge into RUN_ALL_CHECKS, after SERIALIZE_MASTER starts working
  public static final BooleanConfOption SERIALIZE_MASTER =
      new BooleanConfOption(
          "test.LocalBlockRunner.SERIALIZE_MASTER", false, "");

  private LocalBlockRunner() { }

  /**
   * Run Block Application specified within the conf, on a given graph,
   * locally, in-memory.
   *
   * With a boolean flag, you can switch between LocalBlockRunner and
   * InternalVertexRunner implementations of local in-memory computation.
   */
  public static
  <I extends WritableComparable, V extends Writable, E extends Writable>
  TestGraph<I, V, E> runApp(
      TestGraph<I, V, E> graph, boolean useFullDigraphTests) throws Exception {
    if (useFullDigraphTests) {
      return InternalVertexRunner.runWithInMemoryOutput(graph.getConf(), graph);
    } else {
      runApp(graph);
      return graph;
    }
  }

  /**
   * Run Block Application specified within the conf, on a given graph,
   * locally, in-memory.
   */
  public static
  <I extends WritableComparable, V extends Writable, E extends Writable>
  void runApp(TestGraph<I, V, E> graph) {
    SimpleVertexWriter<I, V, E> noOpVertexSaver = noOpVertexSaver();
    runAppWithVertexOutput(graph, noOpVertexSaver);
  }

  /**
   * Run Block from a specified execution stage on a given graph,
   * locally, in-memory.
   */
  public static
  <I extends WritableComparable, V extends Writable, E extends Writable>
  void runBlock(
      TestGraph<I, V, E> graph, Block block, Object executionStage) {
    SimpleVertexWriter<I, V, E> noOpVertexSaver = noOpVertexSaver();
    runBlockWithVertexOutput(
        block, executionStage, graph, noOpVertexSaver);
  }


  /**
   * Run Block Application specified within the conf, on a given graph,
   * locally, in-memory, with a given vertexSaver.
   */
  public static
  <I extends WritableComparable, V extends Writable, E extends Writable>
  void runAppWithVertexOutput(
      TestGraph<I, V, E> graph, final SimpleVertexWriter<I, V, E> vertexSaver) {
    BlockFactory<?> factory = BlockUtils.createBlockFactory(graph.getConf());
    runBlockWithVertexOutput(
        factory.createBlock(graph.getConf()),
        factory.createExecutionStage(graph.getConf()),
        graph, vertexSaver);
  }

  /**
   * Run Block from a specified execution stage on a given graph,
   * locally, in-memory, with a given vertexSaver.
   */
  public static
  <I extends WritableComparable, V extends Writable, E extends Writable>
  void runBlockWithVertexOutput(
      Block block, Object executionStage, TestGraph<I, V, E> graph,
      final SimpleVertexWriter<I, V, E> vertexSaver
  ) {
    Preconditions.checkNotNull(block);
    Preconditions.checkNotNull(graph);
    ImmutableClassesGiraphConfiguration<I, V, E> conf = graph.getConf();
    int numThreads = NUM_THREADS.get(conf);
    int numPartitions = NUM_PARTITIONS.get(conf);
    boolean runAllChecks = RUN_ALL_CHECKS.get(conf);
    boolean serializeMaster = SERIALIZE_MASTER.get(conf);
    final boolean doOutputDuringComputation = conf.doOutputDuringComputation();

    final InternalApi internalApi =
        new InternalApi(graph, conf, numPartitions, runAllChecks);
    final InternalWorkerApi internalWorkerApi = internalApi.getWorkerApi();

    BlockUtils.checkBlockTypes(block, executionStage, conf);

    BlockMasterLogic<Object> blockMasterLogic = new BlockMasterLogic<>();
    blockMasterLogic.initialize(block, executionStage, internalApi);

    BlockWorkerContextLogic workerContextLogic =
        internalApi.getWorkerContextLogic();
    workerContextLogic.preApplication(internalWorkerApi,
        new BlockOutputHandle("", conf, new Progressable() {
          @Override
          public void progress() {
          }
        }));

    ExecutorService executor = Executors.newFixedThreadPool(numThreads);

    if (runAllChecks) {
      for (Vertex<I, V, E> vertex : graph) {
        V value = conf.createVertexValue();
        WritableUtils.copyInto(vertex.getValue(), value);
        vertex.setValue(value);

        vertex.setEdges((Iterable) WritableUtils.createCopy(
            (Writable) vertex.getEdges(), conf.getOutEdgesClass(), conf));
      }
    }

    final AtomicBoolean anyVertexAlive = new AtomicBoolean(true);

    for (int superstep = 0;; superstep++) {
      // serialize master to test continuable computation
      if (serializeMaster) {
        blockMasterLogic = (BlockMasterLogic) WritableUtils.createCopy(
            new KryoWritableWrapper<>(blockMasterLogic),
            KryoWritableWrapper.class,
            conf).get();
        blockMasterLogic.initializeAfterRead(internalApi);
      }

      if (!anyVertexAlive.get()) {
        break;
      }

      final BlockWorkerPieces workerPieces =
          blockMasterLogic.computeNext(superstep);
      if (workerPieces == null) {
        if (!conf.doOutputDuringComputation()) {
          List<Partition<I, V, E>> partitions = internalApi.getPartitions();
          for (Partition<I, V, E> partition : partitions) {
            for (Vertex<I, V, E> vertex : partition) {
              try {
                vertexSaver.writeVertex(vertex);
              } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
              }
            }
          }
        }
        int left = executor.shutdownNow().size();
        Preconditions.checkState(0 == left, "Some work still left to be done?");
        break;
      } else {
        internalApi.afterMasterBeforeWorker(workerPieces);
        List<Partition<I, V, E>> partitions = internalApi.getPartitions();

        workerContextLogic.preSuperstep(
            internalWorkerApi,
            internalWorkerApi,
            KryoWritableWrapper.wrapAndCopy(workerPieces), superstep,
            internalApi.takeWorkerMessages());

        final CountDownLatch latch = new CountDownLatch(numPartitions);
        final AtomicReference<Throwable> exception = new AtomicReference<>();
        anyVertexAlive.set(false);
        for (final Partition<I, V, E> partition : partitions) {
          executor.execute(new Runnable() {
            @Override
            public void run() {
              try {
                boolean anyCurVertexAlive = false;
                BlockWorkerPieces localPieces =
                    KryoWritableWrapper.wrapAndCopy(workerPieces);

                BlockWorkerLogic localLogic = new BlockWorkerLogic(localPieces);
                localLogic.preSuperstep(internalWorkerApi, internalWorkerApi);

                if (internalApi.ignoreExistingVertices()) {
                  Iterable<I> destinations =
                      internalApi.getPartitionDestinationVertices(
                          partition.getId());
                  if (!Iterables.isEmpty(destinations)) {
                    OnlyIdVertex<I> vertex = new OnlyIdVertex<>();

                    for (I vertexId : destinations) {
                      Iterable messages = internalApi.takeMessages(vertexId);
                      Preconditions.checkState(!Iterables.isEmpty(messages));
                      vertex.setId(vertexId);
                      localLogic.compute(vertex, messages);

                      anyCurVertexAlive = true;
                    }
                  }
                } else {
                  for (Vertex<I, V, E> vertex : partition) {
                    Iterable messages =
                        internalApi.takeMessages(vertex.getId());
                    if (vertex.isHalted() && !Iterables.isEmpty(messages)) {
                      vertex.wakeUp();
                    }
                    // Equivalent of ComputeCallable.computePartition
                    if (!vertex.isHalted()) {
                      localLogic.compute(vertex, messages);

                      // Need to unwrap the mutated edges (possibly)
                      vertex.unwrapMutableEdges();
                      //Compact edges representation if possible
                      if (vertex instanceof Trimmable) {
                        ((Trimmable) vertex).trim();
                      }
                      // Write vertex to superstep output
                      // (no-op if it is not used)
                      if (doOutputDuringComputation) {
                        vertexSaver.writeVertex(vertex);
                      }
                      // Need to save the vertex changes (possibly)
                      partition.saveVertex(vertex);
                    }

                    if (!vertex.isHalted()) {
                      anyCurVertexAlive = true;
                    }
                  }
                }

                if (anyCurVertexAlive) {
                  anyVertexAlive.set(true);
                }
                localLogic.postSuperstep();
              // CHECKSTYLE: stop IllegalCatch
              // Need to propagate all exceptions within test
              } catch (Throwable t) {
              // CHECKSTYLE: resume IllegalCatch
                t.printStackTrace();
                exception.set(t);
              }

              latch.countDown();
            }
          });
        }

        try {
          latch.await();
        } catch (InterruptedException e) {
          throw new RuntimeException("Thread intentionally interrupted", e);
        }

        if (exception.get() != null) {
          throw new RuntimeException("Worker failed", exception.get());
        }

        workerContextLogic.postSuperstep();

        internalApi.afterWorkerBeforeMaster();
      }
    }

    workerContextLogic.postApplication();
    internalApi.postApplication();
  }

  private static
  <I extends WritableComparable, E extends Writable, V extends Writable>
  SimpleVertexWriter<I, V, E> noOpVertexSaver() {
    return new SimpleVertexWriter<I, V, E>() {
      @Override
      public void writeVertex(Vertex<I, V, E> vertex)
          throws IOException, InterruptedException {
        // No-op
      }
    };
  }

}
