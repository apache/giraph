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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.giraph.block_app.framework.api.local.InternalApi.InternalWorkerApi;
import org.apache.giraph.block_app.framework.internal.BlockMasterLogic;
import org.apache.giraph.block_app.framework.internal.BlockWorkerContextLogic;
import org.apache.giraph.block_app.framework.internal.BlockWorkerLogic;
import org.apache.giraph.block_app.framework.internal.BlockWorkerPieces;
import org.apache.giraph.block_app.framework.output.BlockOutputHandle;
import org.apache.giraph.conf.BooleanConfOption;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.giraph.utils.TestGraph;
import org.apache.giraph.utils.WritableUtils;
import org.apache.giraph.writable.kryo.KryoWritableWrapper;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.Progressable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

/**
 * Local in-memory Block application job runner, used for testing.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class LocalBlockRunner {
  public static final IntConfOption NUM_WORKERS = new IntConfOption(
      "test.LocalBlockRunner.NUM_WORKERS", 3, "");
  public static final BooleanConfOption RUN_ALL_CHECKS = new BooleanConfOption(
      "test.LocalBlockRunner.RUN_ALL_CHECKS", true, "");
  // merge into RUN_ALL_CHECKS, after SERIALIZE_MASTER starts working
  public static final BooleanConfOption SERIALIZE_MASTER =
      new BooleanConfOption(
          "test.LocalBlockRunner.SERIALIZE_MASTER", false, "");

  private LocalBlockRunner() { }

  /**
   * With a boolean flag, you can switch between LocalBlockRunner and
   * InternalVertexRunner for running the unit test.
   */
  public static
  <I extends WritableComparable, V extends Writable, E extends Writable>
  TestGraph<I, V, E> runWithInMemoryOutput(
      TestGraph<I, V, E> graph, GiraphConfiguration conf,
      boolean useFullDigraphTests) throws Exception {
    if (useFullDigraphTests) {
      return InternalVertexRunner.runWithInMemoryOutput(conf, graph);
    } else {
      runWithInMemoryOutput(graph, conf);
      return graph;
    }
  }

  public static
  <I extends WritableComparable, V extends Writable, E extends Writable>
  void runWithInMemoryOutput(
      TestGraph<I, V, E> graph, GiraphConfiguration conf) throws Exception {
    VertexSaver<I, V, E> noOpVertexSaver = new VertexSaver<I, V, E>() {
      @Override
      public void saveVertex(Vertex<I, V, E> vertex) {
        // No-op
      }
    };
    runWithVertexSaverOutput(graph, noOpVertexSaver, conf);
  }

  public static
  <I extends WritableComparable, V extends Writable, E extends Writable>
  void runWithVertexSaverOutput(
      TestGraph<I, V, E> graph, final VertexSaver<I, V, E> vertexSaver,
      GiraphConfiguration conf) throws Exception {
    int numWorkers = NUM_WORKERS.get(conf);
    boolean runAllChecks = RUN_ALL_CHECKS.get(conf);
    boolean serializeMaster = SERIALIZE_MASTER.get(conf);
    final boolean doOutputDuringComputation = conf.doOutputDuringComputation();

    ImmutableClassesGiraphConfiguration<I, V, E> immConf =
        new ImmutableClassesGiraphConfiguration(conf);
    final InternalApi internalApi =
        new InternalApi(graph, immConf, runAllChecks);
    final InternalWorkerApi internalWorkerApi = internalApi.getWorkerApi();

    BlockMasterLogic<Object> blockMasterLogic = new BlockMasterLogic<>();
    blockMasterLogic.initialize(immConf, internalApi);

    BlockWorkerContextLogic workerContextLogic =
        internalApi.getWorkerContextLogic();
    workerContextLogic.preApplication(internalWorkerApi,
        new BlockOutputHandle("", conf, new Progressable() {
          @Override
          public void progress() {
          }
        }));

    ExecutorService executor = Executors.newFixedThreadPool(numWorkers);
    Random rand = new Random();

    if (runAllChecks) {
      for (Vertex<I, V, E> vertex : graph) {
        V value = immConf.createVertexValue();
        WritableUtils.copyInto(vertex.getValue(), value);
        vertex.setValue(value);

        vertex.setEdges((Iterable) WritableUtils.createCopy(
            (Writable) vertex.getEdges(), immConf.getOutEdgesClass(), immConf));
      }
    }

    final AtomicBoolean anyVertexAlive = new AtomicBoolean(true);

    for (int superstep = 0;; superstep++) {
      // serialize master to test continuable computation
      if (serializeMaster) {
        blockMasterLogic = (BlockMasterLogic) WritableUtils.createCopy(
            new KryoWritableWrapper<>(blockMasterLogic),
            KryoWritableWrapper.class,
            immConf).get();
        blockMasterLogic.initializeAfterRead(internalApi);
      }

      if (!anyVertexAlive.get()) {
        break;
      }

      final BlockWorkerPieces workerPieces =
          blockMasterLogic.computeNext(superstep);
      if (workerPieces == null) {
        if (!conf.doOutputDuringComputation()) {
          Collection<Vertex<I, V, E>> vertices = internalApi.getAllVertices();
          for (Vertex<I, V, E> vertex : vertices) {
            vertexSaver.saveVertex(vertex);
          }
        }
        int left = executor.shutdownNow().size();
        Preconditions.checkState(0 == left, "Some work still left to be done?");
        break;
      } else {
        internalApi.afterMasterBeforeWorker(workerPieces);
        List<List<Vertex<I, V, E>>> verticesPerWorker = new ArrayList<>();
        for (int i = 0; i < numWorkers; i++) {
          verticesPerWorker.add(new ArrayList<Vertex<I, V, E>>());
        }
        Collection<Vertex<I, V, E>> allVertices = internalApi.getAllVertices();
        for (Vertex<I, V, E> vertex : allVertices) {
          verticesPerWorker.get(rand.nextInt(numWorkers)).add(vertex);
        }

        workerContextLogic.preSuperstep(
            internalWorkerApi,
            internalWorkerApi,
            KryoWritableWrapper.wrapAndCopy(workerPieces), superstep,
            internalApi.takeWorkerMessages());

        final CountDownLatch latch = new CountDownLatch(numWorkers);
        final AtomicReference<Throwable> exception = new AtomicReference<>();
        anyVertexAlive.set(false);
        for (final List<Vertex<I, V, E>> curVertices : verticesPerWorker) {
          executor.execute(new Runnable() {
            @Override
            public void run() {
              try {
                boolean anyCurVertexAlive = false;
                BlockWorkerPieces localPieces =
                    KryoWritableWrapper.wrapAndCopy(workerPieces);

                BlockWorkerLogic localLogic = new BlockWorkerLogic(localPieces);
                localLogic.preSuperstep(internalWorkerApi, internalWorkerApi);

                for (Vertex<I, V, E> vertex : curVertices) {
                  Iterable messages = internalApi.takeMessages(vertex.getId());
                  if (vertex.isHalted() && !Iterables.isEmpty(messages)) {
                    vertex.wakeUp();
                  }
                  if (!vertex.isHalted()) {
                    localLogic.compute(vertex, messages);
                    if (doOutputDuringComputation) {
                      vertexSaver.saveVertex(vertex);
                    }
                  }

                  if (!vertex.isHalted()) {
                    anyCurVertexAlive = true;
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

        latch.await();
        if (exception.get() != null) {
          throw new RuntimeException("Worker failed", exception.get());
        }

        workerContextLogic.postSuperstep();

        internalApi.afterWorkerBeforeMaster();
      }
    }

    workerContextLogic.postApplication();
  }
}
