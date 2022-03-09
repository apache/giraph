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

package org.apache.giraph.benchmark;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.PseudoRandomInputFormatConstants;
import org.apache.giraph.io.formats.PseudoRandomVertexInputFormat;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.reducers.ReduceSameTypeOperation;
import org.apache.giraph.worker.DefaultWorkerContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.collect.Sets;

/**
 * Benchmark for reducers. Also checks the correctness.
 */
public class ReducersBenchmark extends GiraphBenchmark {
  /** Number of reducers setting */
  private static final String REDUCERS_NUM = "reducersbenchmark.num";

  /** Option for number of reducers */
  private static final BenchmarkOption REDUCERS =
      new BenchmarkOption("r", "reducers",
          true, "Reducers", "Need to set number of reducers (-r)");

  /** LongSumReducer */
  public static class TestLongSumReducer
      extends ReduceSameTypeOperation<LongWritable> {
    /** Singleton */
    public static final TestLongSumReducer INSTANCE = new TestLongSumReducer();

    @Override
    public LongWritable createInitialValue() {
      return new LongWritable();
    }

    @Override
    public LongWritable reduce(
        LongWritable curValue, LongWritable valueToReduce) {
      curValue.set(curValue.get() + valueToReduce.get());
      return curValue;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
    }

    @Override
    public void write(DataOutput out) throws IOException {
    }
  }

  /**
   * Vertex class for ReducersBenchmark
   */
  public static class ReducersBenchmarkComputation extends
      BasicComputation<LongWritable, DoubleWritable, DoubleWritable,
          DoubleWritable> {
    @Override
    public void compute(
        Vertex<LongWritable, DoubleWritable, DoubleWritable> vertex,
        Iterable<DoubleWritable> messages) throws IOException {
      int n = getNumReducers(getConf());
      long superstep = getSuperstep();
      int w = getWorkerContextReduced(getConf(), superstep);
      for (int i = 0; i < n; i++) {
        reduce("w" + i, new LongWritable((superstep + 1) * i));
        reduce("p" + i, new LongWritable(i));

        if (superstep > 0) {
          assertEquals(superstep * (getTotalNumVertices() * i) + w,
              ((LongWritable) getBroadcast("w" + i)).get());
          assertEquals(-(superstep * i),
              ((LongWritable) getBroadcast("m" + i)).get());
          assertEquals(superstep * getTotalNumVertices() * i,
              ((LongWritable) getBroadcast("p" + i)).get());
        }
      }
      if (superstep > 2) {
        vertex.voteToHalt();
      }
    }
  }

  /**
   * MasterCompute class for ReducersBenchmark
   */
  public static class ReducersBenchmarkMasterCompute extends
      DefaultMasterCompute {
    @Override
    public void compute() {
      int n = getNumReducers(getConf());
      long superstep = getSuperstep();
      int w = getWorkerContextReduced(getConf(), superstep);
      for (int i = 0; i < n; i++) {
        String wi = "w" + i;
        String mi = "m" + i;
        String pi = "p" + i;

        registerReducer(wi, TestLongSumReducer.INSTANCE);
        registerReducer(mi, new TestLongSumReducer());

        if (superstep > 0) {
          broadcast(wi, getReduced(wi));
          broadcast(mi, new LongWritable(-superstep * i));
          broadcast(pi, getReduced(pi));

          registerReducer(pi, new TestLongSumReducer(),
              (LongWritable) getReduced(pi));

          assertEquals(superstep * (getTotalNumVertices() * i) + w,
              ((LongWritable) getReduced(wi)).get());
          assertEquals(superstep * getTotalNumVertices() * i,
              ((LongWritable) getReduced(pi)).get());
        } else {
          registerReducer(pi, new TestLongSumReducer());
        }
      }
    }
  }

  /**
   * WorkerContext class for ReducersBenchmark
   */
  public static class ReducersBenchmarkWorkerContext
      extends DefaultWorkerContext {
    @Override
    public void preSuperstep() {
      addToWorkerReducers(1);
      checkReducers();
    }

    @Override
    public void postSuperstep() {
      addToWorkerReducers(2);
      checkReducers();
    }

    /**
     * Check if reducer values are correct for current superstep
     */
    private void checkReducers() {
      int n = getNumReducers(getContext().getConfiguration());
      long superstep = getSuperstep();
      int w = getWorkerContextReduced(
          getContext().getConfiguration(), superstep);
      for (int i = 0; i < n; i++) {
        if (superstep > 0) {
          assertEquals(superstep * (getTotalNumVertices() * i) + w,
              ((LongWritable) getBroadcast("w" + i)).get());
          assertEquals(-(superstep * i),
              ((LongWritable) getBroadcast("m" + i)).get());
          assertEquals(superstep * getTotalNumVertices() * i,
              ((LongWritable) getBroadcast("p" + i)).get());
        }
      }
    }

    /**
     * Add some value to worker reducers.
     *
     * @param valueToAdd Which value to add
     */
    private void addToWorkerReducers(int valueToAdd) {
      int n = getNumReducers(getContext().getConfiguration());
      for (int i = 0; i < n; i++) {
        reduce("w" + i, new LongWritable(valueToAdd));
      }
    }
  }

  /**
   * Get the number of reducers from configuration
   *
   * @param conf Configuration
   * @return Number of reducers
   */
  private static int getNumReducers(Configuration conf) {
    return conf.getInt(REDUCERS_NUM, 0);
  }

  /**
   * Get the value which should be reduced by worker context
   *
   * @param conf Configuration
   * @param superstep Superstep
   * @return The value which should be reduced by worker context
   */
  private static int getWorkerContextReduced(Configuration conf,
      long superstep) {
    return (superstep <= 0) ? 0 : conf.getInt("workers", 0) * 3;
  }

  /**
   * Check if values are equal, throw an exception if they aren't
   *
   * @param expected Expected value
   * @param actual Actual value
   */
  private static void assertEquals(long expected, long actual) {
    if (expected != actual) {
      throw new RuntimeException("expected: " + expected +
          ", actual: " + actual);
    }
  }

  @Override
  public Set<BenchmarkOption> getBenchmarkOptions() {
    return Sets.newHashSet(BenchmarkOption.VERTICES, REDUCERS);
  }

  @Override
  protected void prepareConfiguration(GiraphConfiguration conf,
      CommandLine cmd) {
    conf.setComputationClass(ReducersBenchmarkComputation.class);
    conf.setMasterComputeClass(ReducersBenchmarkMasterCompute.class);
    conf.setVertexInputFormatClass(PseudoRandomVertexInputFormat.class);
    conf.setWorkerContextClass(ReducersBenchmarkWorkerContext.class);
    conf.setLong(PseudoRandomInputFormatConstants.AGGREGATE_VERTICES,
        BenchmarkOption.VERTICES.getOptionLongValue(cmd));
    conf.setLong(PseudoRandomInputFormatConstants.EDGES_PER_VERTEX, 1);
    conf.setInt(REDUCERS_NUM, REDUCERS.getOptionIntValue(cmd));
    conf.setInt("workers", conf.getInt(GiraphConstants.MAX_WORKERS, -1));
  }

  /**
   * Execute the benchmark.
   *
   * @param args Typically the command line arguments.
   * @throws Exception Any exception from the computation.
   */
  public static void main(final String[] args) throws Exception {
    System.exit(ToolRunner.run(new ReducersBenchmark(), args));
  }
}
