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

import java.io.IOException;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.PseudoRandomInputFormatConstants;
import org.apache.giraph.io.formats.PseudoRandomVertexInputFormat;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.utils.MasterLoggingAggregator;
import org.apache.giraph.worker.DefaultWorkerContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.collect.Sets;

/**
 * Benchmark for aggregators. Also checks the correctness.
 */
public class AggregatorsBenchmark extends GiraphBenchmark {
  /** Number of aggregators setting */
  private static final String AGGREGATORS_NUM = "aggregatorsbenchmark.num";

  /** Option for number of aggregators */
  private static final BenchmarkOption AGGREGATORS =
      new BenchmarkOption("a", "aggregators",
          true, "Aggregators", "Need to set number of aggregators (-a)");

  /**
   * Vertex class for AggregatorsBenchmark
   */
  public static class AggregatorsBenchmarkComputation extends
      BasicComputation<LongWritable, DoubleWritable, DoubleWritable,
          DoubleWritable> {
    @Override
    public void compute(
        Vertex<LongWritable, DoubleWritable, DoubleWritable> vertex,
        Iterable<DoubleWritable> messages) throws IOException {
      int n = getNumAggregators(getConf());
      long superstep = getSuperstep();
      int w = getWorkerContextAggregated(getConf(), superstep);
      for (int i = 0; i < n; i++) {
        aggregate("w" + i, new LongWritable((superstep + 1) * i));
        aggregate("p" + i, new LongWritable(i));

        assertEquals(superstep * (getTotalNumVertices() * i) + w,
            ((LongWritable) getAggregatedValue("w" + i)).get());
        assertEquals(-(superstep * i),
            ((LongWritable) getAggregatedValue("m" + i)).get());
        assertEquals(superstep * getTotalNumVertices() * i,
            ((LongWritable) getAggregatedValue("p" + i)).get());
      }
      if (superstep > 2) {
        vertex.voteToHalt();
      }
    }
  }

  /**
   * MasterCompute class for AggregatorsBenchmark
   */
  public static class AggregatorsBenchmarkMasterCompute extends
      DefaultMasterCompute {
    @Override
    public void initialize() throws InstantiationException,
        IllegalAccessException {
      int n = getNumAggregators(getConf());
      for (int i = 0; i < n; i++) {
        registerAggregator("w" + i, LongSumAggregator.class);
        registerAggregator("m" + i, LongSumAggregator.class);
        registerPersistentAggregator("p" + i, LongSumAggregator.class);
      }
    }

    @Override
    public void compute() {
      int n = getNumAggregators(getConf());
      long superstep = getSuperstep();
      int w = getWorkerContextAggregated(getConf(), superstep);
      for (int i = 0; i < n; i++) {
        setAggregatedValue("m" + i, new LongWritable(-superstep * i));

        if (superstep > 0) {
          assertEquals(superstep * (getTotalNumVertices() * i) + w,
              ((LongWritable) getAggregatedValue("w" + i)).get());
          assertEquals(superstep * getTotalNumVertices() * i,
              ((LongWritable) getAggregatedValue("p" + i)).get());
        }
      }
    }
  }

  /**
   * WorkerContext class for AggregatorsBenchmark
   */
  public static class AggregatorsBenchmarkWorkerContext
      extends DefaultWorkerContext {
    @Override
    public void preSuperstep() {
      addToWorkerAggregators(1);
      checkAggregators();
      MasterLoggingAggregator.aggregate("everything fine", this, getConf());
    }

    @Override
    public void postSuperstep() {
      addToWorkerAggregators(2);
      checkAggregators();
    }

    /**
     * Check if aggregator values are correct for current superstep
     */
    private void checkAggregators() {
      int n = getNumAggregators(getContext().getConfiguration());
      long superstep = getSuperstep();
      int w = getWorkerContextAggregated(
          getContext().getConfiguration(), superstep);
      for (int i = 0; i < n; i++) {
        assertEquals(superstep * (getTotalNumVertices() * i) + w,
            ((LongWritable) getAggregatedValue("w" + i)).get());
        assertEquals(-(superstep * i),
            ((LongWritable) getAggregatedValue("m" + i)).get());
        assertEquals(superstep * getTotalNumVertices() * i,
            ((LongWritable) getAggregatedValue("p" + i)).get());
      }
    }

    /**
     * Add some value to worker aggregators.
     *
     * @param valueToAdd Which value to add
     */
    private void addToWorkerAggregators(int valueToAdd) {
      int n = getNumAggregators(getContext().getConfiguration());
      for (int i = 0; i < n; i++) {
        aggregate("w" + i, new LongWritable(valueToAdd));
      }
    }
  }

  /**
   * Get the number of aggregators from configuration
   *
   * @param conf Configuration
   * @return Number of aggregators
   */
  private static int getNumAggregators(Configuration conf) {
    return conf.getInt(AGGREGATORS_NUM, 0);
  }

  /**
   * Get the value which should be aggreagted by worker context
   *
   * @param conf Configuration
   * @param superstep Superstep
   * @return The value which should be aggregated by worker context
   */
  private static int getWorkerContextAggregated(Configuration conf,
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
    return Sets.newHashSet(BenchmarkOption.VERTICES, AGGREGATORS);
  }

  @Override
  protected void prepareConfiguration(GiraphConfiguration conf,
      CommandLine cmd) {
    conf.setComputationClass(AggregatorsBenchmarkComputation.class);
    conf.setMasterComputeClass(AggregatorsBenchmarkMasterCompute.class);
    conf.setVertexInputFormatClass(PseudoRandomVertexInputFormat.class);
    conf.setWorkerContextClass(AggregatorsBenchmarkWorkerContext.class);
    conf.setLong(PseudoRandomInputFormatConstants.AGGREGATE_VERTICES,
        BenchmarkOption.VERTICES.getOptionLongValue(cmd));
    conf.setLong(PseudoRandomInputFormatConstants.EDGES_PER_VERTEX, 1);
    conf.setInt(AGGREGATORS_NUM, AGGREGATORS.getOptionIntValue(cmd));
    conf.setInt("workers", conf.getInt(GiraphConstants.MAX_WORKERS, -1));
    MasterLoggingAggregator.setUseMasterLoggingAggregator(true, conf);
  }

  /**
   * Execute the benchmark.
   *
   * @param args Typically the command line arguments.
   * @throws Exception Any exception from the computation.
   */
  public static void main(final String[] args) throws Exception {
    System.exit(ToolRunner.run(new AggregatorsBenchmark(), args));
  }
}
