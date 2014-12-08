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
package org.apache.giraph.debugger.examples.simpledebug;

import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.debugger.examples.exceptiondebug.BuggySimpleTriangleClosingComputation;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.LongWritable;

/**
 * Master compute associated with {@link BuggySimpleShortestPathsComputation}.
 * It handles dangling nodes.
 */
public class SimpleShortestPathsMaster extends DefaultMasterCompute {

  /**
   * Name of the aggregator keeping the number of vertices which have distance
   * less than three to the source vertex.
   */
  public static final String NV_DISTANCE_LESS_THAN_THREE_AGGREGATOR =
    "nvWithDistanceLessThanThree";

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public void compute() {
    System.out.print("Running SimpleShortestPathsMaster.compute. superstep " +
      getSuperstep() + "\n");
    LongWritable aggregatorValue = getAggregatedValue(
      NV_DISTANCE_LESS_THAN_THREE_AGGREGATOR);
    if (aggregatorValue != null) {
      System.out.print("At Master.compute() with aggregator: " +
        aggregatorValue.get() + "\n");
    }
    // if (getSuperstep() == 2) {
    // throw new IllegalArgumentException("DUMMY EXCEPTION FOR TESTING");
    // }

    // Dummy code for testing Instrumenter analysis
    if (getSuperstep() == 100000) {
      // which is extremely less likely to happen,
      setComputation(BuggySimpleTriangleClosingComputation.class);
    } else if (getSuperstep() == 200000) {
      try {
        setComputation((Class<? extends Computation>) Class.forName(
          "org.apache.giraph.debugger.examples.integrity." +
          "ConnectedComponentsActualComputation"));
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public void initialize() throws InstantiationException,
    IllegalAccessException {
    registerPersistentAggregator(NV_DISTANCE_LESS_THAN_THREE_AGGREGATOR,
      LongSumAggregator.class);
  }
}
