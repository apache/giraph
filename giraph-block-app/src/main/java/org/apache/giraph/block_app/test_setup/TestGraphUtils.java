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
package org.apache.giraph.block_app.test_setup;

import org.apache.giraph.block_app.framework.BlockUtils;
import org.apache.giraph.conf.BulkConfigurator;
import org.apache.giraph.block_app.framework.api.local.LocalBlockRunner;
import org.apache.giraph.conf.BooleanConfOption;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.function.Supplier;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Utility functions for running TestGraph unit tests.
 */
public class TestGraphUtils {
  /** modify locally for running full Digraph tests from IDE */
  public static final
  BooleanConfOption USE_FULL_GIRAPH_ENV_IN_TESTS = new BooleanConfOption(
      "giraph.blocks.test_setup.use_full_giraph_env_in_tests", false,
      "Whether to use full giraph environemnt for tests, " +
      "or only local implementation");

  // if you want to check stability of the test and make sure it passes always
  // test it with larger number, like ~10.
  private static int TEST_REPEAT_TIMES = 1;

  private TestGraphUtils() { }

  /**
   * Creates configuration using configurator, initializes the graph using
   * graphInitializer, and checks it via graphChecker.
   *
   * Supports using TEST_REPEAT_TIMES for running the same test multiple times.
   */
  public static
  <I extends WritableComparable, V extends Writable, E extends Writable>
  void runTest(
      final TestGraphModifier<? super I, ? super V, ? super E> graphInitializer,
      final TestGraphChecker<? super I, ? super V, ? super E> graphChecker,
      final BulkConfigurator configurator) throws Exception {
    repeat(
        repeatTimes(),
        new OneTest() {
          @Override
          public void test() throws Exception {
            GiraphConfiguration conf = new GiraphConfiguration();
            configurator.configure(conf);
            BlockUtils.initAndCheckConfig(conf);
            runTest(graphInitializer, graphChecker, conf);
          }
        });
  }

  /**
   * Uses provided configuration, initializes the graph using
   * graphInitializer, and checks it via graphChecker.
   */
  public static
  <I extends WritableComparable, E extends Writable, V extends Writable>
  void runTest(
      TestGraphModifier<? super I, ? super V, ? super E> graphInitializer,
      TestGraphChecker<? super I, ? super V, ? super E> graphChecker,
      GiraphConfiguration conf) throws Exception {
    NumericTestGraph<I, V, E> graph = new NumericTestGraph<>(conf);
    graphInitializer.modifyGraph((NumericTestGraph) graph);
    runTest(graph, graphChecker);
  }

  /**
   * Base of runTest. Takes a created graph, a graph-checker and conf and runs
   * the test.
   */
  public static
  <I extends WritableComparable, E extends Writable, V extends Writable>
  void runTest(
      NumericTestGraph<I, V, E> graph,
      TestGraphChecker<? super I, ? super V, ? super E> graphChecker
  ) throws Exception {
    graph = new NumericTestGraph<I, V, E>(
      LocalBlockRunner.runApp(
          graph.getTestGraph(), useFullDigraphTests(graph.getConf())));
    if (graphChecker != null) {
      graphChecker.checkOutput((NumericTestGraph) graph);
    }
  }

  /**
   * Chain execution of multiple TestGraphModifier into one.
   */
  @SafeVarargs
  public static
  <I extends WritableComparable, V extends Writable, E extends Writable>
  TestGraphModifier<I, V, E> chainModifiers(
          final TestGraphModifier<I, V, E>... graphModifiers) {
    return new TestGraphModifier<I, V, E>() {
      @Override
      public void modifyGraph(
          NumericTestGraph<I, V, E> graph) {
        for (TestGraphModifier<I, V, E> graphModifier : graphModifiers) {
          graphModifier.modifyGraph(graph);
        }
      }
    };
  }

  /**
   * Chain execution of multiple BulkConfigurators into one.
   *
   * Order might matter, if they are setting the same fields.
   * (later one will override what previous one already set).
   */
  public static BulkConfigurator chainConfigurators(
      final BulkConfigurator... configurators) {
    return new BulkConfigurator() {
      @Override
      public void configure(GiraphConfiguration conf) {
        for (BulkConfigurator configurator : configurators) {
          configurator.configure(conf);
        }
      }
    };
  }


  public static Supplier<DoubleWritable> doubleSupplier(final double value) {
    return new Supplier<DoubleWritable>() {
      @Override
      public DoubleWritable get() {
        return new DoubleWritable(value);
      }
    };
  }

  public static Supplier<NullWritable> nullSupplier() {
    return new Supplier<NullWritable>() {
      @Override
      public NullWritable get() {
        return NullWritable.get();
      }
    };
  }

  /** Interface for running a single test that can throw an exception */
  interface OneTest {
    void test() throws Exception;
  }

  private static void repeat(int times, OneTest test) throws Exception {
    if (times == 1) {
      test.test();
    } else {
      int failures = 0;
      StringBuilder failureMsgs = new StringBuilder();
      AssertionError firstError = null;
      for (int i = 0; i < times; i++) {
        try {
          test.test();
        } catch (AssertionError error) {
          failures++;
          failureMsgs.append("\n").append(error.getMessage());
          if (firstError == null) {
            firstError = error;
          }
        }
      }

      if (failures > 0) {
        throw new AssertionError(
            "Failed " + failures + " times out of " + times +
            " runs, messages: " + failureMsgs,
            firstError);
      }
    }
  }

  private static boolean useFullDigraphTests(GiraphConfiguration conf) {
    return USE_FULL_GIRAPH_ENV_IN_TESTS.get(conf) ||
        System.getProperty("test_setup.UseFullGiraphEnvInTests") != null;
  }

  private static int repeatTimes() {
    String value = System.getProperty("test_setup.TestRepeatTimes");
    return value != null ? Integer.parseInt(value) : TEST_REPEAT_TIMES;
  }

  public static void setTestRepeatTimes(int testRepeatTimes) {
    TestGraphUtils.TEST_REPEAT_TIMES = testRepeatTimes;
  }
}
