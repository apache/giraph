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

package org.apache.giraph.master;

import org.apache.giraph.combiner.MessageCombiner;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.IntNoOpComputation;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.junit.Test;

import java.io.IOException;

/** Test type verification when switching computation and combiner types */
public class TestComputationCombinerTypes {
  @Test
  public void testAllMatchWithoutCombiner() {
    SuperstepClasses classes =
        new SuperstepClasses(IntNoOpComputation.class, null);
    classes.verifyTypesMatch(createConfiguration(IntNoOpComputation.class), true);
  }

  @Test
  public void testAllMatchWithCombiner() {
    SuperstepClasses classes =
        new SuperstepClasses(IntIntIntLongDoubleComputation.class,
            IntDoubleMessageCombiner.class);
    classes.verifyTypesMatch(
        createConfiguration(IntIntIntIntLongComputation.class), true);
  }

  @Test(expected = IllegalStateException.class)
  public void testDifferentIdTypes() {
    SuperstepClasses classes =
        new SuperstepClasses(LongIntIntLongIntComputation.class, null);
    classes.verifyTypesMatch(
        createConfiguration(IntIntIntIntLongComputation.class), true);
  }

  @Test(expected = IllegalStateException.class)
  public void testDifferentVertexValueTypes() {
    SuperstepClasses classes =
        new SuperstepClasses(IntLongIntLongIntComputation.class, null);
    classes.verifyTypesMatch(
        createConfiguration(IntIntIntIntLongComputation.class), true);
  }

  @Test(expected = IllegalStateException.class)
  public void testDifferentEdgeDataTypes() {
    SuperstepClasses classes =
        new SuperstepClasses(IntIntLongLongIntComputation.class, null);
    classes.verifyTypesMatch(
        createConfiguration(IntIntIntIntLongComputation.class), true);
  }

  @Test(expected = IllegalStateException.class)
  public void testDifferentMessageTypes() {
    SuperstepClasses classes =
        new SuperstepClasses(IntIntIntIntLongComputation.class, null);
    classes.verifyTypesMatch(
        createConfiguration(IntIntIntLongDoubleComputation.class), true);
  }

  @Test(expected = IllegalStateException.class)
  public void testDifferentCombinerIdType() {
    SuperstepClasses classes =
        new SuperstepClasses(IntIntIntLongDoubleComputation.class,
            DoubleDoubleMessageCombiner.class);
    classes.verifyTypesMatch(
        createConfiguration(IntIntIntIntLongComputation.class), true);
  }

  @Test(expected = IllegalStateException.class)
  public void testDifferentCombinerMessageType() {
    SuperstepClasses classes =
        new SuperstepClasses(IntIntIntLongDoubleComputation.class,
            IntLongMessageCombiner.class);
    classes.verifyTypesMatch(
        createConfiguration(IntIntIntIntLongComputation.class), true);
  }

  private static ImmutableClassesGiraphConfiguration createConfiguration(
      Class<? extends Computation> computationClass) {
    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(computationClass);
    return new ImmutableClassesGiraphConfiguration(conf);
  }

  public static class NoOpComputation<I extends WritableComparable,
      V extends Writable, E extends Writable, M1 extends Writable,
      M2 extends Writable> extends AbstractComputation<I, V, E, M1, M2> {
    @Override
    public void compute(Vertex<I, V, E> vertex,
        Iterable<M1> messages) throws IOException {
    }
  }

  private static class IntIntIntIntLongComputation extends
      NoOpComputation<IntWritable, IntWritable, IntWritable, IntWritable,
          LongWritable> { }

  private static class IntIntIntLongDoubleComputation extends
      NoOpComputation<IntWritable, IntWritable, IntWritable, LongWritable,
          DoubleWritable> { }

  private static class LongIntIntLongIntComputation extends
      NoOpComputation<LongWritable, IntWritable, IntWritable, LongWritable,
          IntWritable> { }

  private static class IntLongIntLongIntComputation extends
      NoOpComputation<IntWritable, LongWritable, IntWritable, LongWritable,
          IntWritable> { }

  private static class IntIntLongLongIntComputation extends
      NoOpComputation<IntWritable, IntWritable, LongWritable, LongWritable,
          IntWritable> { }

  private static class NoOpMessageCombiner<I extends WritableComparable,
      M extends Writable> extends MessageCombiner<I, M> {
    @Override
    public void combine(I vertexIndex, M originalMessage, M messageToCombine) {
    }

    @Override
    public M createInitialMessage() {
      return null;
    }
  }

  private static class IntDoubleMessageCombiner
      extends NoOpMessageCombiner<IntWritable,
                  DoubleWritable> { }

  private static class DoubleDoubleMessageCombiner
      extends NoOpMessageCombiner<DoubleWritable,
                  DoubleWritable> { }

  private static class IntLongMessageCombiner
      extends NoOpMessageCombiner<IntWritable,
                  LongWritable> { }
}
