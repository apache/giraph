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
package org.apache.giraph.utils;

import static org.apache.giraph.utils.ReflectionUtils.getTypeArguments;
import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.giraph.conf.TypesHolder;
import org.apache.giraph.edge.ByteArrayEdges;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.factories.DefaultEdgeValueFactory;
import org.apache.giraph.factories.DefaultMessageValueFactory;
import org.apache.giraph.factories.DefaultVertexIdFactory;
import org.apache.giraph.factories.DefaultVertexValueFactory;
import org.apache.giraph.factories.EdgeValueFactory;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.giraph.factories.VertexIdFactory;
import org.apache.giraph.factories.VertexValueFactory;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.graph.DefaultVertexResolver;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexResolver;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.junit.Test;

public class TestReflectionUtils {
  @Test
  public void testPackagePath() {
    assertEquals("org/apache/giraph/utils",
        ReflectionUtils.getPackagePath(TestReflectionUtils.class));
    assertEquals("org/apache/giraph/utils",
        ReflectionUtils.getPackagePath(getClass()));
    assertEquals("org/apache/giraph/utils",
        ReflectionUtils.getPackagePath(this));
  }

  private static class IntTypes implements TypesHolder<IntWritable,
      IntWritable, IntWritable, IntWritable, IntWritable> { }

  private static class IntComputation extends AbstractComputation<IntWritable,
          IntWritable, IntWritable, IntWritable, IntWritable> {
    @Override
    public void compute(Vertex<IntWritable, IntWritable, IntWritable> vertex,
        Iterable<IntWritable> messages) throws IOException {
    }
  }

  private static class IntBasicComputation extends BasicComputation<IntWritable,
      IntWritable, IntWritable, IntWritable> {
    @Override
    public void compute(Vertex<IntWritable, IntWritable, IntWritable> vertex,
        Iterable<IntWritable> messages) throws IOException {
    }
  }

  @Test
  public void testInferWithGenerics() {
    Class<?>[] classes = getTypeArguments(VertexResolver.class,
        DefaultVertexResolver.class);
    assertEquals(3, classes.length);
    assertEquals(WritableComparable.class, classes[0]);
    assertEquals(Writable.class, classes[1]);
    assertEquals(Writable.class, classes[2]);

    classes = getTypeArguments(VertexIdFactory.class,
        DefaultVertexIdFactory.class);
    assertEquals(1, classes.length);
    assertEquals(WritableComparable.class, classes[0]);

    classes = getTypeArguments(VertexValueFactory.class,
        DefaultVertexValueFactory.class);
    assertEquals(1, classes.length);
    assertEquals(Writable.class, classes[0]);

    classes = getTypeArguments(EdgeValueFactory.class,
        DefaultEdgeValueFactory.class);
    assertEquals(1, classes.length);
    assertEquals(Writable.class, classes[0]);

    classes = getTypeArguments(MessageValueFactory.class,
        DefaultMessageValueFactory.class);
    assertEquals(1, classes.length);
    assertEquals(Writable.class, classes[0]);

    classes = getTypeArguments(OutEdges.class, ByteArrayEdges.class);
    assertEquals(2, classes.length);
    assertEquals(WritableComparable.class, classes[0]);
    assertEquals(Writable.class, classes[1]);
  }

  @Test
  public void testInferTypeParams() {
    checkTypes(TypesHolder.class, IntTypes.class, 5);
    checkTypes(TypesHolder.class, IntComputation.class, 5);
    checkTypes(Computation.class, IntComputation.class, 5);
    checkTypes(TypesHolder.class, IntBasicComputation.class, 5);
    checkTypes(Computation.class, IntBasicComputation.class, 5);
    checkTypes(BasicComputation.class, IntBasicComputation.class, 4);
  }

  private <T> void checkTypes(Class<T> baseClass,
    Class<? extends T> childClass, int numArgs) {
    Class<?>[] classes = getTypeArguments(baseClass, childClass);
    assertEquals(numArgs, classes.length);
    for (Class<?> klass : classes) {
      assertEquals(IntWritable.class, klass);
    }
  }
}
