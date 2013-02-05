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
package org.apache.giraph.partition;

import org.apache.giraph.graph.DefaultEdge;
import org.apache.giraph.graph.Edge;
import org.apache.giraph.job.GiraphJob;
import org.apache.giraph.graph.GiraphTransferRegulator;
import org.apache.giraph.vertex.EdgeListVertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test the GiraphTransferRegulator.
 */
public class TestGiraphTransferRegulator {
  /** Job filled in by setup() */
  private GiraphJob job;
  /** Instantiated vertex filled in from setup() */
  private IFDLEdgeListVertex vertex = new IFDLEdgeListVertex();

  /**
   * Simple instantiable class that extends
   * {@link org.apache.giraph.vertex.EdgeListVertex}.
   */
  public static class IFDLEdgeListVertex extends
      EdgeListVertex<IntWritable, FloatWritable, DoubleWritable, LongWritable> {
    @Override
    public void compute(Iterable<LongWritable> messages) throws IOException { }
  }

  @Before
  public void setUp() {
    try {
      job = new GiraphJob("TestGiraphTransferRegulator");
    } catch (IOException e) {
      throw new RuntimeException("setUp: Failed", e);
    }
    job.getConfiguration().setVertexClass(IFDLEdgeListVertex.class);
  }

  @Test
  public void testGiraphTransferRegulator() {
    job.getConfiguration()
        .setInt(GiraphTransferRegulator.MAX_VERTICES_PER_TRANSFER, 1);
    job.getConfiguration()
        .setInt(GiraphTransferRegulator.MAX_EDGES_PER_TRANSFER, 3);
    List<Edge<IntWritable, DoubleWritable>> edges = Lists.newLinkedList();
    edges.add(new DefaultEdge<IntWritable, DoubleWritable>(new IntWritable(2),
        new DoubleWritable(22)));
    edges.add(new DefaultEdge<IntWritable, DoubleWritable>(new IntWritable(3),
        new DoubleWritable(33)));
    edges.add(new DefaultEdge<IntWritable, DoubleWritable>(new IntWritable(4),
        new DoubleWritable(44)));
    vertex.initialize(null, null, edges);
    GiraphTransferRegulator gtr =
        new GiraphTransferRegulator(job.getConfiguration());
    PartitionOwner owner = mock(PartitionOwner.class);
    when(owner.getPartitionId()).thenReturn(57);
    assertFalse(gtr.transferThisPartition(owner));
    gtr.incrementCounters(owner, vertex);
    assertTrue(gtr.transferThisPartition(owner));
  }

}
