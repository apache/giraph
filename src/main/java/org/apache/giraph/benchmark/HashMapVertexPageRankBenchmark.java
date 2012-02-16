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

import org.apache.giraph.graph.HashMapVertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
import java.util.Iterator;

/**
 * Same benchmark code as {@link PageRankBenchmark}, but uses
 * Hashmap-backed Vertex implementation rather than
 * {@link org.apache.giraph.graph.EdgeListVertex}
 */
public class HashMapVertexPageRankBenchmark extends HashMapVertex<
    LongWritable, DoubleWritable, DoubleWritable, DoubleWritable> {
  @Override
  public void compute(Iterator<DoubleWritable> msgIterator) throws IOException {
    PageRankComputation.computePageRank(this, msgIterator);
  }
}
