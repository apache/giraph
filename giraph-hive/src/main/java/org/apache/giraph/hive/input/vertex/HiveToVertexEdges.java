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

package org.apache.giraph.hive.input.vertex;

import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.facebook.giraph.hive.HiveReadableRecord;

import java.util.Collections;

/**
 * Interface for creating edges for a vertex from a Hive record.
 * Used with HiveToVertex if you want to also read edges per vertex, as opposed
 * to using {@link org.apache.giraph.hive.input.edge.HiveEdgeInputFormat}
 *
 * @param <I> Vertex ID
 * @param <E> extends Writable
 */
public interface HiveToVertexEdges<I extends WritableComparable,
    E extends Writable> {
  /**
   * Read Vertex's edges from the HiveRecord given.
   *
   * @param record HiveRecord to read from.
   * @return iterable of edges
   */
  Iterable<Edge<I, E>> getEdges(HiveReadableRecord record);

  /**
   * Default implementation that returns an empty list of edges.
   */
  public class Empty implements HiveToVertexEdges {
    /** Singleton */
    private static final Empty INSTANCE = new Empty();

    /** Don't construct, allow inheritance */
    protected Empty() { }

    /**
     * Get singleton instance
     * @return Empty
     */
    public static Empty get() { return INSTANCE; }

    @Override
    public Iterable getEdges(HiveReadableRecord record) {
      return Collections.emptyList();
    }
  }
}
