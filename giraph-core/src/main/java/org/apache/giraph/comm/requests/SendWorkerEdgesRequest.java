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

package org.apache.giraph.comm.requests;

import org.apache.giraph.comm.ServerData;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.utils.ByteArrayVertexIdEdges;
import org.apache.giraph.utils.PairList;
import org.apache.giraph.utils.VertexIdEdges;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Send a collection of edges for a partition.
 *
 * @param <I> Vertex id
 * @param <E> Edge data
 */
@SuppressWarnings("unchecked")
public class SendWorkerEdgesRequest<I extends WritableComparable,
    E extends Writable>
    extends SendWorkerDataRequest<I, Edge<I, E>,
    VertexIdEdges<I, E>> {
  /**
   * Constructor used for reflection only
   */
  public SendWorkerEdgesRequest() { }

  /**
   * Constructor used to send request.
   *
   * @param partVertEdges Map of remote partitions =&gt;
   *                     ByteArrayVertexIdEdges
   */
  public SendWorkerEdgesRequest(
      PairList<Integer, VertexIdEdges<I, E>> partVertEdges) {
    this.partitionVertexData = partVertEdges;
  }

  @Override
  public VertexIdEdges<I, E> createVertexIdData() {
    return new ByteArrayVertexIdEdges<>();
  }

  @Override
  public RequestType getType() {
    return RequestType.SEND_WORKER_EDGES_REQUEST;
  }

  @Override
  public void doRequest(ServerData serverData) {
    PairList<Integer, VertexIdEdges<I, E>>.Iterator
        iterator = partitionVertexData.getIterator();
    while (iterator.hasNext()) {
      iterator.next();
      serverData.getEdgeStore()
          .addPartitionEdges(iterator.getCurrentFirst(),
              iterator.getCurrentSecond());
    }
  }
}
