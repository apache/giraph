/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.graph;

import java.io.IOException;
import java.util.List;

import org.apache.giraph.comm.MsgList;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Interface for Vertex combiner (messages)
 *
 * @param <I extends Writable> index
 * @param <M extends Writable> message data
 *
 **/
@SuppressWarnings("rawtypes")
public interface VertexCombiner<I extends WritableComparable,
                                V extends Writable,
                                E extends Writable,
                                M extends Writable> {

  /**
   * Combines message values for a particular vertex index.
   *
   * @param vertexIndex Index of the vertex getting these messages
   * @param msgList List of the messages to be combined
   * @return Message that is combined from {@link MsgList}
   * @throws IOException
   */
   M combine(I vertexIndex,
             List<M> msgList) throws IOException;
}
