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
package org.apache.giraph.examples.darwini;

import org.apache.giraph.block_app.framework.piece.Piece;
import org.apache.giraph.types.NoMessage;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

/**
 * Marks the new iteration of random edge allocation.
 * Increases a group size by 2x.
 */
public class NextStage extends
    Piece<LongWritable, VertexData, NullWritable, NoMessage, Integer> {

  /**
   * Maximum size of the group
   */
  private int maxSpan;

  /**
   * Constructs this piece by setting the maximum size of
   * the group (in powers of 2)
   * @param maxSpan maximum size of the group
   */
  public NextStage(int maxSpan) {
    this.maxSpan = maxSpan;
  }

  @Override
  public Integer nextExecutionStage(Integer executionStage) {
    executionStage = executionStage + 1;
    if (executionStage >= maxSpan) {
      return maxSpan;
    }
    return executionStage;
  }

}
