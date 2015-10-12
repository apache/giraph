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

package org.apache.giraph.master.input;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Input splits organizer for vertex and edge input splits on master, which
 * doesn't use locality information
 */
public class BasicInputSplitsMasterOrganizer
    implements InputSplitsMasterOrganizer {
  /** Available splits queue */
  private final ConcurrentLinkedQueue<byte[]> splits;

  /**
   * Constructor
   *
   * @param serializedSplits Splits
   */
  public BasicInputSplitsMasterOrganizer(List<byte[]> serializedSplits) {
    splits = new ConcurrentLinkedQueue<>(serializedSplits);
  }

  @Override
  public byte[] getSerializedSplitFor(int workerTaskId) {
    return splits.poll();
  }
}
