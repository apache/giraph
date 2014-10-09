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

package org.apache.giraph.comm;

import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Interface for master to send messages to workers
 */
public interface MasterClient {
  /**
   * Make sure that all the connections to workers have been established.
   */
  void openConnections();

  /**
   * Sends aggregator to its owner
   *
   * @param name Name of the object
   * @param type Global communication type
   * @param value Object value
   * @throws IOException
   */
  void sendToOwner(String name, GlobalCommType type, Writable value)
    throws IOException;

  /**
   * Flush aggregated values cache.
   */
  void finishSendingValues() throws IOException;

  /**
   * Flush all outgoing messages.  This will synchronously ensure that all
   * messages have been send and delivered prior to returning.
   */
  void flush();

  /**
   * Closes all connections.
   */
  void closeConnections();
}

