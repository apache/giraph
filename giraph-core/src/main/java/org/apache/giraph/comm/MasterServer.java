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

import org.apache.giraph.comm.flow_control.FlowControl;

import java.net.InetSocketAddress;

/**
 * Interface for master to receive messages from workers
 */
public interface MasterServer {
  /**
   * Get server address
   *
   * @return Address used by this server
   */
  InetSocketAddress getMyAddress();

  /**
   * Get server host or IP
   *
   * @return Server host or IP
   */
  String getLocalHostOrIp();

  /**
   * Shuts down.
   */
  void close();

  /**
   * Inform the server about the flow control policy used in sending requests
   *
   * @param flowControl reference to flow control policy
   */
  void setFlowControl(FlowControl flowControl);
}
