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
package org.apache.giraph.zk;

import java.net.InetSocketAddress;

/**
 * Zookeeper configuration file.
 * Originally copied from zookeeper sources to allow
 * modification on the fly instead of reading from disk.
 */
public class ZookeeperConfig {

  /** Zookeeper server address */
  private InetSocketAddress clientPortAddress;
  /** Snapshot log dir */
  private String dataDir;
  /** Transaction log dir */
  private String dataLogDir;
  /** minimum session timeout in milliseconds */
  private int minSessionTimeout = -1;
  /** maximum session timeout in milliseconds */
  private int maxSessionTimeout = -1;
  /**
   * Get zookeeper server address
   * @return zookeeper server address
   */
  public InetSocketAddress getClientPortAddress() { return clientPortAddress; }

  /**
   * Snapshot dir
   * @return snapshot dir path
   */
  public String getDataDir() { return dataDir; }

  /**
   * Transaction dir
   * @return transaction dir path
   */
  public String getDataLogDir() {
    if (dataLogDir == null) {
      return dataDir;
    }
    return dataLogDir;
  }
  /**
   * Minimum session timeout in milliseconds.
   * @return Minimum session time.
   */
  public int getMinSessionTimeout() { return minSessionTimeout; }

  /**
   * Maximum session timeout in milliseconds.
   *
   * @return Maximum session time.
   */
  public int getMaxSessionTimeout() { return maxSessionTimeout; }

  /**
   * Set snapshot log dir
   * @param dataDir snapshot log dir path
   */
  public void setDataDir(String dataDir) {
    this.dataDir = dataDir;
  }

  /**
   * Transaction log dir
   * @param dataLogDir transaction log dir path
   */
  public void setDataLogDir(String dataLogDir) {
    this.dataLogDir = dataLogDir;
  }

  /**
   * Set zookeeper server address
   * @param clientPortAddress server address
   */
  public void setClientPortAddress(InetSocketAddress clientPortAddress) {
    this.clientPortAddress = clientPortAddress;
  }

  /**
   * Set minimum session timeout in milliseconds
   * @param minSessionTimeout min session timeout
   */
  public void setMinSessionTimeout(int minSessionTimeout) {
    this.minSessionTimeout = minSessionTimeout;
  }
  /**
   * Set maximum session timeout in milliseconds
   * @param maxSessionTimeout max session timeout
   */
  public void setMaxSessionTimeout(int maxSessionTimeout) {
    this.maxSessionTimeout = maxSessionTimeout;
  }

}
