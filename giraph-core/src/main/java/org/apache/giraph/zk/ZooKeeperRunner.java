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

import org.apache.giraph.conf.ImmutableClassesGiraphConfigurable;

import java.io.IOException;

/**
 * ZooKeeper wrapper interface.
 * Implementation should provide a way to start, stop and cleanup
 * zookeeper.
 */
public interface ZooKeeperRunner extends ImmutableClassesGiraphConfigurable {

  /**
   * Starts zookeeper service in specified working directory with
   * specified config file.
   * @param zkDir working directory
   * @param config zookeeper configuration
   * @return port zookeeper runs on
   * @throws IOException
   */
  int start(String zkDir, ZookeeperConfig config) throws IOException;

  /**
   * Stops zookeeper.
   */
  void stop();

  /**
   * Does necessary cleanup after zookeeper job is complete.
   */
  void cleanup();
}
