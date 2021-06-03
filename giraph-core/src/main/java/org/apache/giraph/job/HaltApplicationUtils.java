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

package org.apache.giraph.job;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.utils.CounterUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;

/**
 * Utility methods for halting application while running
 */
public class HaltApplicationUtils {
  /** Do not instantiate */
  private HaltApplicationUtils() { }

  /**
   * Wait for halt info to become available and print instructions on how to
   * halt
   *
   * @param submittedJob Submitted job
   * @param conf         Configuration
   */
  public static void printHaltInfo(Job submittedJob,
      GiraphConfiguration conf) {
    String zkServer = CounterUtils.waitAndGetCounterNameFromGroup(
        submittedJob, GiraphConstants.ZOOKEEPER_SERVER_PORT_COUNTER_GROUP);
    String haltNode = CounterUtils.waitAndGetCounterNameFromGroup(
        submittedJob, GiraphConstants.ZOOKEEPER_HALT_NODE_COUNTER_GROUP);
    if (zkServer != null && haltNode != null) {
      GiraphConstants.HALT_INSTRUCTIONS_WRITER_CLASS.newInstance(
          conf).writeHaltInstructions(zkServer, haltNode);
    }
  }

  /**
   * Writer of instructions about how to halt
   */
  public interface HaltInstructionsWriter {
    /**
     * Write instructions about how to halt
     *
     * @param zkServer ZooKeeper server
     * @param haltNode ZooKeeper node which should be created in order to halt
     */
    void writeHaltInstructions(String zkServer, String haltNode);
  }

  /**
   * Default implementation of {@link HaltInstructionsWriter} - points to how
   * to use {@link org.apache.giraph.zk.ZooKeeperNodeCreator} to halt
   */
  public static class DefaultHaltInstructionsWriter implements
      HaltInstructionsWriter {
    /** Class logger */
    private static final Logger LOG = Logger.getLogger(
        DefaultHaltInstructionsWriter.class);

    @Override
    public void writeHaltInstructions(String zkServer, String haltNode) {
      if (LOG.isInfoEnabled()) {
        LOG.info("writeHaltInstructions: " +
            "To halt after next superstep execute: " +
            "'bin/halt-application --zkServer " + zkServer +
            " --zkNode " + haltNode + "'");
      }
    }
  }
}
