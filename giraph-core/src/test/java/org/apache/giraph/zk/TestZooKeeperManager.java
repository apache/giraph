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

import org.apache.giraph.conf.GiraphConstants;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestZooKeeperManager {
  @Test
  public void testGetBasePath() {
    Configuration conf = new Configuration();

    // Default is empty, everything goes in root znode
    assertEquals("Default value for base path should be empty",
        "", ZooKeeperManager.getBasePath(conf));

    conf.set(GiraphConstants.BASE_ZNODE_KEY, "/howdy");
    assertEquals("Base path should reflect value of " +
        GiraphConstants.BASE_ZNODE_KEY,
        "/howdy", ZooKeeperManager.getBasePath(conf));

    conf.set(GiraphConstants.BASE_ZNODE_KEY, "no_slash");
    try {
      ZooKeeperManager.getBasePath(conf);
      fail("Should not have allowed path without starting slash");
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains(GiraphConstants.BASE_ZNODE_KEY));
    }
  }
}
