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

package org.apache.giraph.examples.block_app;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.giraph.BspCase;
import org.apache.giraph.block_app.framework.BlockUtils;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.examples.SimplePageRankComputation.SimplePageRankVertexInputFormat;
import org.apache.giraph.job.GiraphJob;
import org.junit.Test;

/**
 * Unit test for many simple BSP applications.
 */
public class
    TestMigrationBspBasic extends BspCase {

  public TestMigrationBspBasic() {
    super(TestMigrationBspBasic.class.getName());
  }

  /**
   * Run a sample BSP job locally and test using migration
   * library for Blocks Framework, as an drop-in replacement.
   */
  @Test
  public void testBspMigrationToBlocksFramework() throws Exception {
    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setVertexInputFormatClass(SimplePageRankVertexInputFormat.class);
    BlockUtils.setAndInitBlockFactoryClass(
        conf, SimpleMigrationMasterBlockFactory.class);
    GiraphJob job = prepareJob(getCallingMethodName(), conf);
    assertTrue(job.run(true));
    if (!runningInDistributedMode()) {
      double finalSum =
          SimpleMigrationMasterBlockFactory.SimpleMigrationMasterWorkerContext.getFinalSum();
      System.out.println("testBspMasterCompute: finalSum=" + finalSum);
      assertEquals(32.5, finalSum, 0d);
    }
  }
}
