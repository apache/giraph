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
package org.apache.giraph.block_app.framework;

import java.util.List;

import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.hadoop.conf.Configuration;

/**
 * Class describing a particular application.
 * Everything except input and output should be fully encapsulated within
 * this class. For any application, it should be enough to only specify
 * particular BlockFactory.
 *
 * Given configuration, it creates a block that represents a full Giraph job.
 *
 * Recommended is to extend AbstractBlockFactory directly for most cases.
 *
 * @param <S> Execution stage type
 */
public interface BlockFactory<S> {
  /**
   * Based on provided configuration, updates it, such that all necessary
   * properties are initialized.
   */
  void initConfig(GiraphConfiguration conf);

  /**
   * Create a block (representing a full Giraph job), based on the given
   * configuration. Configuration should be treated as immutable at this point.
   *
   * If there are issues in configuration, it is very cheap to throw
   * from this method - as Giraph job will not even start.
   * This function will be called two times - once before starting
   * of the Giraph job, to fail early if anything is incorrectly configured.
   * Second time will be on Master, which will return Block instance
   * on which createIterator will be called once, which should return
   * current application run.
   * initConfig will be called only once, before starting Giraph job itself.
   * Master will contain configuration already modified by initConfig.
   */
  Block createBlock(GiraphConfiguration conf);

  /**
   * Create an empty instance of execution stage object.
   *
   * Can be used by application to be aware of what was executed before.
   * Most common example is counting iterations, or for having a boolean whether
   * some important event happened.
   *
   * Execution stage should be immutable object, with creating a new
   * object when different value is needed.
   */
  S createExecutionStage(GiraphConfiguration conf);

  /**
   * Get special GC Java options. If returns null, default options are used.
   */
  List<String> getGcJavaOpts(Configuration conf);

  /**
   * Register outputs to use during the application (vs output at the end of
   * the application), based on provided configuration.
   */
  void registerOutputs(GiraphConfiguration conf);
}
