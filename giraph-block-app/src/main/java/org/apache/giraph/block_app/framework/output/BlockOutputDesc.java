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
package org.apache.giraph.block_app.framework.output;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Progressable;

/**
 * Output description
 *
 * @param <OW> Writer type
 */
public interface BlockOutputDesc<OW extends BlockOutputWriter> {
  /**
   * Initialize output and perform any necessary checks
   *
   * @param jobIdentifier Unique identifier of the job
   * @param conf Configuration
   */
  void initializeAndCheck(String jobIdentifier, Configuration conf);

  /**
   * Create writer
   *
   * @param conf Configuration
   * @param hadoopProgressable Progressable to call progress on
   * @return Writer
   */
  OW createOutputWriter(Configuration conf, Progressable hadoopProgressable);

  /**
   * This method will be called before creating any writers
   */
  default void preWriting() {
  }

  /**
   * This method will be called after all writers are closed
   */
  default void postWriting() {
  }

  /**
   * Commit everything
   */
  void commit();
}
