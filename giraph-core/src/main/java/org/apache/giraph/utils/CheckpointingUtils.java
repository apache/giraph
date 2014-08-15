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

package org.apache.giraph.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import static org.apache.giraph.conf.GiraphConstants.CHECKPOINT_DIRECTORY;

/**
 * Holds useful functions to get checkpoint paths
 * in hdfs.
 */
public class CheckpointingUtils {

  /**
   * Do not call constructor.
   */
  private CheckpointingUtils() {
  }

  /**
   * Path to the checkpoint's root (including job id)
   * @param conf Immutable configuration of the job
   * @param jobId job ID
   * @return checkpoint's root
   */
  public static String getCheckpointBasePath(Configuration conf,
                                             String jobId) {
    return CHECKPOINT_DIRECTORY.getWithDefault(conf,
        CHECKPOINT_DIRECTORY.getDefaultValue() + "/" + jobId);
  }

  /**
   * Path to checkpoint&halt node in hdfs.
   * It is set to let client know that master has
   * successfully finished checkpointing and job can be restarted.
   * @param conf Immutable configuration of the job
   * @param jobId job ID
   * @return path to checkpoint&halt node in hdfs.
   */
  public static Path getCheckpointMarkPath(Configuration conf,
                                           String jobId) {
    return new Path(getCheckpointBasePath(conf, jobId), "halt");
  }
}
